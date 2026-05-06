"""Phase 3.5 — deep website crawl for ``venue_groups`` stuck in ``multiple_candidates``.

Follows in-site links to terms, privacy, contact, about, liquor/RSA, legal, etc., extracts
and checksum-validates ABNs, scores disambiguation signals, and upgrades rows to
``verified`` when possible. Optional ASIC Connect attempt (JSF — often ends as no match).

Run after Phase 3:  ``python scrapers/group_abn_deep_lookup.py``

Then cascade:       ``python scrapers/inherit_group_abn.py``

Requires: ``DATABASE_URL`` in ``env.local`` (``load_dotenv(..., override=True)``).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import ssl
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
import psycopg
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg.types.json import Json
from rapidfuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger("group_abn_deep_lookup")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

USER_AGENT = "MilestoneInnovationsGroup-DataBuilder (richard@milestoneigroup.com)"
IN_GROUP_DELAY_S = 1.5
BETWEEN_GROUPS_DELAY_S = 3.0
WALL_CLOCK_BUDGET_S = 90 * 60
MAX_FETCHES_GLOBAL = 200
MAX_PAGES_PER_GROUP = 45
CLOUDFLARE_ABORT_THRESHOLD = 2
SERVER_ERROR_ABORT_THRESHOLD = 4

ASIC_LANDING = (
    "https://connectonline.asic.gov.au/RegistrySearch/faces/landing/SearchRegisters.jspx"
)


def _asic_ssl_context() -> ssl.SSLContext:
    """Allow legacy TLS renegotiation required by some ASIC Connect endpoints."""
    ctx = ssl.create_default_context()
    try:
        ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT  # type: ignore[attr-defined]
    except AttributeError:
        pass
    return ctx


WEIGHTS = (10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19)

ABN_LABELLED = re.compile(
    r"\bABN[:\s]*(\d{2}\s?\d{3}\s?\d{3}\s?\d{3})\b", re.IGNORECASE
)
DIGITS_11 = re.compile(r"\b(\d{11})\b")

DEEP_PATH_HINT = re.compile(
    r"(/terms(?:[-/._]|\b)|/terms-of-use|/terms-and-conditions|/terms-conditions|"
    r"/terms-of-service|"
    r"/tc(?:/|$)|/t-and-c|"
    r"/privacy(?:/|$)|/privacy-policy|/cookie|"
    r"/policy(?:/|$)|/policies/|"
    r"/contact(?:/|$)|/contact-us|/reach-us|"
    r"/about(?:/|$)|/about-us|/our-story|"
    r"/legal(?:/|$)|/legal-notice|"
    r"/responsible-service|/rsa(?:/|$)|/liquor-licence|/liquor-licen[sc]e|"
    r"/accessibility|/sitemap|"
    r"/footer\.html)",
    re.I,
)


def _deep_cache_dir(group_slug: str) -> Path:
    base = Path("/tmp/abn_cache/groups_deep") / group_slug
    try:
        base.mkdir(parents=True, exist_ok=True)
    except OSError:
        import tempfile

        base = Path(tempfile.gettempdir()) / "abn_cache" / "groups_deep" / group_slug
        base.mkdir(parents=True, exist_ok=True)
    return base


def normalise_abn_digits(raw: str) -> str:
    return re.sub(r"\D", "", raw)


def validate_abn(abn_digits: str) -> bool:
    if len(abn_digits) != 11 or not abn_digits.isdigit():
        return False
    digits = [int(c) for c in abn_digits]
    digits[0] -= 1
    if digits[0] < 0:
        return False
    total = sum(d * w for d, w in zip(digits, WEIGHTS, strict=True))
    return total % 89 == 0


def abn_context_ok(html: str, start: int, end: int) -> bool:
    lo = max(0, start - 50)
    hi = min(len(html), end + 50)
    window = html[lo:hi].lower()
    return "abn" in window or "australian business number" in window


def legal_reference(row: dict[str, Any]) -> str:
    gl = (row.get("group_legal_name") or "").strip()
    if gl:
        return gl
    return (row.get("group_name") or "").strip()


def evidence_method_for_path(path: str) -> str:
    p = path.lower()
    if "terms" in p or "t-and-c" in p or "/tc" in p:
        return "website_terms_page"
    if "privacy" in p or "policy" in p or "cookie" in p:
        return "website_privacy_page"
    if "contact" in p or "reach-us" in p:
        return "website_contact_page"
    if "about" in p or "our-story" in p:
        return "website_about_page"
    if "legal" in p or "sitemap" in p:
        return "website_privacy_page"
    return "website_homepage"


def is_policyish_path(path: str) -> bool:
    p = path.lower()
    return any(
        k in p
        for k in (
            "terms",
            "privacy",
            "policy",
            "legal",
            "disclaimer",
        )
    )


def same_site(a: urlparse, b: urlparse) -> bool:
    def host_key(netloc: str) -> str:
        h = (netloc or "").lower().split("@")[-1].split(":")[0]
        return h.removeprefix("www.")

    return host_key(a.netloc) == host_key(b.netloc)


def canon_url(u: str) -> str:
    p = urlparse(u)
    path = p.path or "/"
    return urlunparse((p.scheme, p.netloc.lower(), path, "", p.query, ""))


def normalise_href(base: urlparse, href: str) -> str | None:
    if not href or href.startswith(("#", "mailto:", "tel:", "javascript:", "sms:")):
        return None
    if href.strip().startswith("//") and "@" in href:
        return None
    abs_u = urljoin(urlunparse(base), href)
    p = urlparse(abs_u)
    if p.scheme not in ("http", "https"):
        return None
    clean = p._replace(fragment="")
    return urlunparse(clean)


def page_slug_from_url(url: str) -> str:
    h = hashlib.sha256(url.encode("utf-8", errors="replace")).hexdigest()[:16]
    path = urlparse(url).path.strip("/").replace("/", "_")[:80]
    return f"{path or 'root'}_{h}" if path else f"root_{h}"


def extract_findings_from_html(
    html: str,
    page_url: str,
    path: str,
) -> list[dict[str, Any]]:
    """Return ABN hits with snippets and whether extracted from ``<footer>`` first pass."""
    soup = BeautifulSoup(html, "lxml")
    path_ev = evidence_method_for_path(path)
    findings: list[dict[str, Any]] = []

    def harvest(buffer: str, from_footer: bool) -> None:
        for m in ABN_LABELLED.finditer(buffer):
            d = normalise_abn_digits(m.group(1))
            if not validate_abn(d):
                continue
            lo, hi = m.start(), m.end()
            sn = buffer[max(0, lo - 80) : min(len(buffer), hi + 80)]
            findings.append(
                {
                    "abn": d,
                    "snippet": sn,
                    "from_footer": from_footer,
                    "page_path": path,
                    "evidence_method": path_ev,
                    "page_url": page_url,
                }
            )
        for m in DIGITS_11.finditer(buffer):
            d = m.group(1)
            if not validate_abn(d) or not abn_context_ok(buffer, m.start(), m.end()):
                continue
            lo, hi = m.start(), m.end()
            sn = buffer[max(0, lo - 80) : min(len(buffer), hi + 80)]
            findings.append(
                {
                    "abn": d,
                    "snippet": sn,
                    "from_footer": from_footer,
                    "page_path": path,
                    "evidence_method": path_ev,
                    "page_url": page_url,
                }
            )

    footer = soup.find("footer")
    if footer:
        ft = footer.get_text(" ", strip=True)
        harvest(ft, True)
    visible = soup.get_text(" ", strip=True)
    if visible:
        harvest(visible, False)
    harvest(html, False)
    return findings


def score_hit(
    hit: dict[str, Any],
    legal_name: str,
) -> int:
    score = 0
    sn = (hit.get("snippet") or "").lower()
    ref = (legal_name or "").lower()
    if ref and fuzz.partial_ratio(ref, sn) >= 55:
        score += 20
    if (
        "trading as" in sn
        or "trading under" in sn
        or "operating as" in sn
    ):
        score += 15
    path = (hit.get("page_path") or "").lower()
    if hit.get("from_footer") and is_policyish_path(path):
        score += 10
    return score


def aggregate_scores(
    all_hits: list[dict[str, Any]],
    legal_name: str,
) -> dict[str, Any]:
    by_abn: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"hits": [], "urls": set(), "best": None}
    )
    for h in all_hits:
        abn = h["abn"]
        by_abn[abn]["hits"].append(h)
        by_abn[abn]["urls"].add(h["page_url"])

    rows: list[dict[str, Any]] = []
    for abn, data in by_abn.items():
        base = 0
        best_hit: dict[str, Any] | None = None
        best_partial = -1
        per_hit_scores: list[int] = []
        for h in data["hits"]:
            s = score_hit(h, legal_name)
            per_hit_scores.append(s)
            pr = fuzz.partial_ratio(legal_name.lower(), (h.get("snippet") or "").lower())
            if s > base or (s == base and pr > best_partial):
                base = max(base, s)
                best_hit = h
                best_partial = pr
        if len(data["urls"]) >= 2:
            base += 5
        max_hit = max(per_hit_scores) if per_hit_scores else 0
        auth_boost = 0
        for h in data["hits"]:
            ul = (h.get("page_url") or "").lower()
            if any(x in ul for x in ("terms", "privacy", "policy", "legal")):
                auth_boost = 1
                break
        blob = " ".join((h.get("snippet") or "") for h in data["hits"])
        name_affinity = fuzz.partial_ratio((legal_name or "").lower(), blob.lower())
        cumulative = sum(score_hit(h, legal_name) for h in data["hits"])
        rows.append(
            {
                "total": base,
                "name_affinity": name_affinity,
                "cumulative": cumulative,
                "auth_boost": auth_boost,
                "max_hit": max_hit,
                "url_count": len(data["urls"]),
                "hit_count": len(data["hits"]),
                "abn": abn,
                "best_hit": best_hit or data["hits"][0],
            }
        )

    rows.sort(
        key=lambda r: (
            -r["total"],
            -r["name_affinity"],
            -r["cumulative"],
            -r["auth_boost"],
            -r["max_hit"],
            -r["url_count"],
            -r["hit_count"],
            r["abn"],
        ),
    )
    return {"ranked_rows": rows}


def merge_notes(
    existing: Any,
    payload: dict[str, Any],
) -> Json:
    base: dict[str, Any]
    if existing is None:
        base = {}
    elif isinstance(existing, dict):
        base = dict(existing)
    elif isinstance(existing, str):
        try:
            base = dict(json.loads(existing))
        except json.JSONDecodeError:
            base = {"previous_notes_text": existing}
    else:
        base = {}
    base["phase_3_5_deep_lookup"] = payload
    return Json(base)


def load_targets(conn: psycopg.Connection) -> list[dict[str, Any]]:
    sql = """
    SELECT group_id, group_slug, group_website, group_legal_name, group_name,
           group_hq_state, group_abn_notes
    FROM public.venue_groups
    WHERE group_abn_status = 'multiple_candidates'
    ORDER BY group_slug
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def persist_upgrade(
    conn: psycopg.Connection,
    group_id: str,
    *,
    group_abn: str | None,
    status: str,
    evidence_method: str | None,
    evidence_url: str | None,
    entity_name: str | None,
    notes: Json,
) -> None:
    sql = """
    UPDATE public.venue_groups
    SET group_abn = %(abn)s,
        group_abn_status = %(st)s,
        group_abn_evidence_method = %(ev_m)s,
        group_abn_evidence_url = %(ev_u)s,
        group_abn_entity_name = COALESCE(%(en)s, group_abn_entity_name),
        group_abn_lookup_attempted_at = NOW(),
        group_abn_notes = %(notes)s
    WHERE group_id = %(gid)s
      AND group_abn_status = 'multiple_candidates'
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "gid": group_id,
                "abn": group_abn,
                "st": status,
                "ev_m": evidence_method,
                "ev_u": evidence_url,
                "en": entity_name,
                "notes": notes,
            },
        )


def fetch_page(
    client: httpx.Client,
    url: str,
    fetch_counter: list[int],
    cache_dir: Path,
    group_slug: str,
) -> tuple[str | None, int | None, str | None]:
    """Return ``(html, status_code, error_tag)``. Counts toward global fetch cap on attempt."""
    if fetch_counter[0] >= MAX_FETCHES_GLOBAL:
        return None, None, "global_fetch_cap"

    time.sleep(IN_GROUP_DELAY_S)
    try:
        r = client.get(url, timeout=40.0, follow_redirects=True)
    except Exception as ex:
        LOG.warning("Timeout / network for %s: %s", url, ex)
        fetch_counter[0] += 1
        return None, None, "timeout"

    st = r.status_code
    if st >= 500:
        LOG.warning("Server error %s for %s", st, url)
        fetch_counter[0] += 1
        return None, st, "5xx"

    if st == 403:
        body_l = (r.text or "")[:8000].lower()
        cf_hdr = (r.headers.get("cf-ray") or "").lower()
        if "cloudflare" in body_l or "cf-ray" in cf_hdr:
            fetch_counter[0] += 1
            return None, st, "cloudflare"
        fetch_counter[0] += 1
        return None, st, "403"

    if st >= 400:
        LOG.warning("Client error %s for %s", st, url)
        fetch_counter[0] += 1
        return None, st, "4xx"

    fetch_counter[0] += 1
    html = r.text
    slug = page_slug_from_url(url)
    try:
        (cache_dir / f"{slug}.html").write_text(
            html, encoding="utf-8", errors="replace"
        )
    except OSError as ex:
        LOG.debug("Cache write failed: %s", ex)
    return html, st, None


def deep_crawl_group(
    client: httpx.Client,
    row: dict[str, Any],
    fetch_counter: list[int],
    legal_name: str,
) -> tuple[
    str | None,
    str | None,
    str | None,
    int,
    list[dict[str, Any]],
    str | None,
]:
    """
    Returns abn, evidence_method, evidence_url, pages_fetched, all_hits, abort_reason.
    """
    web = (row.get("group_website") or "").strip()
    if not web.startswith("http"):
        return None, None, None, 0, [], "no_website"

    base = urlparse(web)
    root = f"{base.scheme}://{base.netloc}/"
    cache_dir = _deep_cache_dir(row["group_slug"])

    seeds: list[str] = []
    for s in (web, root):
        c = canon_url(s)
        if c not in seeds:
            seeds.append(c)

    visited: set[str] = set()
    queue: list[str] = list(seeds)

    cf_hits = 0
    fivexx = 0
    pages_this_group = 0
    all_hits: list[dict[str, Any]] = []

    while queue and pages_this_group < MAX_PAGES_PER_GROUP:
        if fetch_counter[0] >= MAX_FETCHES_GLOBAL:
            break
        url = queue.pop(0)
        c_url = canon_url(url)
        if c_url in visited:
            continue
        visited.add(c_url)

        html, _st, err = fetch_page(
            client, c_url, fetch_counter, cache_dir, row["group_slug"]
        )
        if err == "cloudflare":
            cf_hits += 1
            if cf_hits >= CLOUDFLARE_ABORT_THRESHOLD:
                LOG.warning(
                    "Aborting deep crawl for %s (Cloudflare / bot protection)",
                    row["group_slug"],
                )
                return None, None, None, pages_this_group, all_hits, "cloudflare"
            continue
        if err == "5xx":
            fivexx += 1
            if fivexx >= SERVER_ERROR_ABORT_THRESHOLD:
                LOG.warning(
                    "Aborting deep crawl for %s (repeated server errors)",
                    row["group_slug"],
                )
                return None, None, None, pages_this_group, all_hits, "server_errors"
            continue
        if html is None:
            continue

        pages_this_group += 1

        path = urlparse(c_url).path or "/"
        hits = extract_findings_from_html(html, c_url, path)
        all_hits.extend(hits)

        soup = BeautifulSoup(html, "lxml")
        cur_p = urlparse(c_url)
        for a in soup.find_all("a", href=True):
            nu = normalise_href(cur_p, a["href"])
            if not nu:
                continue
            pu = urlparse(nu)
            if not same_site(base, pu):
                continue
            nu_c = canon_url(nu)
            if nu_c in visited:
                continue
            ppath = pu.path or "/"
            qry = (pu.query or "").lower()
            if DEEP_PATH_HINT.search(ppath) or DEEP_PATH_HINT.search(qry):
                if nu_c not in queue:
                    queue.append(nu_c)

    if not all_hits:
        return None, None, None, pages_this_group, all_hits, None

    agg = aggregate_scores(all_hits, legal_name)
    rows = agg["ranked_rows"]
    if not rows:
        return None, None, None, pages_this_group, all_hits, None

    top = rows[0]
    win_abn = top["abn"]
    best_hit = top["best_hit"]
    ev_url = best_hit["page_url"]
    ev_m = best_hit["evidence_method"]
    return win_abn, ev_m, ev_url, pages_this_group, all_hits, None


def try_asic_company_lookup(
    headers: dict[str, str],
    organisation_name: str,
    hq_state: str | None,
    fetch_counter: list[int],
) -> tuple[str | None, str | None]:
    """Best-effort ASIC Connect search (JSF). Returns ``(abn, detail_url)``."""
    if fetch_counter[0] >= MAX_FETCHES_GLOBAL:
        return None, None
    if not organisation_name:
        return None, None

    with httpx.Client(
        headers=headers,
        verify=_asic_ssl_context(),
        follow_redirects=True,
    ) as ac:
        time.sleep(IN_GROUP_DELAY_S)
        try:
            r = ac.get(ASIC_LANDING, timeout=35.0)
            fetch_counter[0] += 1
        except Exception as ex:
            LOG.info("ASIC landing fetch failed: %s", ex)
            return None, None

        if r.status_code >= 400:
            LOG.info("ASIC landing HTTP %s", r.status_code)
            return None, None

        soup = BeautifulSoup(r.text, "lxml")
        vs = soup.find(
            "input", attrs={"name": re.compile(r"javax\.faces\.ViewState", re.I)}
        )
        view_state = vs.get("value") if vs else None
        form = soup.find("form")
        if not form or not view_state:
            LOG.info(
                "ASIC Connect: no JSF ViewState parsed; skipping automated search."
            )
            return None, None

        action = form.get("action")
        if not action:
            return None, None
        post_url = urljoin(ASIC_LANDING, action)

        inputs: dict[str, str] = {}
        for inp in form.find_all("input"):
            nm = inp.get("name")
            if not nm:
                continue
            inputs[nm] = inp.get("value") or ""

        guess_name = organisation_name[:120]
        filled = False
        for cand in (
            re.compile(r"nameOrNumber", re.I),
            re.compile(r"orgName", re.I),
            re.compile(r"SearchForm:name", re.I),
            re.compile(r":name", re.I),
        ):
            for k in list(inputs.keys()):
                if cand.search(k) and "Number" not in k:
                    inputs[k] = guess_name
                    filled = True
                    break
            if filled:
                break

        if not filled:
            for inp in form.find_all("input"):
                itype = (inp.get("type") or "text").lower()
                nm = inp.get("name") or ""
                if not nm:
                    continue
                nl = nm.lower()
                if itype not in ("text", "search"):
                    continue
                if any(
                    x in nl
                    for x in (
                        "number",
                        "family",
                        "given",
                        "surname",
                        "firstname",
                        "last",
                        "middle",
                    )
                ):
                    continue
                if "name" in nl or "organisation" in nl or nl.endswith(":orgname"):
                    inputs[nm] = guess_name
                    filled = True
                    break

        if not filled:
            LOG.info(
                "ASIC Connect: could not map organisation name field; skipping POST."
            )
            return None, None

        inputs["javax.faces.ViewState"] = view_state

        try:
            time.sleep(IN_GROUP_DELAY_S)
            pr = ac.post(post_url, data=inputs, timeout=45.0)
            fetch_counter[0] += 1
        except Exception as ex:
            LOG.info("ASIC POST failed: %s", ex)
            return None, None

        body = pr.text or ""
        if hq_state:
            st = hq_state.strip().upper()[:3]
            if st and st not in body.upper():
                pass

        for m in ABN_LABELLED.finditer(body):
            d = normalise_abn_digits(m.group(1))
            if validate_abn(d):
                return d, post_url
        for m in DIGITS_11.finditer(body):
            d = m.group(1)
            if validate_abn(d) and abn_context_ok(body, m.start(), m.end()):
                return d, post_url

        return None, None


def main() -> None:
    db = os.getenv("DATABASE_URL", "").strip()
    if not db:
        raise SystemExit("DATABASE_URL missing in env.local")

    started = time.monotonic()
    fetch_counter = [0]
    stats = {
        "groups": 0,
        "verified": 0,
        "probable_asic": 0,
        "still_multiple": 0,
        "errors": 0,
    }

    headers = {"User-Agent": USER_AGENT}
    row_ix = 0
    with httpx.Client(headers=headers) as client, psycopg.connect(
        db, autocommit=False
    ) as conn:
        targets = load_targets(conn)
        LOG.info("Deep lookup targets: %s", len(targets))

        for row in targets:
            if time.monotonic() - started > WALL_CLOCK_BUDGET_S:
                LOG.error("90-minute wall clock exceeded; stopping.")
                break

            row_ix += 1
            if row_ix > 1:
                time.sleep(BETWEEN_GROUPS_DELAY_S)

            gid = row["group_id"]
            slug = row["group_slug"]
            legal_name = legal_reference(row)
            existing_notes = row.get("group_abn_notes")

            try:
                abn, ev_m, ev_u, pages_n, hits, abort = deep_crawl_group(
                    client, row, fetch_counter, legal_name
                )

                deep_payload: dict[str, Any] = {
                    "pages_fetched_estimate": pages_n,
                    "hits_count": len(hits),
                    "abort": abort,
                }

                if abn:
                    ranking = aggregate_scores(hits, legal_name)
                    top_s = (
                        ranking["ranked_rows"][0]["total"]
                        if ranking["ranked_rows"]
                        else 0
                    )
                    summary = (
                        f"Phase 3.5 deep crawl — found via deeper page research. "
                        f"Pages searched: {pages_n}, ABN candidates found: {len(hits)}, "
                        f"winner score: {top_s}."
                    )
                    deep_payload["summary"] = summary
                    deep_payload["winner_abn"] = abn
                    deep_payload["winner_score_components_total"] = top_s
                    deep_payload["audit_hits_sample"] = hits[:25]

                    notes = merge_notes(
                        existing_notes,
                        deep_payload,
                    )
                    persist_upgrade(
                        conn,
                        gid,
                        group_abn=abn,
                        status="verified",
                        evidence_method=ev_m,
                        evidence_url=ev_u,
                        entity_name=legal_name or None,
                        notes=notes,
                    )
                    conn.commit()
                    stats["verified"] += 1
                    stats["groups"] += 1
                    LOG.info("Upgraded %s → verified (ABN %s)", slug, abn)
                    continue

                asic_abn, asic_url = try_asic_company_lookup(
                    headers,
                    legal_name,
                    row.get("group_hq_state"),
                    fetch_counter,
                )
                if asic_abn:
                    deep_payload["asic"] = True
                    deep_payload["summary"] = (
                        "Phase 3.5 — ASIC Connect search surfaced a probable ABN match."
                    )
                    notes = merge_notes(existing_notes, deep_payload)
                    persist_upgrade(
                        conn,
                        gid,
                        group_abn=asic_abn,
                        status="probable",
                        evidence_method="asic_company_search",
                        evidence_url=asic_url,
                        entity_name=legal_name or None,
                        notes=notes,
                    )
                    conn.commit()
                    stats["probable_asic"] += 1
                    LOG.info("Upgraded %s → probable via ASIC", slug)
                else:
                    deep_payload["summary"] = (
                        "Phase 3.5 deep crawl — no definitive ABN. "
                        f"Pages tried: {pages_n}, hits: {len(hits)}, abort={abort}."
                    )
                    deep_payload["audit_hits_sample"] = hits[:15]
                    notes = merge_notes(existing_notes, deep_payload)
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE public.venue_groups
                            SET group_abn_lookup_attempted_at = NOW(),
                                group_abn_notes = %(notes)s
                            WHERE group_id = %(gid)s
                              AND group_abn_status = 'multiple_candidates'
                            """,
                            {"gid": gid, "notes": notes},
                        )
                    conn.commit()
                    stats["still_multiple"] += 1
                    LOG.info(
                        "No upgrade for %s (pages=%s, hits=%s)",
                        slug,
                        pages_n,
                        len(hits),
                    )

                stats["groups"] += 1
            except Exception as ex:
                conn.rollback()
                stats["errors"] += 1
                LOG.exception("Group %s failed: %s", slug, ex)

    elapsed = time.monotonic() - started
    LOG.info(
        "Deep lookup finished in %.1f s; fetches=%s; stats=%s",
        elapsed,
        fetch_counter[0],
        stats,
    )


if __name__ == "__main__":
    main()
