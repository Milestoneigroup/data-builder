"""Resolve operator-group ABNs: website scrape (Method A), then ABR MatchingNames (Method B).

For each ``venue_groups`` row with ``abn_lookup_strategy='operator_group_lookup'`` where
``group_abn_lookup_attempted_at`` is null (and status is not ``skipped``), fetch the
group website, extract and checksum-validate ABNs, or fall back to the ABR JSON API.

Run: ``python scrapers/group_abn_lookup.py``

Requires: ``DATABASE_URL``, ``ABN_GUID`` in ``env.local`` (``load_dotenv(..., override=True)``).

Etiquette: 1 s between site fetches, 0.5 s between ABR calls; hard cap 50 ABR HTTP calls
total; 90-minute wall clock budget for this run.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rapidfuzz import fuzz
import psycopg
from psycopg.types.json import Json

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger("group_abn_lookup")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

USER_AGENT = "MilestoneInnovationsGroup-DataBuilder (richard@milestoneigroup.com)"
ABR_MATCH_URL = "https://abr.business.gov.au/json/MatchingNames.aspx"
SITE_DELAY_S = 1.0
ABR_DELAY_S = 0.5
MAX_ABR_CALLS = 50
WALL_CLOCK_BUDGET_S = 90 * 60
MAX_CONSECUTIVE_DB_FAILURES = 5

SUBPAGE_PATHS = (
    "",
    "/about",
    "/about-us",
    "/contact",
    "/terms",
    "/terms-of-use",
    "/privacy",
    "/privacy-policy",
)

WEIGHTS = (10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19)

ABN_LABELLED = re.compile(
    r"\bABN[:\s]*(\d{2}\s?\d{3}\s?\d{3}\s?\d{3})\b", re.IGNORECASE
)
DIGITS_11 = re.compile(r"\b(\d{11})\b")


def _cache_dir() -> Path:
    base = Path("/tmp/abn_cache/groups")
    try:
        base.mkdir(parents=True, exist_ok=True)
    except OSError:
        import tempfile

        base = Path(tempfile.gettempdir()) / "abn_cache" / "groups"
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


def pairs_labelled_then_context(buffer: str, ev_method: str) -> list[tuple[str, str, str]]:
    """Return ``(abn_digits, snippet, ev_method)`` in order: labelled matches, then 11-digit."""
    out: list[tuple[str, str, str]] = []
    for m in ABN_LABELLED.finditer(buffer):
        d = normalise_abn_digits(m.group(1))
        if not validate_abn(d):
            continue
        lo, hi = m.start(), m.end()
        sn = buffer[max(0, lo - 50) : min(len(buffer), hi + 50)]
        out.append((d, sn, ev_method))
    for m in DIGITS_11.finditer(buffer):
        d = m.group(1)
        if not validate_abn(d) or not abn_context_ok(buffer, m.start(), m.end()):
            continue
        lo, hi = m.start(), m.end()
        sn = buffer[max(0, lo - 50) : min(len(buffer), hi + 50)]
        out.append((d, sn, ev_method))
    return out


def evidence_for_path(path: str) -> str:
    p = path.lower().rstrip("/") or "/"
    if p in ("/about", "/about-us"):
        return "website_about_page"
    if p in ("/terms", "/terms-of-use"):
        return "website_terms_page"
    if p in ("/privacy", "/privacy-policy"):
        return "website_homepage"
    if p in ("/contact",):
        return "website_homepage"
    return "website_homepage"


def legal_guess_name(row: dict[str, Any]) -> str:
    gl = (row.get("group_legal_name") or "").strip()
    if gl:
        return gl
    return (row.get("group_name") or "").strip()


def pick_abn_from_snippets(
    triples: list[tuple[str, str, str]], reference: str
) -> tuple[str | None, str | None, str | None, bool]:
    """Pick best ABN via fuzzy match of *reference* to contextual snippet.

    Returns ``(abn, ev_method, evidence_url placeholder handled by caller, ambiguous)``.
    """
    if not triples:
        return None, None, None, False
    by_abn: dict[str, tuple[str, str, int]] = {}
    ref_low = (reference or "").lower()
    for abn, snippet, ev in triples:
        score = fuzz.partial_ratio(ref_low, snippet.lower()) if ref_low else 0
        prev = by_abn.get(abn)
        if prev is None or score > prev[2]:
            by_abn[abn] = (snippet, ev, score)
    items = sorted(by_abn.items(), key=lambda kv: -kv[1][2])
    if len(items) == 1:
        abn = items[0][0]
        return abn, items[0][1][1], None, False
    top_score = items[0][1][2]
    second_score = items[1][1][2]
    if top_score == second_score:
        return None, None, None, True
    abn = items[0][0]
    return abn, items[0][1][1], None, False


def fetch_html(client: httpx.Client, url: str) -> str | None:
    try:
        r = client.get(url, timeout=45.0)
        r.raise_for_status()
        return r.text
    except Exception as ex:
        LOG.warning("Fetch failed %s: %s", url, ex)
        return None


def method_a_website(
    client: httpx.Client,
    base_url: str,
    group_slug: str,
    reference_name: str,
) -> tuple[str | None, str | None, str | None, list[str], dict[str, Any] | None]:
    """Returns abn, evidence_method, evidence_url, all_raw_abns, notes_dict if ambiguous."""
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        return None, None, None, [], None

    cache_dir = _cache_dir()
    root = f"{parsed.scheme}://{parsed.netloc}"

    for sub in SUBPAGE_PATHS:
        path = sub or "/"
        url = urljoin(root + "/", path.lstrip("/"))
        time.sleep(SITE_DELAY_S)
        html = fetch_html(client, url)
        if not html:
            continue
        cache_file = cache_dir / f"{group_slug}{sub.replace('/', '_') or '_root'}.html"
        try:
            cache_file.write_text(html, encoding="utf-8", errors="replace")
        except OSError as ex:
            LOG.debug("Cache write skipped: %s", ex)

        soup = BeautifulSoup(html, "lxml")
        footer = soup.find("footer")
        footer_text = footer.get_text(" ", strip=True) if footer else ""
        path_ev = evidence_for_path(sub)

        triples: list[tuple[str, str, str]] = []
        seen_abn: set[str] = set()

        if footer_text:
            for d, sn, _ev in pairs_labelled_then_context(footer_text, "website_footer"):
                if d not in seen_abn:
                    seen_abn.add(d)
                    triples.append((d, sn, "website_footer"))

        for d, sn, _ev in pairs_labelled_then_context(html, path_ev):
            if d not in seen_abn:
                seen_abn.add(d)
                triples.append((d, sn, path_ev))

        if not triples:
            continue

        raw_abns = list(dict.fromkeys(t[0] for t in triples))
        if len(raw_abns) == 1:
            abn_only = raw_abns[0]
            ev_m = next(t[2] for t in triples if t[0] == abn_only)
            return abn_only, ev_m, url, raw_abns, None

        chosen, ev_method, _, amb = pick_abn_from_snippets(triples, reference_name)
        if amb or chosen is None:
            return (
                None,
                None,
                url,
                raw_abns,
                {
                    "reason": "multiple_valid_abns",
                    "abns": raw_abns,
                    "page_url": url,
                },
            )
        ev_for_chosen = next(t[2] for t in triples if t[0] == chosen)
        return chosen, ev_for_chosen, url, raw_abns, None

    return None, None, None, [], None


def parse_jsonp(raw: str) -> dict[str, Any]:
    m = re.match(r"^callback\((.*)\)\s*$", raw.strip(), re.DOTALL)
    if not m:
        raise ValueError("Unexpected ABR response (not JSONP)")
    return json.loads(m.group(1))


def abr_get(client: httpx.Client, url: str, params: dict[str, Any], counter: list[int]) -> str:
    if counter[0] >= MAX_ABR_CALLS:
        raise RuntimeError("ABR call budget exhausted")
    time.sleep(ABR_DELAY_S)
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            r = client.get(url, params=params, timeout=45.0)
            if r.status_code == 429:
                if attempt == 0:
                    LOG.warning("ABR 429; backing off 30 s")
                    time.sleep(30)
                    continue
                r.raise_for_status()
            r.raise_for_status()
            counter[0] += 1
            return r.text
        except Exception as ex:
            last_exc = ex
            if attempt == 0:
                time.sleep(30)
                continue
            raise last_exc from ex
    raise RuntimeError(last_exc)


def method_b_abr(
    client: httpx.Client,
    name: str,
    hq_state: str | None,
    guid: str,
    abr_counter: list[int],
) -> tuple[
    str | None,
    str,
    str | None,
    str | None,
    str | None,
    dict[str, Any] | None,
]:
    """ABR MatchingNames only (counts **one** call). ``EntityTypeCode`` is not returned here.

    Sole-trader filtering via ``EntityTypeCode='IND'`` needs AbnDetails, which would blow the
    50-call budget when combined with 42 groups; ``NameType`` / name heuristics are used instead.
    """
    notes: dict[str, Any] = {}
    try:
        text = abr_get(
            client,
            ABR_MATCH_URL,
            {"name": name, "maxResults": "10", "guid": guid},
            abr_counter,
        )
        data = parse_jsonp(text)
    except RuntimeError as ex:
        LOG.error("ABR MatchingNames failed: %s", ex)
        return None, "not_found", None, None, None, {"error": str(ex)}
    except Exception as ex:
        LOG.exception("ABR MatchingNames error: %s", ex)
        return None, "not_found", None, None, None, {"error": str(ex)}

    names = data.get("Names") or []
    if not names:
        return None, "not_found", None, None, None, None

    def state_rank(n: dict[str, Any]) -> int:
        st = (n.get("State") or "").upper()
        if hq_state and st == hq_state.upper():
            return 2
        if st:
            return 1
        return 0

    def type_rank(n: dict[str, Any]) -> int:
        nt = (n.get("NameType") or "").lower()
        if "entity" in nt:
            return 2
        return 1

    def score_rank(n: dict[str, Any]) -> int:
        try:
            return int(n.get("Score") or 0)
        except (TypeError, ValueError):
            return 0

    ranked = sorted(
        names,
        key=lambda n: (state_rank(n), type_rank(n), score_rank(n)),
        reverse=True,
    )

    shortlist: list[dict[str, Any]] = []
    for item in ranked[:10]:
        abn = normalise_abn_digits(str(item.get("Abn", "")))
        if len(abn) != 11 or not validate_abn(abn):
            continue
        nm = str(item.get("Name") or "")
        if _looks_like_sole_trader_match(nm, item):
            continue
        shortlist.append(item)

    if not shortlist:
        notes["top_raw"] = [
            {"Abn": x.get("Abn"), "Name": x.get("Name")} for x in ranked[:5]
        ]
        return None, "not_found", None, None, None, notes

    if len(shortlist) == 1:
        item = shortlist[0]
        abn = normalise_abn_digits(str(item.get("Abn")))
        ev = (
            "abr_legal_name_state_match"
            if hq_state
            and (item.get("State") or "").upper() == hq_state.upper()
            else "abr_legal_name_match"
        )
        en = str(item.get("Name") or "") or None
        et = str(item.get("NameType") or "") or None
        return abn, "probable", ev, en, et, None

    norm_names = {str(x.get("Name", "")).strip().lower() for x in shortlist}
    if 2 <= len(shortlist) <= 5 and len(norm_names) <= 2:
        shortest = min(shortlist, key=lambda x: len(str(x.get("Name", ""))))
        abn = normalise_abn_digits(str(shortest.get("Abn")))
        notes["considered"] = [
            {"Abn": x.get("Abn"), "Name": x.get("Name")} for x in shortlist
        ]
        en = str(shortest.get("Name") or "") or None
        et = str(shortest.get("NameType") or "") or None
        ev = (
            "abr_legal_name_state_match"
            if hq_state
            and (shortest.get("State") or "").upper() == hq_state.upper()
            else "abr_legal_name_match"
        )
        return abn, "probable", ev, en, et, notes

    if len(shortlist) >= 6:
        notes["top"] = [
            {"Abn": x.get("Abn"), "Name": x.get("Name")} for x in shortlist[:5]
        ]
        return None, "multiple_candidates", None, None, None, notes

    notes["candidates"] = [
        {"Abn": x.get("Abn"), "Name": x.get("Name")} for x in shortlist[:5]
    ]
    return None, "multiple_candidates", None, None, None, notes


def _looks_like_sole_trader_match(name: str, item: dict[str, Any]) -> bool:
    """Fallback filter when AbnDetails cannot be called (50-call budget)."""
    nt = (item.get("NameType") or "").lower()
    if "business name" in nt and "pty" not in name.lower() and "ltd" not in name.lower():
        parts = name.replace(".", "").split()
        if len(parts) == 2 and all(p[:1].isupper() for p in parts if p):
            return True
    return False


def load_groups(conn: psycopg.Connection) -> list[dict[str, Any]]:
    sql = """
    SELECT group_id, group_slug, group_website, group_legal_name, group_name,
           group_hq_state, group_abn_status
    FROM public.venue_groups
    WHERE abn_lookup_strategy = 'operator_group_lookup'
      AND group_abn_lookup_attempted_at IS NULL
      AND COALESCE(group_abn_status, '') <> 'skipped'
    ORDER BY group_slug
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def persist_group(
    conn: psycopg.Connection,
    group_id: str,
    *,
    group_abn: str | None,
    group_abn_status: str,
    evidence_method: str | None,
    evidence_url: str | None,
    entity_name: str | None,
    entity_type: str | None,
    notes: dict[str, Any] | None,
) -> None:
    sql = """
    UPDATE public.venue_groups
    SET group_abn = %(abn)s,
        group_abn_status = %(st)s,
        group_abn_evidence_method = %(ev_m)s,
        group_abn_evidence_url = %(ev_u)s,
        group_abn_entity_name = %(en)s,
        group_abn_entity_type = %(et)s,
        group_abn_lookup_attempted_at = NOW(),
        group_abn_notes = %(notes)s,
        updated_at = NOW()
    WHERE group_id = %(gid)s
    """
    notes_json = Json(notes) if notes else None
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "gid": group_id,
                "abn": group_abn,
                "st": group_abn_status,
                "ev_m": evidence_method,
                "ev_u": evidence_url,
                "en": entity_name,
                "et": entity_type,
                "notes": notes_json,
            },
        )


def main() -> None:
    db = os.getenv("DATABASE_URL", "").strip()
    guid = os.getenv("ABN_GUID", "").strip()
    if not db:
        raise SystemExit("DATABASE_URL is required in env.local")
    if not guid:
        raise SystemExit("ABN_GUID is required in env.local")

    started = time.monotonic()
    abr_counter = [0]
    consec_fail = 0
    outcomes: dict[str, int] = {}

    headers = {"User-Agent": USER_AGENT}
    with (
        httpx.Client(headers=headers, follow_redirects=True) as client,
        psycopg.connect(db, autocommit=False) as conn,
    ):
        groups = load_groups(conn)
        LOG.info("Groups to process: %s", len(groups))

        for row in groups:
            if time.monotonic() - started > WALL_CLOCK_BUDGET_S:
                LOG.error("Wall-clock budget (90 min) exceeded; stopping.")
                break

            gid = row["group_id"]
            slug = row["group_slug"]
            reference = legal_guess_name(row)
            hq_state = row.get("group_hq_state")

            try:
                web = (row.get("group_website") or "").strip()
                abn: str | None = None
                ev_method: str | None = None
                ev_url: str | None = None
                notes: dict[str, Any] | None = None
                entity_name: str | None = None
                entity_type: str | None = None
                status: str = "not_found"

                if web.startswith("http"):
                    abn, ev_method, ev_url, _raw, amb_notes = method_a_website(
                        client, web, slug, reference
                    )
                    if amb_notes:
                        notes = amb_notes
                        status = "multiple_candidates"
                    elif abn:
                        status = "verified"

                if abn is None and status != "multiple_candidates":
                    if abr_counter[0] >= MAX_ABR_CALLS:
                        LOG.warning("ABR budget exhausted before Method B for %s", slug)
                        status = "not_found"
                        notes = {"reason": "abr_budget_exhausted"}
                    elif not reference:
                        status = "not_found"
                        notes = {"reason": "no_legal_name_for_abr"}
                    else:
                        b_abn, b_status, b_ev, b_en, b_et, b_notes = method_b_abr(
                            client, reference, hq_state, guid, abr_counter
                        )
                        if b_notes:
                            notes = {**(notes or {}), **b_notes}
                        if b_abn:
                            abn = b_abn
                            status = b_status
                            ev_method = b_ev
                            ev_url = ABR_MATCH_URL
                            entity_name = b_en
                            entity_type = b_et
                        else:
                            status = b_status
                            if not notes and b_notes:
                                notes = b_notes

                if status == "verified" and abn:
                    entity_name = entity_name or reference
                    persist_group(
                        conn,
                        gid,
                        group_abn=abn,
                        group_abn_status="verified",
                        evidence_method=ev_method,
                        evidence_url=ev_url,
                        entity_name=entity_name,
                        entity_type=entity_type,
                        notes=notes,
                    )
                elif status == "probable" and abn:
                    persist_group(
                        conn,
                        gid,
                        group_abn=abn,
                        group_abn_status="probable",
                        evidence_method=ev_method,
                        evidence_url=ev_url,
                        entity_name=entity_name,
                        entity_type=entity_type,
                        notes=notes,
                    )
                elif status == "multiple_candidates":
                    persist_group(
                        conn,
                        gid,
                        group_abn=None,
                        group_abn_status="multiple_candidates",
                        evidence_method=None,
                        evidence_url=ev_url,
                        entity_name=None,
                        entity_type=None,
                        notes=notes,
                    )
                else:
                    persist_group(
                        conn,
                        gid,
                        group_abn=None,
                        group_abn_status="not_found",
                        evidence_method=None,
                        evidence_url=None,
                        entity_name=None,
                        entity_type=None,
                        notes=notes,
                    )

                conn.commit()
                consec_fail = 0
                outcomes[status] = outcomes.get(status, 0) + 1
                LOG.info(
                    "Updated %s status=%s abn=%s (ABR calls so far=%s)",
                    slug,
                    status,
                    abn,
                    abr_counter[0],
                )
            except Exception as ex:
                conn.rollback()
                consec_fail += 1
                LOG.exception("Failed row %s: %s", slug, ex)
                if consec_fail >= MAX_CONSECUTIVE_DB_FAILURES:
                    raise SystemExit(
                        f"Stopping: {MAX_CONSECUTIVE_DB_FAILURES} consecutive failures"
                    ) from ex

    elapsed = time.monotonic() - started
    LOG.info(
        "W1 complete in %.1f s; ABR HTTP calls=%s; outcomes=%s",
        elapsed,
        abr_counter[0],
        outcomes,
    )


if __name__ == "__main__":
    main()
