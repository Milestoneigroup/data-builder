"""Council permit enrichment: ref_councils + site-search, then HTML extraction."""

from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

LOG_PATH = _ROOT / "logs" / "council_permit_enrichment.log"
MAX_PAGE_CHARS = 70_000
HTTP_DELAY_S = 0.4
CLAUDE_DELAY_S = 0.5
LINK_HINTS = ("permit", "event", "park", "wedding", "outdoor", "reserve", "recreation", "ceremony", "application", "licence", "book")


def _setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("council_permit_enrichment")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    return log


def _load_env() -> None:
    for p in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if p.is_file():
            load_dotenv(p, override=True, encoding="utf-8")


def _parse_json(text: str) -> dict[str, Any]:
    t = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", t)
    if m:
        t = m.group(1).strip()
    return json.loads(t)


def _norm_text(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in {"null", "none", "n/a", "unknown"}:
        return None
    return s


def _norm_bool(v: Any) -> bool | None:
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"true", "yes", "1"}:
        return True
    if s in {"false", "no", "0"}:
        return False
    return None


def _norm_int(v: Any) -> int | None:
    if isinstance(v, int):
        return v
    if v is None:
        return None
    d = re.sub(r"[^\d-]", "", str(v))
    if not d:
        return None
    try:
        return int(d)
    except ValueError:
        return None


def _norm_data_confidence(v: Any) -> str:
    """DB check: high | medium | low only."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        x = float(v)
        if x > 1.0:
            if x >= 67:
                return "high"
            if x >= 34:
                return "medium"
            return "low"
        if x >= 0.67:
            return "high"
        if x >= 0.34:
            return "medium"
        return "low"
    s = (_norm_text(str(v)) or "").lower().strip()
    if s in ("high", "medium", "low"):
        return s
    if s in ("very high", "excellent", "strong"):
        return "high"
    if s in ("moderate", "fair", "average"):
        return "medium"
    return "low"


def _norm_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if v is None:
        return []
    return [x.strip() for x in re.split(r"[,;\n]+", str(v)) if x.strip()]


def _council_domain(website: str) -> str:
    p = urlparse(website)
    h = (p.netloc or "").lower()
    if h.startswith("www."):
        h = h[4:]
    return h


def _origin_home_url(url: str) -> str:
    p = urlparse((url or "").strip())
    if not p.netloc:
        return ""
    sc = p.scheme if p.scheme in ("http", "https") else "https"
    return f"{sc}://{p.netloc}/"


def _valid_http_url(url: str | None) -> bool:
    if not url or not isinstance(url, str):
        return False
    u = url.strip()
    return bool(re.match(r"^https?://", u, re.I))


def _parse_fee_aud_numeric(s: str | None) -> float | None:
    """Rough AUD amount from free-text fee (single value or range midpoint)."""
    if not s:
        return None
    t = re.sub(r"[$,\s]", "", str(s).lower())
    nums = re.findall(r"\d+(?:\.\d+)?", t)
    if not nums:
        return None
    vals = [float(x) for x in nums[:4]]
    if len(vals) >= 2 and ("-" in s or " to " in t.lower()):
        return (vals[0] + vals[1]) / 2.0
    return sum(vals) / len(vals)


def _fetch_html(client: httpx.Client, url: str, timeout_s: float, ua: str, log: logging.Logger) -> str | None:
    try:
        r = client.get(
            url,
            follow_redirects=True,
            timeout=timeout_s,
            headers={
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-AU,en;q=0.9",
            },
        )
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001
        log.warning("Fetch failed url=%s err=%s", url, e)
        return None


def _fetch_html_playwright(url: str, user_agent: str, log: logging.Logger) -> str | None:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        log.debug("Playwright N/A: %s", e)
        return None
    try:
        with sync_playwright() as p:
            b = p.chromium.launch(headless=True)
            page = b.new_page(user_agent=user_agent)
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
            h = page.content()
            b.close()
            return h
    except Exception as e:  # noqa: BLE001
        log.warning("Playwright failed url=%s err=%s", url, e)
        return None


def _fetch_resilient(client: httpx.Client, url: str, timeout_s: float, user_agent: str, log: logging.Logger) -> str | None:
    html = _fetch_html(client, url, timeout_s, user_agent, log)
    if html:
        return html
    log.info("Trying Playwright for url=%s", url)
    return _fetch_html_playwright(url, user_agent, log)


def _snapshot(url: str, html: str) -> tuple[str, str, list[str]]:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.get_text(strip=True) if soup.title else "")[:300]
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    text = soup.get_text("\n", strip=True)
    if len(text) > MAX_PAGE_CHARS:
        text = text[:MAX_PAGE_CHARS] + "\n[TRUNCATED]"
    links: list[str] = []
    seen: set[str] = set()
    for a in soup.select("a[href]"):
        href = str(a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(url, href)
        if full in seen:
            continue
        seen.add(full)
        links.append(full)
    return title, text, links


def _permit_links(base_url: str, links: list[str]) -> list[str]:
    host = urlparse(base_url).netloc.lower()
    scored: list[tuple[int, str]] = []
    for link in links:
        p = urlparse(link)
        if p.scheme not in ("http", "https") or p.netloc.lower() != host:
            continue
        low = link.lower()
        score = sum(1 for k in LINK_HINTS if k in low)
        if score > 0:
            scored.append((score, link))
    scored.sort(key=lambda x: x[0], reverse=True)
    out: list[str] = []
    seen: set[str] = set()
    for _, link in scored:
        if link not in seen:
            seen.add(link)
            out.append(link)
        if len(out) >= 8:
            break
    return out


def _claude_text(msg: Any) -> str:
    parts: list[str] = []
    for b in getattr(msg, "content", []) or []:
        if hasattr(b, "text"):
            parts.append(str(b.text))
        elif isinstance(b, dict) and b.get("type") == "text":
            parts.append(str(b.get("text") or ""))
    return "".join(parts).strip()


def _permit_url_via_site_search(
    *, anthropic_key: str, model: str, council: dict[str, Any], log: logging.Logger
) -> str | None:
    from anthropic import Anthropic

    site = str(council.get("website") or "").strip()
    dom = _council_domain(site)
    if not dom:
        return None
    q1 = f"site:{dom} wedding permit"
    q2 = f"site:{dom} wedding ceremony park"
    q3 = f"site:{dom} event permit outdoor"
    prompt = f"""You are helping find the single best official page on a council website about outdoor / park wedding or event permits.

Council: {council.get("council_name")} (state: {council.get("state_code")})
Homepage: {site}
Domain for search: {dom}

Run web search for these queries (in order) and pick ONE best result URL on the council site:
1) {q1}
2) {q2}
3) {q3}

Prefer: permit / event application / public land / park booking / ceremony pages. Avoid: news, mayor, councillor, generic contact-only pages if a permit page exists.

Return JSON only:
{{"permit_page_url": "https://..."}} or {{"permit_page_url": null}} if no suitable page.
The URL must be on the same site (host contains {dom})."""
    try:
        ac = Anthropic(api_key=anthropic_key)
        msg = ac.messages.create(
            model=model,
            max_tokens=900,
            tools=[{"type": "web_search_20250305", "name": "web_search"}],
            messages=[{"role": "user", "content": prompt}],
        )
        data = _parse_json(_claude_text(msg))
        u = _norm_text(data.get("permit_page_url"))
        if not u or not re.match(r"^https?://", u, re.I):
            return None
        if dom not in u.lower() and _council_domain(u) != dom:
            log.warning("Search returned off-domain URL, ignoring: %s", u)
            return None
        return u
    except Exception as e:  # noqa: BLE001
        log.warning("Site search failed council=%r err=%s", council.get("council_id"), e)
        return None


def _extract_from_pages(
    *, anthropic_key: str, model: str, council: dict[str, Any], pages: list[dict[str, Any]]
) -> dict[str, Any] | None:
    from anthropic import Anthropic

    body_parts: list[str] = []
    for i, page in enumerate(pages, start=1):
        body_parts.append(
            f"PAGE {i}\nURL: {page['url']}\nTITLE: {page['title']}\nCONTENT:\n{page['text']}\n"
        )
    prompt = (
        "Extract outdoor wedding/event permit information from this council website content.\n"
        "Return JSON only with keys: "
        "council_name,state_code,permit_page_url,permit_required,permit_fee_aud,permit_lead_time_days,"
        "max_guests_outdoor,approved_locations,restricted_times,insurance_required,insurance_min_cover_aud,"
        "alcohol_permitted,caterers_approved_list,contact_name,contact_email,contact_phone,application_url,"
        "application_form_url,notes,data_confidence.\n"
        f"Council name: {council.get('council_name')}\nState: {council.get('state_code')}\n"
        "If unknown, return null.\n\n" + "\n\n".join(body_parts)
    )
    try:
        ac = Anthropic(api_key=anthropic_key)
        msg = ac.messages.create(model=model, max_tokens=1800, messages=[{"role": "user", "content": prompt}])
        data = _parse_json(_claude_text(msg))
    except Exception:
        return None

    return {
        "council_name": _norm_text(data.get("council_name")) or _norm_text(council.get("council_name")),
        "state_code": _norm_text(data.get("state_code")) or _norm_text(council.get("state_code")),
        "permit_page_url": _norm_text(data.get("permit_page_url")),
        "permit_required": _norm_bool(data.get("permit_required")),
        "permit_fee_aud": _norm_text(data.get("permit_fee_aud")),
        "permit_lead_time_days": _norm_int(data.get("permit_lead_time_days")),
        "max_guests_outdoor": _norm_int(data.get("max_guests_outdoor")),
        "approved_locations": _norm_list(data.get("approved_locations")),
        "restricted_times": _norm_text(data.get("restricted_times")),
        "insurance_required": _norm_bool(data.get("insurance_required")),
        "insurance_min_cover_aud": _norm_text(data.get("insurance_min_cover_aud")),
        "alcohol_permitted": _norm_bool(data.get("alcohol_permitted")),
        "caterers_approved_list": _norm_bool(data.get("caterers_approved_list")),
        "contact_name": _norm_text(data.get("contact_name")),
        "contact_email": _norm_text(data.get("contact_email")),
        "contact_phone": _norm_text(data.get("contact_phone")),
        "application_url": _norm_text(data.get("application_url")),
        "application_form_url": _norm_text(data.get("application_form_url")),
        "notes": _norm_text(data.get("notes")),
        "scraped_date": date.today().isoformat(),
        "data_confidence": _norm_data_confidence(data.get("data_confidence")),
    }


def _pick_mixed(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        buckets.setdefault(str(r.get("state_code") or "UNK"), []).append(r)
    states = sorted(buckets.keys())
    out: list[dict[str, Any]] = []
    i = 0
    while len(out) < n:
        moved = False
        for st in states:
            arr = buckets[st]
            if i < len(arr):
                out.append(arr[i])
                moved = True
                if len(out) >= n:
                    break
        if not moved:
            break
        i += 1
    return out


@dataclass
class PermitGroup:
    """One site-search + fetch + extract, upserted to every destination in the group."""

    key: str
    council_for_search: dict[str, Any]
    destinations: list[dict[str, Any]]
    council_id: str | None = None


def _fetch_all_destinations(sb: Any, log: logging.Logger) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    page = 500
    while True:
        try:
            res = (
                sb.schema("shared")
                .table("ref_destinations")
                .select("destination_id,destination_name,state_code,council_permit_url")
                .range(offset, offset + page - 1)
                .execute()
            )
        except Exception as e:  # noqa: BLE001
            log.error("ref_destinations page failed offset=%s err=%s", offset, e)
            break
        batch = res.data or []
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def _fetch_active_councils_with_website(sb: Any) -> list[dict[str, Any]]:
    res = (
        sb.schema("shared")
        .table("ref_councils")
        .select("council_id,council_name,state_code,website,aligned_destination_ids")
        .eq("is_active", True)
        .not_.is_("website", "null")
        .neq("website", "")
        .execute()
    )
    rows = res.data or []
    return [c for c in rows if re.match(r"^https?://", str(c.get("website") or "").strip(), re.I)]


def _build_permit_groups(
    destinations: list[dict[str, Any]], councils: list[dict[str, Any]], log: logging.Logger
) -> list[PermitGroup]:
    dest_by_id = {str(d["destination_id"]): d for d in destinations if d.get("destination_id")}
    council_by_dest: dict[str, dict[str, Any]] = {}
    for c in councils:
        home = str(c.get("website") or "").strip()
        if not home or not _council_domain(home):
            continue
        for did in c.get("aligned_destination_ids") or []:
            sid = str(did).strip()
            if not sid or sid not in dest_by_id:
                continue
            council_by_dest.setdefault(sid, c)

    aligned_ids = set(council_by_dest.keys())
    groups: list[PermitGroup] = []

    by_council: dict[str, list[dict[str, Any]]] = {}
    for did, c in council_by_dest.items():
        cid = str(c.get("council_id") or "")
        if not cid:
            continue
        by_council.setdefault(cid, []).append(dest_by_id[did])

    for cid, dests in by_council.items():
        c = next((x for x in councils if str(x.get("council_id")) == cid), None)
        if not c:
            continue
        home = str(c.get("website") or "").strip()
        if not home:
            continue
        groups.append(
            PermitGroup(
                key=f"council:{cid}",
                council_for_search={
                    "council_name": c.get("council_name"),
                    "state_code": c.get("state_code"),
                    "website": home,
                },
                destinations=dests,
                council_id=cid,
            )
        )

    orphan_by_dom: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for d in destinations:
        did = str(d.get("destination_id") or "")
        if not did or did in aligned_ids:
            continue
        pu = d.get("council_permit_url")
        if not _valid_http_url(str(pu or "")):
            continue
        dom = _council_domain(str(pu))
        if not dom:
            continue
        orphan_by_dom[dom].append(d)

    for dom, dests in orphan_by_dom.items():
        first = dests[0]
        pu = str(first.get("council_permit_url") or "").strip()
        home = _origin_home_url(pu) or f"https://{dom}/"
        groups.append(
            PermitGroup(
                key=f"permit_domain:{dom}",
                council_for_search={
                    "council_name": first.get("destination_name"),
                    "state_code": first.get("state_code"),
                    "website": home,
                },
                destinations=dests,
                council_id=None,
            )
        )

    log.info(
        "Permit groups: councils_with_aligned=%s permit_url_only_domains=%s total_groups=%s",
        len(by_council),
        len(orphan_by_dom),
        len(groups),
    )
    return groups


def _enrich_one_group(
    *,
    client: httpx.Client,
    grp: PermitGroup,
    anthropic_key: str,
    model: str,
    timeout_s: float,
    ua: str,
    log: logging.Logger,
) -> tuple[dict[str, Any] | None, str | None]:
    """Returns (extracted_row, best_search_url) or (None, None) on failure."""
    council = grp.council_for_search
    home = str(council.get("website") or "").strip()
    if not home:
        return None, None

    time.sleep(HTTP_DELAY_S)
    best = _permit_url_via_site_search(anthropic_key=anthropic_key, model=model, council=council, log=log)
    time.sleep(CLAUDE_DELAY_S)
    primary = best or home
    if best:
        log.info("Site-search primary URL: %s", best)

    html = _fetch_resilient(client, primary, timeout_s, ua, log)
    if not html and best and primary != home:
        log.info("Retrying with homepage after permit URL failed")
        html = _fetch_resilient(client, home, timeout_s, ua, log)
    time.sleep(HTTP_DELAY_S)
    if not html:
        log.warning("No HTML for group=%s", grp.key)
        return None, best

    title, text, links = _snapshot(primary, html)
    pages = [{"url": primary, "title": title, "text": text}]
    for link in _permit_links(primary, links)[:2]:
        h2 = _fetch_resilient(client, link, timeout_s, ua, log)
        time.sleep(HTTP_DELAY_S)
        if not h2:
            continue
        t2, txt2, _ = _snapshot(link, h2)
        pages.append({"url": link, "title": t2, "text": txt2})
    ex = _extract_from_pages(anthropic_key=anthropic_key, model=model, council=council, pages=pages)
    time.sleep(CLAUDE_DELAY_S)
    if not ex:
        return None, best
    if best and ex.get("permit_page_url") is None:
        ex["permit_page_url"] = best
    return ex, best


def main() -> None:
    parser = argparse.ArgumentParser(description="Council permit enrichment")
    parser.add_argument("--test-count", type=int, default=5)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--progress-every", type=int, default=10)
    args = parser.parse_args()

    _load_env()
    log = _setup_logging()
    from data_builder.config import get_settings
    from supabase import create_client

    settings = get_settings()
    sb = create_client((settings.supabase_url or "").strip(), (settings.supabase_service_role_key or "").strip())
    anthropic_key = (settings.anthropic_api_key or "").strip()
    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5-20250929").strip()
    timeout_s = max(45.0, float(settings.request_timeout_seconds or 45.0))
    ua = (settings.scraper_user_agent or "MilestoneDataBuilder/0.1").strip()
    dry_run = not args.apply

    destinations = _fetch_all_destinations(sb, log)
    councils = _fetch_active_councils_with_website(sb)
    all_groups = _build_permit_groups(destinations, councils, log)

    if dry_run:
        reps = [
            {"state_code": g.destinations[0].get("state_code"), "key": g.key, "n_dests": len(g.destinations)}
            for g in all_groups
            if g.destinations
        ]
        target = _pick_mixed(reps, min(args.test_count, len(reps)))
        keys = {t["key"] for t in target}
        groups = [g for g in all_groups if g.key in keys] if keys else all_groups[: args.test_count]
    else:
        groups = all_groups

    if not groups:
        print("No permit groups to process (no aligned destinations on ref_councils and no ref_destinations.council_permit_url).")
        print(f"Log: {LOG_PATH}")
        return

    results: list[tuple[PermitGroup, dict[str, Any]]] = []
    permits_found_groups = 0
    failed_groups = 0
    prog_every = max(1, int(args.progress_every))

    with httpx.Client() as client:
        for i, grp in enumerate(groups, start=1):
            log.info("[%s/%s] group=%s dests=%s site=%s", i, len(groups), grp.key, len(grp.destinations), grp.council_for_search.get("website"))
            ex, _best = _enrich_one_group(
                client=client,
                grp=grp,
                anthropic_key=anthropic_key,
                model=model,
                timeout_s=timeout_s,
                ua=ua,
                log=log,
            )
            if ex and _norm_text(ex.get("permit_page_url")):
                permits_found_groups += 1
            else:
                failed_groups += 1
            if ex:
                results.append((grp, ex))
            if i % prog_every == 0 or i == len(groups):
                print(f"Progress: {i}/{len(groups)} | Permits found: {permits_found_groups} | Failed: {failed_groups}")

    print("\n=== Sample extracted permit JSON (first groups) ===")
    for grp, r in results[: args.test_count]:
        payload = {**r, "destination_ids": [d.get("destination_id") for d in grp.destinations]}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        print("---")

    if dry_run:
        print("\nDRY RUN complete. No DB writes were made.")
        print(f"Log: {LOG_PATH}")
        return

    upserts = 0
    url_updates = 0
    insurance_true = 0
    lead_days: list[int] = []
    fees: list[float] = []
    online_app_dests = 0
    permit_page_dests = 0

    for grp, r in results:
        permit_url = _norm_text(r.get("permit_page_url"))
        app_url = _norm_text(r.get("application_url"))
        ins = r.get("insurance_required")
        lead = _norm_int(r.get("permit_lead_time_days"))
        fee_n = _parse_fee_aud_numeric(r.get("permit_fee_aud"))

        for d in grp.destinations:
            dest_id = d.get("destination_id")
            row = {
                "destination_id": dest_id,
                "council_name": r.get("council_name"),
                "state_code": r.get("state_code"),
                "permit_page_url": permit_url,
                "permit_required": r.get("permit_required"),
                "permit_fee_aud": r.get("permit_fee_aud"),
                "permit_lead_time_days": lead,
                "max_guests_outdoor": r.get("max_guests_outdoor"),
                "approved_locations": r.get("approved_locations") or [],
                "restricted_times": r.get("restricted_times"),
                "insurance_required": ins,
                "insurance_min_cover_aud": r.get("insurance_min_cover_aud"),
                "alcohol_permitted": r.get("alcohol_permitted"),
                "caterers_approved_list": r.get("caterers_approved_list"),
                "contact_name": r.get("contact_name"),
                "contact_email": r.get("contact_email"),
                "contact_phone": r.get("contact_phone"),
                "application_url": app_url,
                "application_form_url": r.get("application_form_url"),
                "notes": r.get("notes"),
                "scraped_date": r.get("scraped_date"),
                "data_confidence": _norm_data_confidence(r.get("data_confidence")),
            }
            sb.schema("shared").table("ref_council_permits").upsert(row, on_conflict="destination_id").execute()
            upserts += 1
            if permit_url:
                permit_page_dests += 1
                sb.schema("shared").table("ref_destinations").update({"council_permit_url": permit_url}).eq(
                    "destination_id", dest_id
                ).execute()
                url_updates += 1
            if ins is True:
                insurance_true += 1
            if lead is not None:
                lead_days.append(lead)
            if fee_n is not None:
                fees.append(fee_n)
            if app_url:
                online_app_dests += 1

    councils_loaded = 0
    try:
        cnt = (
            sb.schema("shared")
            .table("ref_councils")
            .select("council_id", count="exact")
            .eq("is_active", True)
            .limit(1)
            .execute()
        )
        councils_loaded = int(cnt.count or 0)
    except Exception as e:  # noqa: BLE001
        log.warning("Could not count ref_councils: %s", e)

    avg_fee = sum(fees) / len(fees) if fees else None
    avg_lead = sum(lead_days) / len(lead_days) if lead_days else None

    print("\n=== Permit enrichment summary ===")
    print(f"Councils loaded to Supabase (active ref_councils): {councils_loaded}")
    print(f"Permit groups processed: {len(groups)}")
    print(f"Destination permit upserts: {upserts}")
    print(f"Permit pages found (destinations with permit_page_url): {permit_page_dests}")
    print(f"Insurance required (destinations): {insurance_true}")
    if avg_fee is not None:
        print(f"Average permit fee where found (estimated): ${avg_fee:.0f}")
    else:
        print("Average permit fee where found (estimated): n/a")
    if avg_lead is not None:
        print(f"Average lead time days (where numeric): {avg_lead:.1f}")
    else:
        print("Average lead time days (where numeric): n/a")
    print(f"Destinations with online application URL: {online_app_dests}")
    online_groups = sum(1 for _grp, r in results if _norm_text(r.get("application_url")))
    print(f"Councils / permit-domain groups with online application URL: {online_groups}")
    print(f"ref_destinations council_permit_url updates: {url_updates}")
    print(f"Log: {LOG_PATH}")


if __name__ == "__main__":
    main()
