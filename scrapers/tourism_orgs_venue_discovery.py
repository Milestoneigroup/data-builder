"""Wedding Weekend Intelligence Layer — tourism org enrichment (A/B/C).

Section A: Profile enrichment (Claude + homepage HTML) for shared.ref_tourism_organisations.
Section B: Weekend content -> shared.ref_tourism_weekend_content (after migration 004). Not in this run.
Section C: Listings -> shared.ref_tourism_listings (after migration 005). Not in this run.

This module currently implements Section A. Apply supabase/migrations/004_*.sql and 005_*.sql
on your project before B/C.

Examples:
  python scrapers/tourism_orgs_venue_discovery.py --section a --limit 3
  python scrapers/tourism_orgs_venue_discovery.py --section a --full

Requires: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, ANTHROPIC_API_KEY
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any
import httpx
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

LOG_PATH = _ROOT / "logs" / "tourism_wedding_weekend.log"
MAX_HTML = 150_000
HTTP_TIMEOUT = 75.0
API_DELAY_S = 0.5
SECTION_B_PAGE_CHAR_CAP = 12_000
SECTION_B_TOTAL_CHAR_CAP = 70_000
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
SECTION_B_LINK_HINTS = (
    "things-to-do",
    "attractions",
    "activities",
    "experiences",
    "rainy-day",
    "indoor",
    "accommodation",
    "where-to-stay",
    "getting-here",
    "transport",
)

SENTINELS = frozenset(
    {
        "",
        "not_available",
        "not applicable",
        "n/a",
        "na",
        "null",
        "none",
        "verify_required",
        "unknown",
    }
)
NOT_FOUND = "NOT_FOUND"


def _setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger("tourism_wedding_weekend")
    root.setLevel(logging.INFO)
    root.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _norm_str(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _field_needs_fill(val: Any) -> bool:
    t = _norm_str(val)
    if not t:
        return True
    return t.upper() in (x.upper() for x in SENTINELS) or t.upper() == "NOT_FOUND"


def _coord_needs_fill(lat: Any, lng: Any) -> bool:
    try:
        la = float(lat) if lat is not None and str(lat).strip() != "" else 0.0
        lo = float(lng) if lng is not None and str(lng).strip() != "" else 0.0
    except (TypeError, ValueError):
        return True
    if abs(la) < 0.0001 and abs(lo) < 0.0001:
        return True
    return False


def _truncate(s: str) -> str:
    if len(s) <= MAX_HTML:
        return s
    h = (MAX_HTML * 2) // 3
    t = MAX_HTML - h - 50
    return s[:h] + "\n<!-- TRUNC -->\n" + s[-t:]


def _parse_json_claude(text: str) -> dict[str, Any]:
    t = _norm_str(text)
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", t)
    if m:
        t = m.group(1).strip()
    return json.loads(t)


def _norm_url(val: Any) -> str:
    s = _norm_str(val)
    if not s or s.lower() in ("null", "none", "n/a", "not found"):
        return NOT_FOUND
    if s.startswith("www."):
        s = "https://" + s
    if s.startswith(("http://", "https://")):
        return s
    return NOT_FOUND


def _norm_social_insta(val: Any) -> str:
    s = _norm_str(val)
    if not s or s.lower() in ("null", "none", "n/a", "not found"):
        return NOT_FOUND
    s = s.lstrip("@")
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if re.match(r"^[\w.]+$", s):
        return f"https://www.instagram.com/{s}/"
    return NOT_FOUND


def _norm_social_facebook(val: Any) -> str:
    s = _norm_str(val)
    if not s or s.lower() in ("null", "none", "n/a", "not found"):
        return NOT_FOUND
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("facebook.com") or s.startswith("www.facebook.com"):
        return "https://" + s.lstrip("htps:/")
    if re.match(r"^[\w.\-]+\/?$", s) and " " not in s:
        return f"https://www.facebook.com/{s.strip('/')}/"
    return NOT_FOUND


def _norm_social_pinterest(val: Any) -> str:
    s = _norm_str(val)
    if not s or s.lower() in ("null", "none", "n/a", "not found"):
        return NOT_FOUND
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("pinterest.com") or s.startswith("www.pinterest.com"):
        return "https://" + s.lstrip("htps:/")
    if re.match(r"^[\w.\-\/]+\/?$", s):
        if "/" in s or s.isalnum() or "." in s:
            return f"https://www.pinterest.com/{s.strip('/')}/"
    return NOT_FOUND


def _norm_email(val: Any) -> str:
    s = _norm_str(val)
    if not s or s.lower() in ("null", "none", "n/a", "not found"):
        return NOT_FOUND
    if "@" in s and "." in s.split("@")[-1]:
        return s
    return NOT_FOUND


def _norm_lat_lng(
    data: dict[str, Any],
) -> tuple[Any, Any] | None:
    lat, lng = data.get("lat"), data.get("lng")
    if lat is None and lng is None:
        return None
    try:
        la = float(lat) if lat is not None and str(lat) != "" else None
        lo = float(lng) if lng is not None and str(lng) != "" else None
    except (TypeError, ValueError):
        return None
    if la is None or lo is None:
        return None
    # Rough Australia bounds
    if not (-45.0 <= la <= -9.0) or not (110.0 <= lo <= 155.0):
        return None
    return (round(la, 6), round(lo, 6))


def _fetch_homepage(client: httpx.Client, url: str, log: logging.Logger) -> str:
    r = client.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-AU,en;q=0.9",
        },
        follow_redirects=True,
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    return r.text or ""


def _claude_profile_extract(
    *,
    html: str,
    org_name: str,
    region_name: str,
    homepage_url: str,
    api_key: str,
    model: str,
    log: logging.Logger,
) -> dict[str, Any] | None:
    from anthropic import Anthropic

    instructions = f"""You are extracting structured data from a regional tourism body website in Australia (wedding guest planning context).
Organisation: {org_name}
Region label: {region_name}
Homepage: {homepage_url}

From the HTML that follows, extract and return **JSON only** (no markdown) with these exact keys:
- "instagram": full Instagram profile URL, or a handle like @name, or null if not found
- "facebook": full Facebook page URL, or null
- "pinterest": full Pinterest profile/board URL, or null
- "email": a public contact or visitor-centre style email, or null
- "weddings_url": the best page URL about weddings, wedding venues, or honeymoons in this region, or null
- "events_url": a page about events, things to do, or what is on, or null
- "lat": decimal latitude for the main visitor centre, office, or org HQ if discoverable, else null
- "lng": decimal longitude, else null

If a value cannot be found, use null. Do not guess URLs; only use what is clearly linked or in visible content."""

    body = _truncate(html)
    user_msg = f"{instructions}\n\n--- HTML ---\n{body}"
    try:
        ac = Anthropic(api_key=api_key)
        msg = ac.messages.create(
            model=model,
            max_tokens=2000,
            messages=[{"role": "user", "content": user_msg}],
        )
        parts: list[str] = []
        for b in msg.content or []:
            if hasattr(b, "text"):
                parts.append(b.text)
            elif isinstance(b, dict) and b.get("type") == "text":
                parts.append(str(b.get("text", "")))
        return _parse_json_claude("".join(parts).strip())
    except Exception as e:  # noqa: BLE001
        log.error("Claude profile extract failed: %s", e)
        return None


def _fetch_html(client: httpx.Client, url: str, log: logging.Logger) -> str | None:
    try:
        r = client.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-AU,en;q=0.9",
            },
            follow_redirects=True,
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        return r.text or ""
    except Exception as e:  # noqa: BLE001
        log.warning("HTTP fetch failed url=%s err=%s", url, e)
        return None


def _fetch_html_playwright(url: str, log: logging.Logger) -> str | None:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        log.debug("Playwright not available: %s", e)
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=USER_AGENT, locale="en-AU")
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=75_000)
            page.wait_for_timeout(2500)
            html = page.content()
            context.close()
            browser.close()
            return html
    except Exception as e:  # noqa: BLE001
        log.warning("Playwright fetch failed url=%s err=%s", url, e)
        return None


def _needs_js_render(html: str | None) -> bool:
    if not html:
        return True
    if len(html) < 2500:
        return True
    anchors = len(re.findall(r"<a\\b", html, flags=re.I))
    if anchors < 5:
        return True
    low = html.lower()
    if "__next" in low or "_nuxt" in low or "reactroot" in low:
        return anchors < 20
    return False


def _keyword_links(html: str, base_url: str) -> list[str]:
    hrefs = re.findall(r"""href=["']([^"'#]+)["']""", html, flags=re.I)
    out: list[str] = []
    for h in hrefs:
        candidate = h.strip()
        if not candidate or candidate.startswith(("mailto:", "tel:", "javascript:")):
            continue
        if candidate.startswith("//"):
            candidate = "https:" + candidate
        if candidate.startswith("/"):
            parts = base_url.rstrip("/")
            candidate = parts + candidate
        low = candidate.lower()
        if any(k in low for k in SECTION_B_LINK_HINTS):
            out.append(candidate)
    uniq: list[str] = []
    seen: set[str] = set()
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq[:8]


def _combine_docs(docs: list[tuple[str, str]]) -> str:
    chunks: list[str] = []
    for url, html in docs:
        slim = re.sub(r"\s+", " ", html).strip()
        if len(slim) > SECTION_B_PAGE_CHAR_CAP:
            slim = slim[:SECTION_B_PAGE_CHAR_CAP]
        chunks.append(f"URL: {url}\nCONTENT:\n{slim}")
    joined = "\n\n-----\n\n".join(chunks)
    if len(joined) > SECTION_B_TOTAL_CHAR_CAP:
        joined = joined[:SECTION_B_TOTAL_CHAR_CAP]
    return joined


def _claude_weekend_extract(
    *,
    org_name: str,
    region_name: str,
    state_code: str,
    combined_content: str,
    api_key: str,
    model: str,
    log: logging.Logger,
) -> str | None:
    from anthropic import Anthropic

    prompt = f"""You are building a wedding weekend guide for guests attending a wedding in {region_name}, {state_code}, Australia.
Organisation: {org_name}

From this tourism website content, extract and return JSON only with this exact schema:
{{
  "org_id": null,
  "state_code": "{state_code}",
  "region_name": "{region_name}",
  "things_to_do_rainy_day": [],
  "things_to_do_outdoor": [],
  "things_to_do_couples": [],
  "things_to_do_groups": [],
  "accommodation_types": [],
  "accommodation_price_range": "",
  "accommodation_booking_url": "",
  "nearest_airport": "",
  "airport_distance_note": "",
  "transport_options": [],
  "notable_restaurants": [],
  "wineries_breweries": [],
  "local_produce": "",
  "top_attractions": [],
  "things_to_do_url": "",
  "accommodation_url": "",
  "transport_url": "",
  "scraped_date": "{date.today().isoformat()}",
  "data_confidence": "",
  "source_urls": []
}}

Rules:
- Fill arrays with specific items where possible (prefer <= 5 items each for activity arrays).
- Use empty string/empty array when unknown.
- Keep response as valid JSON only, no markdown.
- Keep list items concise.

Website content:
{combined_content}
"""
    try:
        ac = Anthropic(api_key=api_key)
        msg = ac.messages.create(
            model=model,
            max_tokens=3200,
            messages=[{"role": "user", "content": prompt}],
        )
        parts: list[str] = []
        for b in msg.content or []:
            if hasattr(b, "text"):
                parts.append(b.text)
            elif isinstance(b, dict) and b.get("type") == "text":
                parts.append(str(b.get("text", "")))
        return "".join(parts).strip()
    except Exception as e:  # noqa: BLE001
        log.error("Claude weekend extract failed org=%s err=%s", org_name, e)
        return None


def run_section_a(
    limit: int | None,
    log: logging.Logger,
) -> None:
    from data_builder.config import get_settings
    from supabase import create_client

    settings = get_settings()
    url = (settings.supabase_url or "").strip()
    key = (settings.supabase_service_role_key or "").strip()
    ak = (settings.anthropic_api_key or "").strip()
    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5-20250929").strip()

    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")
    if not ak:
        raise SystemExit("ANTHROPIC_API_KEY is required.")

    sb = create_client(url, key)
    tbl = sb.schema("shared").table("ref_tourism_organisations")

    res = (
        tbl.select(
            "org_id,org_name,region_name,state_code,website_homepage,website_weddings_page,"
            "website_events_page,visitor_centre_email,social_instagram,social_facebook,social_pinterest,lat,lng"
        )
        .order("display_order")
        .execute()
    )
    rows: list[dict[str, Any]] = list(res.data or [])
    if not rows:
        log.warning("No tourism organisations found.")
        return
    n = min(len(rows), limit) if limit else len(rows)
    batch = rows[:n]
    log.info("Section A: processing %s org(s) (of %s total).", n, len(rows))

    today = date.today().isoformat()
    updated = 0
    failed = 0

    with httpx.Client() as client:
        for i, row in enumerate(batch, start=1):
            oid = row.get("org_id")
            name = _norm_str(row.get("org_name"))
            region = _norm_str(row.get("region_name"))
            home = _norm_str(row.get("website_homepage"))
            log.info("(%s/%s) %s | %s", i, n, oid, name)

            if not home.lower().startswith("http"):
                log.warning("  skip: invalid website_homepage")
                failed += 1
                continue

            try:
                html = _fetch_homepage(client, home, log)
            except Exception as e:  # noqa: BLE001
                log.error("  HTTP failed: %s", e)
                failed += 1
                time.sleep(API_DELAY_S)
                continue
            time.sleep(API_DELAY_S)

            data = _claude_profile_extract(
                html=html,
                org_name=name,
                region_name=region,
                homepage_url=home,
                api_key=ak,
                model=model,
                log=log,
            )
            time.sleep(API_DELAY_S)
            if not data:
                failed += 1
                continue

            patch: dict[str, Any] = {"updated_at": today}

            if _field_needs_fill(row.get("visitor_centre_email")):
                patch["visitor_centre_email"] = _norm_email(data.get("email"))

            if _field_needs_fill(row.get("social_instagram")):
                patch["social_instagram"] = _norm_social_insta(data.get("instagram"))

            if _field_needs_fill(row.get("social_facebook")):
                patch["social_facebook"] = _norm_social_facebook(data.get("facebook"))

            if _field_needs_fill(row.get("social_pinterest")):
                patch["social_pinterest"] = _norm_social_pinterest(data.get("pinterest"))

            if _field_needs_fill(row.get("website_weddings_page")):
                patch["website_weddings_page"] = _norm_url(data.get("weddings_url"))

            if _field_needs_fill(row.get("website_events_page")):
                patch["website_events_page"] = _norm_url(data.get("events_url"))

            if _coord_needs_fill(row.get("lat"), row.get("lng")):
                coords = _norm_lat_lng(data)
                if coords:
                    patch["lat"] = coords[0]
                    patch["lng"] = coords[1]

            keys = [k for k in patch if k not in ("updated_at",)]
            if not keys:
                log.info("  no gap fields to update for this org.")
            try:
                if not keys:
                    continue
                tbl.update(patch).eq("org_id", oid).execute()
                log.info(
                    "  updated: keys=%s",
                    [k for k in patch if k != "updated_at"],
                )
                updated += 1
            except Exception as e:  # noqa: BLE001
                log.error("  Supabase update failed: %s", e)
                failed += 1

    log.info("Section A done. rows_ok=%s rows_failed=%s", updated, failed)
    print(f"Section A: updated {updated} org row(s), failed {failed}. Log: {LOG_PATH}")


def run_section_b(*, limit: int | None, full: bool) -> None:
    from data_builder.config import get_settings
    from supabase import create_client

    log = logging.getLogger("tourism_wedding_weekend")
    settings = get_settings()
    url = (settings.supabase_url or "").strip()
    key = (settings.supabase_service_role_key or "").strip()
    ak = (settings.anthropic_api_key or "").strip()
    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5-20250929").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")
    if not ak:
        raise SystemExit("ANTHROPIC_API_KEY is required.")

    sb = create_client(url, key)
    tbl = sb.schema("shared").table("ref_tourism_organisations")
    rows = (
        tbl.select(
            "org_id,org_name,state_code,region_name,website_homepage,website_events_page"
        )
        .eq("scraper_priority", "p1_urgent")
        .order("display_order")
        .execute()
        .data
        or []
    )
    if full:
        chosen = list(rows)
        log.info("Section B FULL MODE: processing %s p1_urgent orgs.", len(chosen))
    else:
        lim = limit if limit is not None else 3
        chosen = list(rows)[: max(1, lim)]
        log.info("Section B TEST MODE: processing first %s p1_urgent orgs.", len(chosen))

    with httpx.Client() as client:
        for idx, row in enumerate(chosen, start=1):
            org_id = _norm_str(row.get("org_id"))
            org_name = _norm_str(row.get("org_name"))
            state = _norm_str(row.get("state_code"))
            region = _norm_str(row.get("region_name"))
            homepage = _norm_str(row.get("website_homepage"))
            events = _norm_str(row.get("website_events_page"))
            log.info("(%s/%s) %s | %s", idx, len(chosen), org_id, org_name)

            docs: list[tuple[str, str]] = []
            seed_urls = [u for u in (homepage, events) if u.lower().startswith("http")]
            seen_url: set[str] = set()
            for u in seed_urls:
                if u in seen_url:
                    continue
                seen_url.add(u)
                html = _fetch_html(client, u, log)
                time.sleep(API_DELAY_S)
                if _needs_js_render(html):
                    html = _fetch_html_playwright(u, log) or html
                    time.sleep(API_DELAY_S)
                if html:
                    docs.append((u, html))

            # Follow targeted link hints from fetched seed pages.
            follow_urls: list[str] = []
            for seed_url, html in docs:
                for u in _keyword_links(html, seed_url):
                    if u not in seen_url:
                        seen_url.add(u)
                        follow_urls.append(u)
            for u in follow_urls[:6]:
                html = _fetch_html(client, u, log)
                time.sleep(API_DELAY_S)
                if _needs_js_render(html):
                    html = _fetch_html_playwright(u, log) or html
                    time.sleep(API_DELAY_S)
                if html:
                    docs.append((u, html))

            combined = _combine_docs(docs) if docs else ""
            if not combined:
                log.warning("No extractable content for %s", org_id)
                print(f"\n=== SECTION B RAW JSON | {org_id} | {org_name} ===")
                print('{"error":"no_content_fetched"}')
                continue

            raw = _claude_weekend_extract(
                org_name=org_name,
                region_name=region,
                state_code=state,
                combined_content=combined,
                api_key=ak,
                model=model,
                log=log,
            )
            time.sleep(API_DELAY_S)
            print(f"\n=== SECTION B RAW JSON | {org_id} | {org_name} ===")
            print(raw or '{"error":"claude_failed"}')


def run_section_c() -> None:
    raise SystemExit(
        "Section C (listings) is not run in this build. "
        "Apply supabase/migrations/005_tourism_listings.sql, confirm Section B, then we add C."
    )


def main() -> None:
    _load_env()
    _setup_logging()
    log = logging.getLogger("tourism_wedding_weekend")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    p = argparse.ArgumentParser(description="Wedding weekend intelligence (tourism orgs).")
    p.add_argument(
        "--section",
        choices=("a", "b", "c"),
        default="a",
        help="a=org profile enrich, b=weekend content (test mode), c=listings.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max orgs to process (default: 3 for a safe test; use with --section a).",
    )
    p.add_argument(
        "--full",
        action="store_true",
        help="Process all orgs in table (ignores default test limit for section a).",
    )
    args = p.parse_args()

    if args.section == "a":
        lim = 10_000 if args.full else (args.limit if args.limit is not None else 3)
        run_section_a(limit=lim, log=log)
    elif args.section == "b":
        run_section_b(limit=args.limit, full=args.full)
    else:
        run_section_c()


if __name__ == "__main__":
    main()
