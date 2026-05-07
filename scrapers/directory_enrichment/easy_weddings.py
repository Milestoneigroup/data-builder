"""Easy Weddings listing scrape (venues, celebrants, photographers)."""

from __future__ import annotations

import html as html_module
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from bs4 import BeautifulSoup

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from scrapers.directory_enrichment._framework import (  # noqa: E402
    RAILWAY_DIRECTORY_HEADERS,
    fetch_with_retry,
    polite_delay,
)
from scrapers.directory_enrichment._vendor_router import (  # noqa: E402
    EW_VENDOR_TYPE_TO_PATH,
    VendorRouter,
    VendorTable,
    match_or_insert,
    vendor_table_from_ew_segment,
)

EW_BASE = "https://www.easyweddings.com.au"


def ew_listing_url(segment: str, page: int) -> str:
    if page <= 1:
        return f"{EW_BASE}/{segment}/"
    return f"{EW_BASE}/{segment}/{page}/"


def _premium_award_text(raw: str | None, card_text: str) -> str | None:
    lines: list[str] = []
    if raw:
        raw_un = html_module.unescape(raw.strip())
        try:
            parsed = json.loads(raw_un)
            if isinstance(parsed, list):
                lines = [str(x).strip() for x in parsed if str(x).strip()]
        except json.JSONDecodeError:
            pass
    blob = " ".join(lines) + " " + card_text
    if not blob.strip():
        return None
    if re.search(r"award|winner|finalist", blob, re.I):
        return "; ".join(lines) if lines else None
    return None


def parse_ew_page(
    *,
    html: str,
    segment: str,
    page_num: int,
) -> list[dict[str, Any]]:
    """Parse one listing page; mirrors ``celebrant_active_enrichment.scrape_easy_weddings`` card logic."""
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, Any]] = []
    for card in soup.select(".supplier-card"):
        anchor = card.select_one("span.supplierlisting, span.anchor.supplierlisting")
        if not anchor:
            continue
        name = html_module.unescape(anchor.get("data-supplier-name", "") or "").strip()
        slug = anchor.get("data-supplier-slug", "") or ""
        loc_slug = anchor.get("data-location-slug", "") or ""
        state = html_module.unescape(anchor.get("data-supplier-state", "") or "").strip()
        addr = html_module.unescape(anchor.get("data-ew-address", "") or "").strip()
        suburb = addr.split(",")[0].strip() if addr else ""
        raw_json = anchor.get("data-json", "") or "{}"
        try:
            j = json.loads(html_module.unescape(raw_json))
        except json.JSONDecodeError:
            j = {}
        rating = j.get("reviewScore")
        rev_c = j.get("reviewCount")
        prem = anchor.get("data-premium-feature-text", "") or ""
        award = _premium_award_text(prem, card.get_text(" ", strip=True))
        profile = f"{EW_BASE}/{segment}/{loc_slug}/{slug}/" if loc_slug and slug else ""
        rows.append(
            {
                "name": name,
                "suburb": suburb or None,
                "state": state or None,
                "profile_url": profile,
                "rating": rating,
                "review_count": rev_c,
                "award": award,
                "ew_page": page_num,
            }
        )
    return rows


def run_easy_weddings(
    *,
    client: httpx.Client,
    router: VendorRouter,
    log: logging.Logger,
    vendor_types: list[str],
    limit: int,
    start_page: int,
    dry_run: bool,
    deadline_mono: float | None,
    user_agent: str,
) -> dict[str, int]:
    _ = dry_run
    counts: dict[str, int] = {"parsed": 0, "exact": 0, "fuzzy": 0, "inserted": 0}

    vt_filter: dict[str, VendorTable] = {}
    for k, seg in EW_VENDOR_TYPE_TO_PATH.items():
        vt = vendor_table_from_ew_segment(seg)
        if vt:
            vt_filter[k] = vt

    active_types = vendor_types
    if "all" in active_types:
        active_types = list(vt_filter.keys())

    chosen: list[tuple[str, str, VendorTable]] = []
    for kind in active_types:
        seg = EW_VENDOR_TYPE_TO_PATH.get(kind)
        vtt = vt_filter.get(kind)
        if not seg or not vtt:
            log.warning("Unknown Easy Weddings vendor type %r — skipping", kind)
            continue
        chosen.append((kind, seg, vtt))

    now = datetime.now(timezone.utc).isoformat()

    for _kind, segment, vt in chosen:
        page = max(1, start_page)
        while counts["parsed"] < limit:
            if deadline_mono is not None and time.monotonic() > deadline_mono:
                log.warning("Stopped Easy Weddings early (runtime cap).")
                break
            url = ew_listing_url(segment, page)
            log.info("Easy Weddings fetch %s", url)
            r = fetch_with_retry(client, url, user_agent=user_agent)
            batch = parse_ew_page(html=r.text, segment=segment, page_num=page)
            if not batch:
                log.info("No supplier cards on %s — end of pagination", url)
                break

            for rec in batch:
                if counts["parsed"] >= limit:
                    break
                if not str(rec.get("name") or "").strip():
                    continue
                counts["parsed"] += 1
                profile = rec.get("profile_url") or ""
                directory_patch = {
                    "easy_weddings_url": profile or None,
                    "easy_weddings_url_confirmed": True,
                    "easy_weddings_rating": rec.get("rating"),
                    "easy_weddings_review_count": rec.get("review_count"),
                    "easy_weddings_award": rec.get("award"),
                    "easy_weddings_listing_seen_at": now,
                    "last_directory_check_at": now,
                }
                vendor_payload = {
                    "name": rec["name"],
                    "state": rec.get("state"),
                    "suburb": rec.get("suburb"),
                    "postcode": None,
                }
                outcome, _row = match_or_insert(
                    router,
                    vt,
                    "easy_weddings",
                    vendor_payload,
                    directory_patch=directory_patch,
                )
                counts[outcome] = counts.get(outcome, 0) + 1

            polite_delay()
            page += 1

    return counts


def build_httpx_client() -> httpx.Client:
    headers = dict(RAILWAY_DIRECTORY_HEADERS)
    return httpx.Client(headers=headers, timeout=60.0, follow_redirects=True)


def extract_user_agent() -> str:
    return str(RAILWAY_DIRECTORY_HEADERS.get("User-Agent") or "MilestoneDataBuilder/1.0")
