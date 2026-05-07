"""Hello May directory scrape (subset of categories × states)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from scrapers.directory_enrichment._framework import (  # noqa: E402
    RAILWAY_DIRECTORY_HEADERS,
    fetch_with_retry,
    is_blocked_non_vendor_url,
    looks_like_vendor_website,
    polite_delay,
)
from scrapers.directory_enrichment._vendor_router import (  # noqa: E402
    VendorRouter,
    match_or_insert,
    vendor_table_from_hm_category,
)

HELLO_MAY_BASE = "https://hellomay.com.au"

HELLO_MAY_SUB_A_TYPES = ("venues", "photographers", "cinematographers", "celebrant")

STATE_SLUG_TO_CODE: dict[str, str | None] = {
    "new-south-wales": "NSW",
    "victoria": "VIC",
    "queensland": "QLD",
    "western-australia": "WA",
    "south-australia": "SA",
    "tasmania": "TAS",
    "australian-capital-territory": "ACT",
    "northern-territory": "NT",
    "international": None,
}


def hm_listing_url(type_slug: str, state_slug: str) -> str:
    return f"{HELLO_MAY_BASE}/directory/{type_slug.strip('/')}/{state_slug.strip('/')}/"


def cli_vendor_tables(vendor_types: list[str]) -> set[str]:
    if not vendor_types or "all" in vendor_types:
        return {"venues", "photographers", "celebrants"}
    out: set[str] = set()
    for c in vendor_types:
        if c == "venues":
            out.add("venues")
        elif c == "photographers":
            out.add("photographers")
        elif c in ("celebrants", "celebrant"):
            out.add("celebrants")
    return out or {"venues", "photographers", "celebrants"}


def collect_profile_listing(listing_html: str) -> list[dict[str, Any]]:
    """Rows: url, category_slug, state_slug, label."""
    soup = BeautifulSoup(listing_html, "lxml")
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(HELLO_MAY_BASE, href)
        p = urlparse(full)
        if p.netloc != "hellomay.com.au":
            continue
        parts = [x for x in (p.path or "").split("/") if x]
        if len(parts) < 4 or parts[0] != "directory":
            continue
        category, state_slug, _slug = parts[1], parts[2], parts[3]
        if category.lower() in ("page", "tag", "category"):
            continue
        if full in seen:
            continue
        seen.add(full)
        text = (a.get_text() or "").strip()
        if text.lower() == "read more":
            text = ""
        rows.append(
            {
                "url": full,
                "category_slug": category.lower(),
                "state_slug": state_slug.lower(),
                "label": text,
            }
        )
    return rows


def parse_profile_social(html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "lxml")

    instagram: str | None = None
    facebook: str | None = None
    tiktok: str | None = None
    pinterest: str | None = None
    website: str | None = None

    for tag in soup.select("a[href]"):
        href = (tag.get("href") or "").strip()
        text = (tag.get_text() or "").strip()
        low = href.lower()
        if is_blocked_non_vendor_url(href):
            continue
        if "instagram.com" in low and "hellomay" not in low:
            if "l.instagram.com" in low:
                continue
            if instagram is None:
                instagram = href.split("?", 1)[0]
            continue
        if "facebook.com" in low or "fb.me" in low:
            if "hellomay" not in low and facebook is None:
                facebook = href.split("?", 1)[0]
            continue
        if "tiktok.com" in low and "hellomay" not in low:
            if tiktok is None:
                tiktok = href.split("?", 1)[0]
            continue
        if "pinterest." in low and "hellomay" not in low:
            if pinterest is None:
                pinterest = href.split("?", 1)[0]
            continue
        if website is None and looks_like_vendor_website(href, text):
            website = href.split("?", 1)[0]

    return {
        "instagram": instagram,
        "facebook": facebook,
        "tiktok": tiktok,
        "pinterest": pinterest,
        "website": website,
    }


def display_name_from_profile(html: str, fallback_slug: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.select_one("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t[:300]
    slug = fallback_slug.replace("-", " ").strip()
    return (slug.title() if slug else "Unknown Vendor")[:300]


def run_hello_may(
    *,
    client: httpx.Client,
    router: VendorRouter,
    log: logging.Logger,
    vendor_types: list[str],
    limit: int,
    dry_run: bool,
    deadline_mono: float | None,
    user_agent: str,
) -> dict[str, int]:
    counts: dict[str, int] = {"parsed": 0, "exact": 0, "fuzzy": 0, "inserted": 0}
    _ = dry_run
    wanted = cli_vendor_tables(vendor_types)

    import time as time_mod

    now = datetime.now(timezone.utc).isoformat()

    for type_slug in HELLO_MAY_SUB_A_TYPES:
        for state_slug in STATE_SLUG_TO_CODE:
            if counts["parsed"] >= limit:
                return counts
            if deadline_mono is not None and time_mod.monotonic() > deadline_mono:
                log.warning("Stopped Hello May early (runtime cap).")
                return counts

            listing_url = hm_listing_url(type_slug, state_slug)
            log.info("Hello May listing %s", listing_url)
            try:
                r = fetch_with_retry(client, listing_url, user_agent=user_agent)
            except Exception as exc:  # noqa: BLE001
                log.warning("Listing fetch failed %s: %s", listing_url, exc)
                polite_delay()
                continue

            listings = collect_profile_listing(r.text)
            if not listings:
                polite_delay()
                continue

            for item in listings:
                if counts["parsed"] >= limit:
                    break
                if deadline_mono is not None and time_mod.monotonic() > deadline_mono:
                    log.warning("Stopped Hello May profile loop (runtime cap).")
                    return counts

                profile_url = item["url"]
                category = item["category_slug"]
                st_slug = item["state_slug"]

                vt = vendor_table_from_hm_category(category)
                if vt is None:
                    continue
                if vt.table_name not in wanted:
                    continue

                state_code = STATE_SLUG_TO_CODE.get(st_slug)

                try:
                    pr = fetch_with_retry(client, profile_url, user_agent=user_agent)
                except Exception as exc:  # noqa: BLE001
                    log.warning("Profile fetch failed %s: %s", profile_url, exc)
                    polite_delay()
                    continue

                slug = urlparse(profile_url).path.rstrip("/").split("/")[-1]
                display = item["label"] or ""
                display = display.strip() or display_name_from_profile(pr.text, slug)

                social = parse_profile_social(pr.text)
                directory_patch = {
                    "hello_may_url": profile_url,
                    "hello_may_url_confirmed": True,
                    "hello_may_category": category,
                    "hello_may_listing_seen_at": now,
                    "last_directory_check_at": now,
                }

                matched, how = router.find_match_row(vt, name=display, state=state_code)
                counts["parsed"] += 1

                if matched is not None and how:
                    aug = router.merge_null_only(matched, directory_patch)
                    aug.update(
                        router.hello_may_social_patch(
                            vt,
                            existing=matched,
                            website=social["website"],
                            instagram=social["instagram"],
                            facebook=social["facebook"],
                            tiktok=social["tiktok"],
                            pinterest=social["pinterest"],
                        )
                    )
                    router.upsert_augment(vt, matched, aug)
                    counts[how] += 1
                else:
                    insert_patch = dict(directory_patch)
                    insert_patch.update(
                        router.hello_may_social_patch(
                            vt,
                            existing={},
                            website=social["website"],
                            instagram=social["instagram"],
                            facebook=social["facebook"],
                            tiktok=social["tiktok"],
                            pinterest=social["pinterest"],
                        )
                    )
                    outcome, _row = match_or_insert(
                        router,
                        vt,
                        "hello_may",
                        {
                            "name": display[:300],
                            "state": state_code,
                            "suburb": None,
                            "postcode": None,
                        },
                        directory_patch=insert_patch,
                    )
                    counts[outcome] = counts.get(outcome, 0) + 1

                polite_delay()

            polite_delay()

    return counts


def build_httpx_client() -> httpx.Client:
    return httpx.Client(
        headers=dict(RAILWAY_DIRECTORY_HEADERS),
        timeout=60.0,
        follow_redirects=True,
    )


def extract_user_agent() -> str:
    return str(RAILWAY_DIRECTORY_HEADERS.get("User-Agent") or "MilestoneDataBuilder/1.0")
