"""Paginated cohort queries for Gap-Fill Tier 1 (Supabase REST)."""

from __future__ import annotations

from typing import Any


def _celebr_directory_url_ok(row: dict[str, Any]) -> bool:
    """True when either curated directory column clearly holds an HTTP/S URL."""

    def _looks_url(val: Any) -> bool:
        s = str(val or "").strip()
        return len(s) > 12 and (s.startswith("http://") or s.startswith("https://"))

    return _looks_url(row.get("easy_weddings_url")) or _looks_url(
        row.get("hello_may_url"),
    )


def _celebr_gap_google_ok(row: dict[str, Any], verify: str) -> bool:
    raw = row.get("google_place_id")
    if raw is None:
        return True
    s = str(raw).strip()
    return not s or s == verify


_VENUES_SELECT_GAP = (
    "id,name,state,suburb,postcode,place_id,google_name,google_rating,google_review_count,"
    "google_maps_url,google_primary_type,google_phone,website_from_google,business_status,lat,"
    "lng,google_address,google_address_components_json,google_types_json,google_editorial_summary,"
    "opening_hours_json,enrichment_date,places_match_confidence,website_from_google_low_confidence,"
    "last_directory_check_at,"
    "review_text_1,review_author_1,review_rating_1,review_date_1,review_text_2,review_author_2,"
    "review_rating_2,review_date_2,review_text_3,review_author_3,review_rating_3,review_date_3,"
    "review_text_4,review_author_4,review_rating_4,review_date_4,review_text_5,review_author_5,"
    "review_rating_5,review_date_5,phone,website,enrichment_status"
)

_CELEB_SELECT_GAP = (
    "celebrant_id,name,full_name,state,suburb,easy_weddings_url,hello_may_url,is_active_market,"
    "google_place_id,google_name,google_rating,google_review_count,google_maps_url,"
    "google_primary_type,google_phone,google_address,google_types_json,google_editorial_summary,"
    "website_from_google,website_from_places,places_enriched_date,places_match_confidence,"
    "last_places_enrich_at,phone_from_places,business_status,lat,lng,google_address_components_json,"
    "website,phone,website_from_google_low_confidence,last_directory_check_at"
)

_PHOTO_SELECT_GAP = (
    "photographer_id,name,business_name,state,suburb,google_place_id,google_name,google_rating,"
    "google_review_count,google_maps_url,google_primary_type,google_phone,google_address,"
    "google_types_json,website_from_google,places_enriched_date,places_match_confidence,"
    "business_status,lat,lng,google_editorial_summary,google_address_components_json,website,"
    "tier,website_from_google_low_confidence,last_directory_check_at"
)


def fetch_venue_gap_cohort(sb: Any, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page = 1000
    start = 0
    remaining = limit
    while remaining > 0:
        take = min(page, remaining)
        end = start + take - 1
        r = (
            sb.table("venues")
            .select(_VENUES_SELECT_GAP)
            .is_("place_id", "null")
            .order("created_at", desc=True)
            .range(start, end)
            .execute()
        )
        batch = list(getattr(r, "data", None) or [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < take:
            break
        start += take
        remaining -= take
    return rows[:limit]


def fetch_celebrant_gap_cohort(
    sb: Any,
    limit: int,
    *,
    verify_required: str = "VERIFY_REQUIRED",
) -> list[dict[str, Any]]:
    """Active-market cohort with curated directory URLs sans reliable Google linkage.

    Rows may expose ``google_place_id`` as SQL NULL **or** the sentinel string — see DECISIONS.md.
    Pagination scans newest ``created_at`` rows until ``limit`` matches pass the URL predicate,
    mirroring WHERE (easy … OR hello …) semantics without falsely accepting VERIFY placeholders.
    """
    matched: list[dict[str, Any]] = []
    page = 500
    start = 0
    guard_loops = 0
    while len(matched) < limit:
        guard_loops += 1
        if guard_loops > 500:
            break
        end = start + page - 1
        r = (
            sb.table("celebrants")
            .select(_CELEB_SELECT_GAP)
            .eq("is_active_market", True)
            .order("created_at", desc=True)
            .range(start, end)
            .execute()
        )
        batch = list(getattr(r, "data", None) or [])
        if not batch:
            break
        for row in batch:
            if not _celebr_gap_google_ok(row, verify_required):
                continue
            if not _celebr_directory_url_ok(row):
                continue
            matched.append(row)
            if len(matched) >= limit:
                break
        if len(batch) < page:
            break
        start += page
    return matched[:limit]


def fetch_photographer_gap_cohort(sb: Any, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page = 1000
    start = 0
    remaining = limit
    while remaining > 0:
        take = min(page, remaining)
        end = start + take - 1
        r = (
            sb.table("photographers")
            .select(_PHOTO_SELECT_GAP)
            .is_("google_place_id", "null")
            .order("created_at", desc=True)
            .range(start, end)
            .execute()
        )
        batch = list(getattr(r, "data", None) or [])
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < take:
            break
        start += take
        remaining -= take
    return rows[:limit]
