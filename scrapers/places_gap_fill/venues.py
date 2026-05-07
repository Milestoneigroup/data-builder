"""Venues Tier 1 gap-fill (Places Text Search → Details with venue-only richness)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from ._framework import (
    HIGH_CONFIDENCE_THRESHOLD,
    LOW_CONFIDENCE_THRESHOLD,
    augmented_subset,
    confidence_band_from_pct,
    is_blank_for_augment,
)
from ._places_client import (
    extract_universal_fields,
    extract_venue_specific_fields,
    find_place_with_fallbacks,
    place_id_from_resource,
)
from ._query_builder import fetch_venue_gap_cohort

_VENUES_AUGMENT_KEYS = {
    "place_id",
    "google_name",
    "google_rating",
    "google_review_count",
    "google_maps_url",
    "google_primary_type",
    "google_phone",
    "website_from_google",
    "website_from_google_low_confidence",
    "lat",
    "lng",
    "business_status",
    "google_address",
    "google_address_components_json",
    "google_types_json",
    "google_editorial_summary",
    "opening_hours_json",
    "review_text_1",
    "review_author_1",
    "review_rating_1",
    "review_date_1",
    "review_text_2",
    "review_author_2",
    "review_rating_2",
    "review_date_2",
    "review_text_3",
    "review_author_3",
    "review_rating_3",
    "review_date_3",
    "review_text_4",
    "review_author_4",
    "review_rating_4",
    "review_date_4",
    "review_text_5",
    "review_author_5",
    "review_rating_5",
    "review_date_5",
    "last_directory_check_at",
}

_EXTRA_ALWAYS_KEYS = frozenset(
    {
        "places_match_confidence",
        "enrichment_date",
        "enrichment_status",
        "website_from_google_low_confidence",
    },
)

def _place_id_claimed_elsewhere(sb: Any, place_id: str, venue_id: str) -> bool:
    norm = place_id_from_resource(place_id)
    if not norm:
        return True
    r = (
        sb.table("venues")
        .select("id")
        .eq("place_id", norm)
        .limit(25)
        .execute()
    )
    owned = str(venue_id).strip()
    for rec in getattr(r, "data", None) or []:
        rid = str(rec.get("id") or "").strip()
        if rid and rid != owned:
            return True
    return False


def _search_label(row: dict[str, Any]) -> str:
    return str(row.get("name") or "").strip()


def _compose_proposed_high(existing: dict[str, Any], det: dict[str, Any], conf: str) -> dict[str, Any]:
    uni = extract_universal_fields(det)
    extras = extract_venue_specific_fields(det)
    canon = uni.pop("canonical_place_id") or ""

    merged: dict[str, Any] = {
        **uni,
        **extras,
        "place_id": canon,
        "places_match_confidence": conf,
        "enrichment_date": datetime.now(timezone.utc).isoformat(),
        "enrichment_status": "enriched",
        "website_from_google_low_confidence": False,
    }
    wf_g = merged.get("website_from_google")
    if wf_g and not is_blank_for_augment(existing.get("website_from_google")):
        merged.pop("website_from_google", None)
    return merged


def run_venues_gap_fill(
    sb: Any,
    http: httpx.Client,
    tracker: Any,
    *,
    limit: int,
    dry_run: bool,
    log: Any,
) -> dict[str, Any]:
    rows = fetch_venue_gap_cohort(sb, limit)
    log.info("venues cohort fetched count=%s cap=%s", len(rows), limit)
    enriched_high = enriched_low = skipped = stopped_budget = 0
    total_query_variations = 0
    vendors_touched = 0

    for row in rows:
        vid = str(row.get("id") or "").strip()
        label = _search_label(row)
        suburb = str(row.get("suburb") or "").strip()
        state = str(row.get("state") or "").strip()

        if not vid or not label:
            skipped += 1
            continue

        vendors_touched += 1
        fp = find_place_with_fallbacks(
            name=label,
            state=state or None,
            suburb=suburb or None,
            vendor_type="venues",
            http=http,
            tracker=tracker,
            logger=log,
            place_id_claimed=lambda pid: _place_id_claimed_elsewhere(sb, pid, vid),
        )
        total_query_variations += fp.queries_tried

        if fp.budget_exhausted:
            stopped_budget += 1
            break

        if fp.details is None:
            log.info("no match across 5 variations, skipping %s", vid)
            skipped += 1
            continue

        confidence = fp.confidence
        query_used = fp.query_used or ""

        if confidence >= HIGH_CONFIDENCE_THRESHOLD:
            pct_i = max(0, min(100, int(round(confidence * 100))))
            bucket_final = confidence_band_from_pct(pct_i)
            proposed = _compose_proposed_high(row, fp.details, bucket_final)

            augment_body = augmented_subset(row, proposed, augment_keys=_VENUES_AUGMENT_KEYS)
            patch_final = dict(augment_body)
            for k in _EXTRA_ALWAYS_KEYS:
                patch_final[k] = proposed[k]

            if dry_run:
                log.info(
                    "dry-run venues id=%s would_patch_keys=%s match=%.2f",
                    vid,
                    sorted(patch_final.keys()),
                    confidence,
                )
                enriched_high += 1
                continue

            sb.table("venues").update(patch_final).eq("id", vid).execute()
            enriched_high += 1
            log.info(
                "enriched (high) %s match=%.2f query='%s'",
                vid,
                confidence,
                query_used,
            )

        elif confidence >= LOW_CONFIDENCE_THRESHOLD:
            wuri = str(fp.details.get("websiteUri") or "").strip() or None
            now_iso = datetime.now(timezone.utc).isoformat()
            pct_i = max(0, min(100, int(round(confidence * 100))))
            conf_bucket = confidence_band_from_pct(pct_i)
            proposed = {
                "website_from_google": wuri,
                "website_from_google_low_confidence": True,
                "places_match_confidence": conf_bucket,
                "last_directory_check_at": now_iso,
            }
            augment_body = augmented_subset(row, proposed, augment_keys=_VENUES_AUGMENT_KEYS)
            patch_final = {k: v for k, v in augment_body.items() if v is not None}
            patch_final["website_from_google_low_confidence"] = True
            patch_final["places_match_confidence"] = conf_bucket
            patch_final["last_directory_check_at"] = now_iso

            if dry_run:
                log.info(
                    "dry-run low-conf website venues id=%s keys=%s match=%.2f",
                    vid,
                    sorted(patch_final.keys()),
                    confidence,
                )
                enriched_low += 1
                continue

            if not patch_final:
                log.info("low-conf venues id=%s no augmentable null fields, skip", vid)
                skipped += 1
                continue

            sb.table("venues").update(patch_final).eq("id", vid).execute()
            enriched_low += 1
            log.info(
                "low-conf website captured %s match=%.2f query='%s'",
                vid,
                confidence,
                query_used,
            )

        else:
            log.info("all variations below 0.50, skip %s", vid)
            skipped += 1

    return {
        "processed_type": "venues",
        "cohort_requested_cap": limit,
        "cohort_loaded": len(rows),
        "enriched": enriched_high + enriched_low,
        "enriched_high": enriched_high,
        "enriched_low_website": enriched_low,
        "skipped": skipped,
        "low_confidence": 0,
        "stopped_budget": stopped_budget,
        "total_query_variations": total_query_variations,
        "vendors_touched": vendors_touched,
    }
