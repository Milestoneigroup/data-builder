"""Photographers Tier 1 gap-fill — universal Places fields only."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import httpx

from ._framework import (
    HIGH_CONFIDENCE_THRESHOLD,
    LOW_CONFIDENCE_THRESHOLD,
    augmented_subset,
    is_blank_for_augment,
)
from ._places_client import (
    extract_universal_fields,
    find_place_with_fallbacks,
    place_id_from_resource,
)
from ._query_builder import fetch_photographer_gap_cohort

_PHOTO_AUG_KEYS = {
    "google_place_id",
    "google_name",
    "google_rating",
    "google_review_count",
    "google_maps_url",
    "google_primary_type",
    "google_phone",
    "google_address",
    "google_types_json",
    "website_from_google",
    "website_from_google_low_confidence",
    "places_enriched_date",
    "places_match_confidence",
    "business_status",
    "lat",
    "lng",
    "google_address_components_json",
    "google_editorial_summary",
    "last_directory_check_at",
}

_PATCH_ALWAYS = frozenset(
    {"places_enriched_date", "places_match_confidence", "website_from_google_low_confidence"}
)


def _display_label(row: dict[str, Any]) -> str:
    n = str(row.get("name") or "").strip()
    if n:
        return n
    return str(row.get("business_name") or "").strip()


def _place_id_claimed_elsewhere(sb: Any, place_id: str, photographer_id: str) -> bool:
    norm = place_id_from_resource(place_id)
    if not norm:
        return True
    r = (
        sb.table("photographers")
        .select("photographer_id")
        .eq("google_place_id", norm)
        .limit(40)
        .execute()
    )
    mine = str(photographer_id).strip()
    for rec in getattr(r, "data", None) or []:
        oid = str(rec.get("photographer_id") or "").strip()
        if oid and oid != mine:
            return True
    return False


def _match_conf_text(confidence: float) -> str:
    """Store a numeric string in text ``places_match_confidence`` for downstream casts."""

    return f"{round(confidence, 4):.4f}"


def _compose_proposed_high(
    existing: dict[str, Any],
    det: dict[str, Any],
    confidence: float,
) -> dict[str, Any]:
    uni = extract_universal_fields(det)
    canon = uni.pop("canonical_place_id") or ""

    wf_g = uni.get("website_from_google")
    if wf_g and not is_blank_for_augment(existing.get("website_from_google")):
        uni.pop("website_from_google", None)
    elif not wf_g:
        uni.pop("website_from_google", None)

    rating = uni.get("google_rating")
    try:
        rating_f = float(rating) if rating is not None else None
    except (TypeError, ValueError):
        rating_f = None

    count_raw = uni.get("google_review_count")
    try:
        count_i = int(count_raw) if count_raw is not None else None
    except (TypeError, ValueError):
        count_i = None

    return {
        **uni,
        "google_place_id": canon,
        "google_rating": rating_f,
        "google_review_count": count_i,
        "places_match_confidence": _match_conf_text(confidence),
        "places_enriched_date": date.today().isoformat(),
        "website_from_google_low_confidence": False,
    }


def run_photographers_gap_fill(
    sb: Any,
    http: httpx.Client,
    tracker: Any,
    *,
    limit: int,
    dry_run: bool,
    log: Any,
) -> dict[str, Any]:
    rows = fetch_photographer_gap_cohort(sb, limit)
    log.info(
        "photographers cohort fetched count=%s cap=%s",
        len(rows),
        limit,
    )

    enriched_high = enriched_low = skipped = stopped_budget = 0
    total_query_variations = 0
    vendors_touched = 0

    for row in rows:
        pid_own = str(row.get("photographer_id") or "").strip()
        label = _display_label(row)
        suburb = str(row.get("suburb") or "").strip()
        state = str(row.get("state") or "").strip()

        if not pid_own or not label:
            skipped += 1
            continue

        vendors_touched += 1
        fp = find_place_with_fallbacks(
            name=label,
            state=state or None,
            suburb=suburb or None,
            vendor_type="photographers",
            http=http,
            tracker=tracker,
            logger=log,
            place_id_claimed=lambda pid: _place_id_claimed_elsewhere(sb, pid, pid_own),
        )
        total_query_variations += fp.queries_tried

        if fp.budget_exhausted:
            stopped_budget += 1
            break

        if fp.details is None:
            log.info("no match across 5 variations, skipping %s", pid_own)
            skipped += 1
            continue

        confidence = fp.confidence
        query_used = fp.query_used or ""

        if confidence >= HIGH_CONFIDENCE_THRESHOLD:
            proposed = _compose_proposed_high(row, fp.details, confidence)
            augment_body = augmented_subset(row, proposed, augment_keys=_PHOTO_AUG_KEYS)
            patch_final = dict(augment_body)
            for k in _PATCH_ALWAYS:
                patch_final[k] = proposed[k]

            if dry_run:
                log.info(
                    "dry-run photographers photographer_id=%s would_patch_keys=%s match=%.2f",
                    pid_own,
                    sorted(patch_final.keys()),
                    confidence,
                )
                enriched_high += 1
                continue

            sb.table("photographers").update(patch_final).eq("photographer_id", pid_own).execute()
            enriched_high += 1
            log.info(
                "enriched (high) %s match=%.2f query='%s'",
                pid_own,
                confidence,
                query_used,
            )

        elif confidence >= LOW_CONFIDENCE_THRESHOLD:
            wuri = str(fp.details.get("websiteUri") or "").strip() or None
            now_iso = datetime.now(timezone.utc).isoformat()
            proposed = {
                "website_from_google": wuri,
                "website_from_google_low_confidence": True,
                "places_match_confidence": _match_conf_text(confidence),
                "last_directory_check_at": now_iso,
            }
            augment_body = augmented_subset(row, proposed, augment_keys=_PHOTO_AUG_KEYS)
            patch_final = {k: v for k, v in augment_body.items() if v is not None}
            patch_final["website_from_google_low_confidence"] = True
            patch_final["places_match_confidence"] = _match_conf_text(confidence)
            patch_final["last_directory_check_at"] = now_iso

            if dry_run:
                log.info(
                    "dry-run low-conf photographers photographer_id=%s keys=%s match=%.2f",
                    pid_own,
                    sorted(patch_final.keys()),
                    confidence,
                )
                enriched_low += 1
                continue

            if not patch_final:
                log.info("low-conf photographers photographer_id=%s no augmentable fields, skip", pid_own)
                skipped += 1
                continue

            sb.table("photographers").update(patch_final).eq("photographer_id", pid_own).execute()
            enriched_low += 1
            log.info(
                "low-conf website captured %s match=%.2f query='%s'",
                pid_own,
                confidence,
                query_used,
            )

        else:
            log.info("all variations below 0.50, skip %s", pid_own)
            skipped += 1

    return {
        "processed_type": "photographers",
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
