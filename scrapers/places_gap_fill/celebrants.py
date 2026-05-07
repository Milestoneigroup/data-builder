"""Active-market celebrants with directory URLs → Places universal capture (no venue-only fields)."""

from __future__ import annotations

from datetime import date, datetime, timezone
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
    find_place_with_fallbacks,
    place_id_from_resource,
)
from ._query_builder import fetch_celebrant_gap_cohort

_CELEBR_AUGMENT_KEYS = {
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
    "website_from_places",
    "website_from_google_low_confidence",
    "places_enriched_date",
    "places_match_confidence",
    "last_places_enrich_at",
    "last_directory_check_at",
    "phone_from_places",
    "business_status",
    "lat",
    "lng",
    "google_address_components_json",
    "google_editorial_summary",
}

_SENTINELS_ALWAYS_WRITE = frozenset(
    {
        "places_match_confidence",
        "places_enriched_date",
        "last_places_enrich_at",
    },
)


def _label(row: dict[str, Any]) -> str:
    return (str(row.get("full_name") or "").strip() or str(row.get("name") or "").strip())


def _google_place_claimed_elsewhere(sb: Any, place_id: str, own_id: str) -> bool:
    norm = place_id_from_resource(place_id)
    if not norm:
        return True
    r = (
        sb.table("celebrants")
        .select("celebrant_id")
        .eq("google_place_id", norm)
        .limit(40)
        .execute()
    )
    self_id = str(own_id).strip()
    for rec in getattr(r, "data", None) or []:
        cid = str(rec.get("celebrant_id") or "").strip()
        if cid and cid != self_id:
            return True
    return False


def _rating_text(rating: Any) -> str | None:
    if rating is None:
        return None
    try:
        return f"{float(rating):.1f}"
    except (TypeError, ValueError):
        return None


def _count_text(cnt: Any) -> str | None:
    if cnt is None:
        return None
    try:
        return str(int(cnt))
    except (TypeError, ValueError):
        return None


def _compose_proposed_high(
    existing: dict[str, Any],
    det: dict[str, Any],
    confidence: str,
) -> dict[str, Any]:
    uni = extract_universal_fields(det)
    canon = uni.pop("canonical_place_id") or ""
    rating_src = uni.get("google_rating")
    count_src = uni.get("google_review_count")

    wf = uni.get("website_from_google")
    if wf and not is_blank_for_augment(existing.get("website_from_google")):
        uni.pop("website_from_google", None)
    elif not wf:
        uni.pop("website_from_google", None)

    today = date.today().isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()

    proposed: dict[str, Any] = {
        **uni,
        "google_place_id": canon,
        "google_rating": _rating_text(rating_src),
        "google_review_count": _count_text(count_src),
        "places_match_confidence": confidence,
        "places_enriched_date": today,
        "last_places_enrich_at": now_iso,
        "website_from_google_low_confidence": False,
    }

    wf_places = str(wf or "").strip()
    if wf_places and is_blank_for_augment(existing.get("website_from_places")):
        proposed["website_from_places"] = wf_places

    phone_primary = str(proposed.get("google_phone") or "").strip()
    if phone_primary and is_blank_for_augment(existing.get("phone_from_places")):
        proposed["phone_from_places"] = phone_primary

    return proposed


def run_celebrants_gap_fill(
    sb: Any,
    http: httpx.Client,
    tracker: Any,
    *,
    limit: int,
    dry_run: bool,
    log: Any,
    verify_required: str = "VERIFY_REQUIRED",
) -> dict[str, Any]:
    rows = fetch_celebrant_gap_cohort(sb, limit, verify_required=verify_required)
    log.info(
        "celebrants cohort fetched count=%s cap=%s (active_market+directory_urls+google_gap)",
        len(rows),
        limit,
    )

    enriched_high = enriched_low = skipped = stopped_budget = 0
    total_query_variations = 0
    vendors_touched = 0

    for row in rows:
        cid = str(row.get("celebrant_id") or "").strip()
        label = _label(row)
        suburb = str(row.get("suburb") or "").strip()
        state = str(row.get("state") or "").strip()

        if not cid or not label:
            skipped += 1
            continue

        vendors_touched += 1
        fp = find_place_with_fallbacks(
            name=label,
            state=state or None,
            suburb=suburb or None,
            vendor_type="celebrants",
            http=http,
            tracker=tracker,
            logger=log,
            place_id_claimed=lambda pid: _google_place_claimed_elsewhere(sb, pid, cid),
        )
        total_query_variations += fp.queries_tried

        if fp.budget_exhausted:
            stopped_budget += 1
            break

        if fp.details is None:
            log.info("no match across 5 variations, skipping %s", cid)
            skipped += 1
            continue

        confidence = fp.confidence
        query_used = fp.query_used or ""

        if confidence >= HIGH_CONFIDENCE_THRESHOLD:
            pct_i = max(0, min(100, int(round(confidence * 100))))
            bucket = confidence_band_from_pct(pct_i)
            proposed = _compose_proposed_high(row, fp.details, bucket)

            augment_body = augmented_subset(row, proposed, augment_keys=_CELEBR_AUGMENT_KEYS)

            patch_final = dict(augment_body)
            for k in _SENTINELS_ALWAYS_WRITE:
                patch_final[k] = proposed[k]

            if dry_run:
                log.info(
                    "dry-run celebrants celebrant_id=%s would_patch_keys=%s match=%.2f",
                    cid,
                    sorted(patch_final.keys()),
                    confidence,
                )
                enriched_high += 1
                continue

            sb.table("celebrants").update(patch_final).eq("celebrant_id", cid).execute()
            enriched_high += 1
            log.info(
                "enriched (high) %s match=%.2f query='%s'",
                cid,
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
            augment_body = augmented_subset(row, proposed, augment_keys=_CELEBR_AUGMENT_KEYS)
            patch_final = {k: v for k, v in augment_body.items() if v is not None}
            patch_final["website_from_google_low_confidence"] = True
            patch_final["places_match_confidence"] = conf_bucket
            patch_final["last_directory_check_at"] = now_iso

            if dry_run:
                log.info(
                    "dry-run low-conf celebrants celebrant_id=%s keys=%s match=%.2f",
                    cid,
                    sorted(patch_final.keys()),
                    confidence,
                )
                enriched_low += 1
                continue

            if not patch_final:
                log.info("low-conf celebrants celebrant_id=%s no augmentable fields, skip", cid)
                skipped += 1
                continue

            sb.table("celebrants").update(patch_final).eq("celebrant_id", cid).execute()
            enriched_low += 1
            log.info(
                "low-conf website captured %s match=%.2f query='%s'",
                cid,
                confidence,
                query_used,
            )

        else:
            log.info("all variations below 0.50, skip %s", cid)
            skipped += 1

    return {
        "processed_type": "celebrants",
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
