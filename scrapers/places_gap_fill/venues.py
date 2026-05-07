"""Venues Tier 1 gap-fill (Places Text Search → Details with venue-only richness)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx
from thefuzz import fuzz

from ._framework import (
    MATCH_SCORE_THRESHOLD,
    PLACE_DETAILS_COST,
    TEXT_SEARCH_COST,
    augmented_subset,
    confidence_band_from_pct,
    is_blank_for_augment,
)
from ._places_client import (
    display_name_text,
    extract_universal_fields,
    extract_venue_specific_fields,
    place_details_budgeted,
    place_id_from_resource,
    text_search_budgeted,
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
}

_EXTRA_ALWAYS_KEYS = frozenset(
    {"places_match_confidence", "enrichment_date", "enrichment_status"},
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


def _build_query(label: str, suburb: str, state: str) -> str:
    parts = (
        label,
        (str(suburb or "").strip()),
        (str(state or "").strip()),
        "Australia",
    )
    return ", ".join(p for p in parts if p)


def _venue_search_queries(label: str, suburb: str, state: str) -> list[str]:
    base = _build_query(label, suburb, state)
    st = str(state or "").strip()
    out: list[str] = []
    if base:
        out.append(base)
    if label and st:
        alt = ", ".join(p for p in (label, "wedding venue", st, "Australia") if p)
        if alt and alt not in out:
            out.append(alt)
    return out


def _pick_candidate(
    places: list[dict[str, Any]],
    *,
    match_name: str,
    venue_id: str,
    sb: Any,
    log: Any,
) -> tuple[dict[str, Any] | None, float, str | None]:
    best_place: dict[str, Any] | None = None
    best_pct = -1
    excluded_below = 0
    for cand in places:
        cand_nm = display_name_text(cand)
        pct = fuzz.token_sort_ratio(match_name.lower(), (cand_nm or "").lower())
        if pct / 100.0 < MATCH_SCORE_THRESHOLD:
            excluded_below += 1
            continue
        pid = place_id_from_resource(str(cand.get("name") or cand.get("id") or ""))
        if _place_id_claimed_elsewhere(sb, pid, venue_id):
            excluded_below += 1
            continue
        if pct > best_pct:
            best_place = cand
            best_pct = pct

    if best_place is None or best_pct < 0:
        log.info(
            "low confidence cohort=venues match_name=%s excluded_below_threshold=%s",
            match_name,
            excluded_below,
        )
        return None, 0.0, None

    pct_i = max(0, min(100, int(best_pct)))
    return best_place, best_pct / 100.0, confidence_band_from_pct(pct_i)


def _compose_proposed(existing: dict[str, Any], det: dict[str, Any], conf: str) -> dict[str, Any]:
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
    enriched = skipped = low_conf = stopped_budget = 0

    for row in rows:
        vid = str(row.get("id") or "").strip()
        label = _search_label(row)
        suburb = str(row.get("suburb") or "").strip()
        state = str(row.get("state") or "").strip()

        if not vid or not label:
            skipped += 1
            continue

        ts_payload: dict[str, Any] = {}
        venues_hits: list[dict[str, Any]] = []

        for attempt in _venue_search_queries(label, suburb, state):
            if tracker is not None and not tracker.can_afford(TEXT_SEARCH_COST):
                stopped_budget += 1
                break
            ts_payload = text_search_budgeted(http, tracker, query=attempt)
            if ts_payload.get("_skipped") == "budget":
                stopped_budget += 1
                break
            cand = ts_payload.get("places") or []
            if cand:
                venues_hits = cand
                log.info(
                    "venue text search ok id=%s query=%s hits=%s",
                    vid,
                    attempt,
                    len(cand),
                )
                break

        if stopped_budget:
            break

        if not venues_hits:
            log.info("no text search hits venues id=%s", vid)
            skipped += 1
            continue

        cand, pct_f, bucket = _pick_candidate(
            venues_hits,
            match_name=label,
            venue_id=vid,
            sb=sb,
            log=log,
        )

        if cand is None:
            low_conf += 1
            continue

        if tracker is not None and not tracker.can_afford(PLACE_DETAILS_COST):
            stopped_budget += 1
            break

        pid = place_id_from_resource(str(cand.get("name") or cand.get("id") or ""))
        details = place_details_budgeted(http, tracker, place_id=pid, include_venue_only=True)
        if details.get("_skipped") == "budget":
            stopped_budget += 1
            break

        pct_i = int(round(min(1.0, max(0.0, pct_f)) * 100))
        bucket_final = bucket or confidence_band_from_pct(pct_i)
        proposed = _compose_proposed(row, details, bucket_final)

        augment_body = augmented_subset(row, proposed, augment_keys=_VENUES_AUGMENT_KEYS)
        patch_final = dict(augment_body)
        for k in _EXTRA_ALWAYS_KEYS:
            patch_final[k] = proposed[k]

        if dry_run:
            log.info(
                "dry-run venues id=%s would_patch_keys=%s match=%.3f",
                vid,
                sorted(patch_final.keys()),
                pct_f,
            )
            enriched += 1
            continue

        sb.table("venues").update(patch_final).eq("id", vid).execute()
        enriched += 1
        log.info("enriched venues id=%s match=%.3f", vid, pct_f)

    return {
        "processed_type": "venues",
        "cohort_requested_cap": limit,
        "cohort_loaded": len(rows),
        "enriched": enriched,
        "skipped": skipped,
        "low_confidence": low_conf,
        "stopped_budget": stopped_budget,
    }
