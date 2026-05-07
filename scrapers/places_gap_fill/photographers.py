"""Photographers Tier 1 gap-fill — universal Places fields only."""

from __future__ import annotations

from datetime import date
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
    place_details_budgeted,
    place_id_from_resource,
    text_search_budgeted,
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
    "places_enriched_date",
    "places_match_confidence",
    "business_status",
    "lat",
    "lng",
    "google_address_components_json",
    "google_editorial_summary",
}

_TIMESTAMPS = frozenset({"places_enriched_date", "places_match_confidence"})


def _display_label(row: dict[str, Any]) -> str:
    n = str(row.get("name") or "").strip()
    if n:
        return n
    return str(row.get("business_name") or "").strip()


def _build_query(label: str, suburb: str, state: str) -> str:
    return ", ".join(
        p for p in (label, str(suburb or "").strip(), str(state or "").strip(), "Australia") if p
    )


def _search_queries(label: str, suburb: str, state: str) -> list[str]:
    """Parallel to ``enrich_photographers_places._build_search_queries`` (narrowed)."""

    st = str(state or "").strip()
    su = str(suburb or "").strip()
    n = (label or "").strip()
    qs: list[str] = []

    primary = ", ".join(p for p in (n, su, st, "Australia") if p)
    if primary:
        qs.append(primary)

    qp = ", ".join(p for p in (n, "wedding photographer", st, "Australia") if p)
    if qp and qp not in qs:
        qs.append(qp)

    if su:
        q_photo = ", ".join(p for p in (n, "photography", su, st) if p)
        if q_photo and q_photo not in qs:
            qs.append(q_photo)

    q_fallback = ", ".join(p for p in (n, st, "Australia") if p)
    if q_fallback and q_fallback not in qs:
        qs.append(q_fallback)

    return qs


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


def _pick_candidate(
    places: list[dict[str, Any]],
    *,
    match_name: str,
    photographer_id: str,
    sb: Any,
    log: Any,
) -> tuple[dict[str, Any] | None, float, str]:
    best: dict[str, Any] | None = None
    best_pct = -1
    below = 0
    for cand in places:
        cand_nm = display_name_text(cand)
        pct = fuzz.token_sort_ratio(match_name.lower(), (cand_nm or "").lower())
        if pct / 100.0 < MATCH_SCORE_THRESHOLD:
            below += 1
            continue
        pid = place_id_from_resource(str(cand.get("name") or cand.get("id") or ""))
        if _place_id_claimed_elsewhere(sb, pid, photographer_id):
            below += 1
            continue
        if pct > best_pct:
            best = cand
            best_pct = pct

    if best is None:
        log.info(
            "low confidence cohort=photographers match_name=%s excluded_below_threshold=%s",
            match_name,
            below,
        )
        return None, 0.0, "UNKNOWN"

    pct_i = max(0, min(100, int(best_pct)))
    return best, best_pct / 100.0, confidence_band_from_pct(pct_i)


def _compose_proposed(existing: dict[str, Any], det: dict[str, Any], confidence: str) -> dict[str, Any]:
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
        "places_match_confidence": confidence,
        "places_enriched_date": date.today().isoformat(),
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

    enriched = skipped = low_conf = stopped_budget = 0

    for row in rows:
        pid_own = str(row.get("photographer_id") or "").strip()
        label = _display_label(row)
        suburb = str(row.get("suburb") or "").strip()
        state = str(row.get("state") or "").strip()

        if not pid_own or not label:
            skipped += 1
            continue

        ts_payload: dict[str, Any] = {}
        hits: list[dict[str, Any]] = []

        for attempt in _search_queries(label, suburb, state):
            if tracker is not None and not tracker.can_afford(TEXT_SEARCH_COST):
                stopped_budget += 1
                break
            ts_payload = text_search_budgeted(http, tracker, query=attempt)
            if ts_payload.get("_skipped") == "budget":
                stopped_budget += 1
                hits = []
                break
            cand_hits = ts_payload.get("places") or []
            if cand_hits:
                hits = cand_hits
                log.info(
                    "photographer text search ok photographer_id=%s query=%s hits=%s",
                    pid_own,
                    attempt,
                    len(cand_hits),
                )
                break

        if stopped_budget:
            break

        if not hits:
            log.info("no text search hits photographers photographer_id=%s", pid_own)
            skipped += 1
            continue

        cand, pct_f, bucket = _pick_candidate(
            hits,
            match_name=label,
            photographer_id=pid_own,
            sb=sb,
            log=log,
        )
        if cand is None:
            low_conf += 1
            continue

        if tracker is not None and not tracker.can_afford(PLACE_DETAILS_COST):
            stopped_budget += 1
            break

        gid = place_id_from_resource(str(cand.get("name") or cand.get("id") or ""))
        det = place_details_budgeted(http, tracker, place_id=gid, include_venue_only=False)
        if det.get("_skipped") == "budget":
            stopped_budget += 1
            break

        proposed = _compose_proposed(row, det, bucket)
        augment_body = augmented_subset(row, proposed, augment_keys=_PHOTO_AUG_KEYS)
        patch_final = dict(augment_body)

        for k in _TIMESTAMPS:
            patch_final[k] = proposed[k]

        if dry_run:
            log.info(
                "dry-run photographers photographer_id=%s would_patch_keys=%s match=%.3f",
                pid_own,
                sorted(patch_final.keys()),
                pct_f,
            )
            enriched += 1
            continue

        sb.table("photographers").update(patch_final).eq("photographer_id", pid_own).execute()
        enriched += 1
        log.info("enriched photographers photographer_id=%s match=%.3f", pid_own, pct_f)

    return {
        "processed_type": "photographers",
        "cohort_requested_cap": limit,
        "cohort_loaded": len(rows),
        "enriched": enriched,
        "skipped": skipped,
        "low_confidence": low_conf,
        "stopped_budget": stopped_budget,
    }
