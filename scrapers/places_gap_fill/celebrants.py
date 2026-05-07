"""Active-market celebrants with directory URLs → Places universal capture (no venue-only fields)."""

from __future__ import annotations

from datetime import date, datetime, timezone
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
    "places_enriched_date",
    "places_match_confidence",
    "last_places_enrich_at",
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


def _build_query(label: str, suburb: str, state: str) -> str:
    return ", ".join(
        p for p in (label, str(suburb or "").strip(), str(state or "").strip(), "Australia") if p
    )


def _search_queries(label: str, suburb: str, state: str) -> list[str]:
    primary = _build_query(label, suburb, state)
    st = str(state or "").strip()
    tail: list[str] = [primary]
    if label and st:
        tail.append(f"{label} celebrant {st} Australia".strip())
        tail.append(f"{label} wedding celebrant Australia".strip())
    out: list[str] = []
    for q in tail:
        q = q.strip()
        if q and q not in out:
            out.append(q)
    return out


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


def _pick_candidate(
    places: list[dict[str, Any]],
    *,
    match_name: str,
    celebrant_id: str,
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
        if _google_place_claimed_elsewhere(sb, pid, celebrant_id):
            below += 1
            continue
        if pct > best_pct:
            best = cand
            best_pct = pct
    if best is None:
        log.info(
            "low confidence cohort=celebrants match_name=%s excluded_below_threshold=%s",
            match_name,
            below,
        )
        return None, 0.0, "UNKNOWN"
    pct_i = max(0, min(100, int(best_pct)))
    return best, best_pct / 100.0, confidence_band_from_pct(pct_i)


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


def _compose_proposed(existing: dict[str, Any], det: dict[str, Any], confidence: str) -> dict[str, Any]:
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

    enriched = skipped = low_conf = stopped_budget = 0

    for row in rows:
        cid = str(row.get("celebrant_id") or "").strip()
        label = _label(row)
        suburb = str(row.get("suburb") or "").strip()
        state = str(row.get("state") or "").strip()

        if not cid or not label:
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
                    "celebrant text search ok celebrant_id=%s query=%s hits=%s",
                    cid,
                    attempt,
                    len(cand_hits),
                )
                break

        if stopped_budget:
            break

        if not hits:
            log.info("no text search hits celebrants celebrant_id=%s", cid)
            skipped += 1
            continue

        cand, pct_f, bucket = _pick_candidate(
            hits,
            match_name=label,
            celebrant_id=cid,
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
        det = place_details_budgeted(http, tracker, place_id=pid, include_venue_only=False)
        if det.get("_skipped") == "budget":
            stopped_budget += 1
            break

        proposed = _compose_proposed(row, det, bucket)
        augment_body = augmented_subset(row, proposed, augment_keys=_CELEBR_AUGMENT_KEYS)

        patch_final = dict(augment_body)
        for k in _SENTINELS_ALWAYS_WRITE:
            patch_final[k] = proposed[k]

        if dry_run:
            log.info(
                "dry-run celebrants celebrant_id=%s would_patch_keys=%s match=%.3f",
                cid,
                sorted(patch_final.keys()),
                pct_f,
            )
            enriched += 1
            continue

        sb.table("celebrants").update(patch_final).eq("celebrant_id", cid).execute()
        enriched += 1
        log.info("enriched celebrants celebrant_id=%s match=%.3f", cid, pct_f)

    return {
        "processed_type": "celebrants",
        "cohort_requested_cap": limit,
        "cohort_loaded": len(rows),
        "enriched": enriched,
        "skipped": skipped,
        "low_confidence": low_conf,
        "stopped_budget": stopped_budget,
    }
