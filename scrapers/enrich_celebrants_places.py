"""Google Places (New) celebrant enrichment: active-market refresh plus targeted inactive cohort.

Phase 1: all rows with ``is_active_market`` true (ordered; skips recent HIGH refreshes).
Phase 2: up to 300 inactive celebrants with website or directory signals.

Loads secrets from ``env.local`` only. Augmentation columns per runbook; legacy
``*_from_places`` and ``last_*_enrich_at`` are not modified.

Run: ``python -m scrapers.enrich_celebrants_places``
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from dotenv import load_dotenv
from thefuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

load_dotenv(_ROOT / "env.local", override=True)

LOG_PATH = _ROOT / "logs" / "enrich_celebrants_places.log"
VERIFY = "VERIFY_REQUIRED"
REQUEST_DELAY_S = 0.3

# Google Places API (New) — approximate USD unit costs for budget guardrails.
COST_TEXT_SEARCH_USD = 0.032
COST_PLACE_DETAILS_USD = 0.017

TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TEXT_SEARCH_FIELD_MASK = "places.id,places.displayName,places.formattedAddress,places.location"
DETAILS_FIELD_MASK = (
    "id,displayName,formattedAddress,location,"
    "websiteUri,nationalPhoneNumber,rating,userRatingCount,"
    "businessStatus,googleMapsUri,primaryType,types,editorialSummary,priceLevel,"
    "regularOpeningHours,photos,reviews"
)

PHASE1_BUDGET_USD = 35.0
PHASE1_WALL_S = 60 * 60
PHASE2_BUDGET_USD = 25.0
PHASE2_WALL_S = 30 * 60
COMBINED_BUDGET_USD = 60.0
# Tight comparison so estimated spend stays within the combined cap.
_COMBINED_EPS = 1e-6
COMBINED_WALL_S = 90 * 60

ACTIVE_SIGNAL_TOKEN = "places_match_2026_05_07"

logging.getLogger("httpx").setLevel(logging.WARNING)
LOG = logging.getLogger("enrich_celebrants_places")


def _setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s"))
    LOG.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(message)s"))
    LOG.addHandler(sh)
    LOG.setLevel(logging.INFO)


def _api_key() -> str:
    k = (os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or "").strip()
    if not k:
        raise RuntimeError("GOOGLE_MAPS_API_KEY (or GOOGLE_PLACES_API_KEY) required in env.local")
    return k


def _place_id_from_resource(name: str | None) -> str:
    if not name:
        return ""
    if name.startswith("places/"):
        return name.split("/", 1)[1]
    return name


def _display_name_text(obj: dict[str, Any] | None) -> str:
    if not obj:
        return ""
    dn = obj.get("displayName")
    if isinstance(dn, dict):
        return str(dn.get("text") or "")
    return str(obj.get("name") or "")


def _text_search(client: httpx.Client, query: str, key: str) -> dict[str, Any]:
    r = client.post(
        TEXT_SEARCH_URL,
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": key,
            "X-Goog-FieldMask": TEXT_SEARCH_FIELD_MASK,
        },
        json={"textQuery": query},
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json()


def _place_details(client: httpx.Client, place_id: str, key: str) -> dict[str, Any]:
    pid = quote(place_id, safe="")
    r = client.get(
        f"https://places.googleapis.com/v1/places/{pid}",
        headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": DETAILS_FIELD_MASK},
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json()


def _price_level_int(raw: Any) -> int | None:
    if raw is None or raw == "" or str(raw) == "PRICE_LEVEL_UNSPECIFIED":
        return None
    if isinstance(raw, int) and 0 <= raw <= 4:
        return raw
    m = {
        "PRICE_LEVEL_FREE": 0,
        "PRICE_LEVEL_INEXPENSIVE": 1,
        "PRICE_LEVEL_MODERATE": 2,
        "PRICE_LEVEL_EXPENSIVE": 3,
        "PRICE_LEVEL_VERY_EXPENSIVE": 4,
    }
    return m.get(str(raw))


def _photo_refs_three(photos: list[dict[str, Any]] | None) -> tuple[str | None, str | None, str | None]:
    if not photos:
        return None, None, None
    names: list[str | None] = [None, None, None]
    for i, ph in enumerate(photos[:3]):
        names[i] = str(ph.get("name") or "") or None
    return names[0], names[1], names[2]


def _extract_reviews(reviews: list[dict[str, Any]] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    reviews = reviews or []
    for i in range(5):
        n = i + 1
        if i < len(reviews):
            rv = reviews[i]
            t = rv.get("text") or {}
            text = str(t.get("text") or "") if isinstance(t, dict) else str(t or "")
            auth = rv.get("authorAttribution") or {}
            author = str(auth.get("displayName") or "") if isinstance(auth, dict) else ""
            rating_rv = rv.get("rating")
        else:
            text, author, rating_rv = "", "", None
        out[f"review_text_{n}"] = text or None
        out[f"review_author_{n}"] = author or None
        out[f"review_rating_{n}"] = str(rating_rv) if rating_rv is not None else None
    return out


def _confidence_from_score(score: int) -> str | None:
    if score >= 80:
        return "HIGH"
    if score >= 65:
        return "MEDIUM"
    if score >= 55:
        return "LOW"
    return None


def _needs_curated_website(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return s in ("", VERIFY, "nan") or s.lower() in ("null", "none")


def _abia_truthy(val: Any) -> bool:
    t = str(val or "").strip().lower()
    return t in ("true", "1", "yes", "y")


def _parse_enriched_date(val: Any) -> date | None:
    if val is None or val == "":
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    s = str(val).strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _skip_recent_high_confidence(row: dict[str, Any]) -> bool:
    if str(row.get("places_match_confidence") or "").upper() != "HIGH":
        return False
    d = _parse_enriched_date(row.get("places_enriched_date"))
    if d is None:
        return False
    return d >= (date.today() - timedelta(days=7))


def _place_id_taken_by_other_celebrant(sb: Any, place_id: str, own_celebrant_id: str) -> bool:
    norm = _place_id_from_resource(place_id)
    if not norm or norm == VERIFY:
        return True
    r = sb.table("celebrants").select("celebrant_id").eq("google_place_id", norm).limit(10).execute()
    for rec in getattr(r, "data", None) or []:
        if str(rec.get("celebrant_id") or "").strip() != str(own_celebrant_id).strip():
            return True
    return False


def _append_active_signal(sources: Any, token: str) -> str:
    raw = str(sources or "").strip()
    parts = [p.strip() for p in raw.split("|") if p.strip()] if raw else []
    if token not in parts:
        parts.append(token)
    return "|".join(parts)


def _clean_query_token(val: Any) -> str:
    """Strip DB sentinels so search queries do not contain ``VERIFY_REQUIRED``."""
    s = str(val or "").strip()
    if not s or s == VERIFY or s.lower() in ("nan", "null", "none", "verify_required"):
        return ""
    return s


def _build_search_queries(full_name: str, suburb: str, state: str) -> list[str]:
    fn = (full_name or "").strip()
    if not fn:
        return []
    st = _clean_query_token(state)
    su = _clean_query_token(suburb)
    out: list[str] = []
    q0 = " ".join(p for p in (fn, "celebrant", st, "Australia") if p).strip()
    if q0:
        out.append(q0)
    parts_f1: list[str] = [fn, "marriage celebrant"]
    if su:
        parts_f1.append(su)
    if st:
        parts_f1.append(st)
    q1 = " ".join(parts_f1).strip()
    if q1 and q1 not in out:
        out.append(q1)
    q2 = " ".join(p for p in (fn, "wedding celebrant", st) if p).strip()
    if q2 and q2 not in out:
        out.append(q2)
    return out


def _pick_candidate_from_queries(
    http: httpx.Client,
    places_key: str,
    sb: Any,
    *,
    full_name: str,
    queries: list[str],
    celebrant_id: str,
    spend: dict[str, float],
    budgets_ok: Any,
) -> tuple[dict[str, Any] | None, int, str, str]:
    """Returns (place_dict, score, query_used, reason_if_none)."""
    best: tuple[dict[str, Any], int, str] | None = None
    for q in queries:
        if not q:
            continue
        if not budgets_ok():
            return None, 0, "", "budget_or_time"
        try:
            data = _text_search(http, q, places_key)
            spend["usd"] += COST_TEXT_SEARCH_USD
        except Exception:
            time.sleep(REQUEST_DELAY_S)
            raise
        time.sleep(REQUEST_DELAY_S)
        places = data.get("places") or []
        for p in places:
            cand_name = _display_name_text(p)
            score = int(fuzz.token_sort_ratio(full_name.lower(), (cand_name or "").lower()))
            conf = _confidence_from_score(score)
            if conf is None:
                continue
            raw_id = str(p.get("name") or p.get("id") or "")
            pid = _place_id_from_resource(raw_id)
            if _place_id_taken_by_other_celebrant(sb, pid, celebrant_id):
                continue
            if best is None or score > best[1]:
                best = (p, score, q)
        if best is not None:
            break
    if best is None:
        return None, 0, "", "no_match"
    return best[0], best[1], best[2], "ok"


def _details_to_patch(
    det: dict[str, Any],
    *,
    fuzzy_score: int,
    row: dict[str, Any],
    phase2_flag: bool,
) -> dict[str, Any]:
    pid = _place_id_from_resource(str(det.get("name") or det.get("id") or ""))
    gname = _display_name_text(det)
    addr = str(det.get("formattedAddress") or "").strip()
    loc = det.get("location") or {}
    lat = lng = None
    if isinstance(loc, dict):
        lat = loc.get("latitude")
        lng = loc.get("longitude")
    rating = det.get("rating")
    try:
        rf = float(rating) if rating is not None else None
    except (TypeError, ValueError):
        rf = None
    urc = det.get("userRatingCount")
    try:
        urci = int(urc) if urc is not None else None
    except (TypeError, ValueError):
        urci = None

    p1, p2, p3 = _photo_refs_three(det.get("photos") if isinstance(det.get("photos"), list) else None)
    rev_cols = _extract_reviews(det.get("reviews") if isinstance(det.get("reviews"), list) else None)

    oh = det.get("regularOpeningHours")
    oh_s = json.dumps(oh) if oh is not None else None
    types = det.get("types")
    types_s = json.dumps(types) if isinstance(types, list) else None
    ed = det.get("editorialSummary") or {}
    ed_text = str(ed.get("text") or "") if isinstance(ed, dict) else ""

    conf_l = _confidence_from_score(fuzzy_score)
    conf = conf_l or "LOW"

    wuri = str(det.get("websiteUri") or "").strip()

    patch: dict[str, Any] = {
        "google_place_id": pid or VERIFY,
        "google_name": gname or None,
        "google_address": addr or None,
        "lat": float(lat) if lat is not None else None,
        "lng": float(lng) if lng is not None else None,
        "website_from_google": wuri or None,
        "google_phone": str(det.get("nationalPhoneNumber") or "").strip() or None,
        "google_rating": f"{rf:.1f}" if rf is not None else VERIFY,
        "google_review_count": str(urci) if urci is not None else VERIFY,
        "business_status": str(det.get("businessStatus") or "").strip() or None,
        "google_maps_url": str(det.get("googleMapsUri") or "").strip() or None,
        "google_primary_type": str(det.get("primaryType") or "").strip() or None,
        "google_types_json": types_s,
        "editorial_summary": ed_text or None,
        "price_level": _price_level_int(det.get("priceLevel")),
        "opening_hours": oh_s,
        "photo_ref_1": p1,
        "photo_ref_2": p2,
        "photo_ref_3": p3,
        "places_match_confidence": conf,
        "places_enriched_date": date.today().isoformat(),
    }
    patch.update(rev_cols)

    if _needs_curated_website(row.get("website")) and wuri:
        patch["website"] = wuri

    if phase2_flag:
        patch["active_signal_sources"] = _append_active_signal(row.get("active_signal_sources"), ACTIVE_SIGNAL_TOKEN)

    must_allow_none = frozenset({"google_rating", "google_review_count", "google_place_id"})
    return {k: v for k, v in patch.items() if v is not None or k in must_allow_none}


def _sort_key_phase1(row: dict[str, Any]) -> tuple[int, int, int, int, str]:
    tier_f = 0 if str(row.get("content_tier") or "").strip().lower() == "featured" else 1
    abia_f = 0 if _abia_truthy(row.get("abia_winner")) else 1
    pe = row.get("places_enriched_date")
    never = 0 if pe is None or str(pe).strip() == "" else 1
    conf = str(row.get("places_match_confidence") or "").upper()
    conf_o = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}.get(conf, 3 if never == 1 else 0)
    cid = str(row.get("celebrant_id") or "")
    return (tier_f, abia_f, never, conf_o, cid)


def _fetch_paginated(sb: Any, builder: Any) -> list[dict[str, Any]]:
    page = 0
    page_size = 1000
    out: list[dict[str, Any]] = []
    while True:
        q = builder.range(page, page + page_size - 1)
        resp = q.execute()
        batch = getattr(resp, "data", None) or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        page += page_size
    return out


def _fetch_phase1_rows(sb: Any) -> list[dict[str, Any]]:
    q = (
        sb.table("celebrants")
        .select(
            "celebrant_id,full_name,state,suburb,website,content_tier,abia_winner,"
            "places_enriched_date,places_match_confidence,is_active_market,active_signal_sources"
        )
        .eq("is_active_market", True)
        .order("celebrant_id")
    )
    rows = _fetch_paginated(sb, q)
    filtered = [r for r in rows if not _skip_recent_high_confidence(r)]
    filtered.sort(key=_sort_key_phase1)
    return filtered


def _fetch_phase2_candidates(sb: Any, limit: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Tier A then B then C until ``limit`` rows (deduped)."""
    selected: list[dict[str, Any]] = []
    counts = {"A": 0, "B": 0, "C": 0}
    seen: set[str] = set()

    base_cols = (
        "celebrant_id,full_name,state,suburb,website,content_tier,abia_winner,"
        "places_enriched_date,places_match_confidence,is_active_market,active_signal_sources,"
        "easy_weddings_profile_url,wedding_society_profile_url"
    )

    def ingest(batch: list[dict[str, Any]], tier: str) -> None:
        nonlocal selected
        for r in batch:
            if len(selected) >= limit:
                return
            cid = str(r.get("celebrant_id") or "")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            selected.append(r)
            counts[tier] += 1

    need = limit - len(selected)
    if need > 0:
        resp = (
            sb.table("celebrants")
            .select(base_cols)
            .eq("is_active_market", False)
            .is_("places_enriched_date", "null")
            .not_.is_("website", "null")
            .order("celebrant_id")
            .limit(need)
            .execute()
        )
        ingest(list(getattr(resp, "data", None) or []), "A")

    need = limit - len(selected)
    if need > 0:
        resp = (
            sb.table("celebrants")
            .select(base_cols)
            .eq("is_active_market", False)
            .is_("places_enriched_date", "null")
            .not_.is_("easy_weddings_profile_url", "null")
            .order("celebrant_id")
            .limit(max(need * 3, need))
            .execute()
        )
        ingest(list(getattr(resp, "data", None) or []), "B")

    need = limit - len(selected)
    if need > 0:
        resp = (
            sb.table("celebrants")
            .select(base_cols)
            .eq("is_active_market", False)
            .is_("places_enriched_date", "null")
            .not_.is_("wedding_society_profile_url", "null")
            .order("celebrant_id")
            .limit(max(need * 3, need))
            .execute()
        )
        ingest(list(getattr(resp, "data", None) or []), "C")

    return selected[:limit], counts


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Celebrant Places enrichment (two-phase).")
    parser.add_argument(
        "--phase2-only",
        action="store_true",
        help="Run only Phase 2 (for debugging; default runs Phase 1 then Phase 2).",
    )
    args = parser.parse_args(argv)
    _setup_logging()
    try:
        places_key = _api_key()
    except RuntimeError as e:
        LOG.error("%s", e)
        return 1

    url = (os.getenv("SUPABASE_URL") or "").strip()
    skey = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not skey:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required in env.local")
        return 1

    from supabase import create_client

    sb = create_client(url, skey)

    run_started = time.monotonic()
    spend = {"usd": 0.0}
    phase_started = time.monotonic()

    def combined_time_ok() -> bool:
        return (time.monotonic() - run_started) < COMBINED_WALL_S

    def phase1_ok(*, reserve_details: bool = False) -> bool:
        extra = COST_PLACE_DETAILS_USD if reserve_details else 0.0
        if not combined_time_ok():
            return False
        if (time.monotonic() - phase_started) >= PHASE1_WALL_S:
            return False
        if round(spend["usd"] + extra, 6) > COMBINED_BUDGET_USD + _COMBINED_EPS:
            return False
        if spend["usd"] + extra > PHASE1_BUDGET_USD:
            return False
        return True

    phase1_matched = phase1_no_match = phase1_errors = phase1_stopped_cap = 0
    phase2_matched = phase2_no_match = phase2_errors = 0

    with httpx.Client() as http:
        if not args.phase2_only:
            rows_p1 = _fetch_phase1_rows(sb)
            LOG.info("Phase 1: %s active celebrants to process (after recent-HIGH skip)", len(rows_p1))
            for i, row in enumerate(rows_p1):
                if not phase1_ok():
                    LOG.info("Phase 1 stopped at cap (time or USD). Processed %s/%s rows.", i, len(rows_p1))
                    break
                cid = str(row.get("celebrant_id") or "").strip()
                fn = str(row.get("full_name") or "").strip()
                st = str(row.get("state") or "").strip()
                su = str(row.get("suburb") or "").strip()
                if not cid or not fn:
                    continue

                queries = _build_search_queries(fn, su, st)

                def budgets_ok_p1() -> bool:
                    return phase1_ok()

                try:
                    place, score, q_used, reason = _pick_candidate_from_queries(
                        http,
                        places_key,
                        sb,
                        full_name=fn,
                        queries=queries,
                        celebrant_id=cid,
                        spend=spend,
                        budgets_ok=budgets_ok_p1,
                    )
                    if reason == "budget_or_time":
                        phase1_stopped_cap += 1
                        LOG.info(
                            "Phase1 STOPPED_CAP name=%s spend_usd=%.3f",
                            fn,
                            spend["usd"],
                        )
                        time.sleep(REQUEST_DELAY_S)
                        break
                    if place is None:
                        phase1_no_match += 1
                        LOG.info(
                            "Phase1 NO_MATCH name=%s query_tried=%s score=%s reason=%s spend_usd=%.3f",
                            fn,
                            q_used or queries[0],
                            score,
                            reason,
                            spend["usd"],
                        )
                        time.sleep(REQUEST_DELAY_S)
                        continue
                    if not phase1_ok(reserve_details=True):
                        phase1_stopped_cap += 1
                        LOG.info("Phase1 STOPPED_CAP before details name=%s", fn)
                        break
                    raw_id = str(place.get("name") or place.get("id") or "")
                    pid = _place_id_from_resource(raw_id)
                    det = _place_details(http, pid, places_key)
                    spend["usd"] += COST_PLACE_DETAILS_USD
                    time.sleep(REQUEST_DELAY_S)
                    patch = _details_to_patch(det, fuzzy_score=score, row=row, phase2_flag=False)
                    sb.table("celebrants").update(patch).eq("celebrant_id", cid).execute()
                    phase1_matched += 1
                    LOG.info(
                        "Phase1 UPDATED name=%s query=%s fuzzy=%s conf=%s place_id=%s spend_usd=%.3f",
                        fn,
                        q_used,
                        score,
                        patch.get("places_match_confidence"),
                        patch.get("google_place_id"),
                        spend["usd"],
                    )
                except Exception as e:  # noqa: BLE001
                    phase1_errors += 1
                    LOG.exception("Phase1 ERROR name=%s: %s", fn, e)
                time.sleep(REQUEST_DELAY_S)

        # Phase 2
        phase2_clock = time.monotonic()
        phase2_start_spend = spend["usd"]

        def phase2_ok(*, reserve_details: bool = False) -> bool:
            extra = COST_PLACE_DETAILS_USD if reserve_details else 0.0
            if not combined_time_ok():
                return False
            if (time.monotonic() - phase2_clock) >= PHASE2_WALL_S:
                return False
            if round(spend["usd"] + extra, 6) > COMBINED_BUDGET_USD + _COMBINED_EPS:
                return False
            if (spend["usd"] + extra - phase2_start_spend) > PHASE2_BUDGET_USD:
                return False
            return True

        rows_p2, tier_counts = _fetch_phase2_candidates(sb, 300)
        LOG.info(
            "Phase 2: %s candidates (tier A=%s, B=%s, C=%s)",
            len(rows_p2),
            tier_counts.get("A"),
            tier_counts.get("B"),
            tier_counts.get("C"),
        )

        phase2_stopped_cap = 0
        for i, row in enumerate(rows_p2):
            if not phase2_ok():
                LOG.info("Phase 2 stopped at cap. Row %s/%s.", i, len(rows_p2))
                break
            cid = str(row.get("celebrant_id") or "").strip()
            fn = str(row.get("full_name") or "").strip()
            st = str(row.get("state") or "").strip()
            su = str(row.get("suburb") or "").strip()
            if not cid or not fn:
                continue
            queries = _build_search_queries(fn, su, st)

            try:
                place, score, q_used, reason = _pick_candidate_from_queries(
                    http,
                    places_key,
                    sb,
                    full_name=fn,
                    queries=queries,
                    celebrant_id=cid,
                    spend=spend,
                    budgets_ok=phase2_ok,
                )
                if reason == "budget_or_time":
                    phase2_stopped_cap += 1
                    LOG.info("Phase2 STOPPED_CAP name=%s spend_usd=%.3f", fn, spend["usd"])
                    break
                if place is None:
                    phase2_no_match += 1
                    LOG.info(
                        "Phase2 NO_MATCH name=%s query=%s score=%s reason=%s",
                        fn,
                        q_used or queries[0],
                        score,
                        reason,
                    )
                    time.sleep(REQUEST_DELAY_S)
                    continue
                if not phase2_ok(reserve_details=True):
                    phase2_stopped_cap += 1
                    LOG.info("Phase2 STOPPED_CAP before details name=%s", fn)
                    break
                raw_id = str(place.get("name") or place.get("id") or "")
                pid = _place_id_from_resource(raw_id)
                det = _place_details(http, pid, places_key)
                spend["usd"] += COST_PLACE_DETAILS_USD
                time.sleep(REQUEST_DELAY_S)
                patch = _details_to_patch(det, fuzzy_score=score, row=row, phase2_flag=True)
                sb.table("celebrants").update(patch).eq("celebrant_id", cid).execute()
                phase2_matched += 1
                LOG.info(
                    "Phase2 UPDATED name=%s query=%s fuzzy=%s conf=%s",
                    fn,
                    q_used,
                    score,
                    patch.get("places_match_confidence"),
                )
            except Exception as e:  # noqa: BLE001
                phase2_errors += 1
                LOG.exception("Phase2 ERROR name=%s: %s", fn, e)
            time.sleep(REQUEST_DELAY_S)

    elapsed_s = time.monotonic() - run_started
    LOG.info(
        "Finished. Phase1 matched=%s no_match=%s stopped_cap=%s errors=%s | "
        "Phase2 matched=%s no_match=%s stopped_cap=%s errors=%s | "
        "Estimated USD=%.2f | Elapsed_s=%.1f",
        phase1_matched,
        phase1_no_match,
        phase1_stopped_cap,
        phase1_errors,
        phase2_matched,
        phase2_no_match,
        phase2_stopped_cap,
        phase2_errors,
        spend["usd"],
        elapsed_s,
    )
    return 0 if (phase1_errors + phase2_errors) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
