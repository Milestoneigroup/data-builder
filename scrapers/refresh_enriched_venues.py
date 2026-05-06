"""Refresh Supabase ``public.venues`` rows using Google Places Place Details only.

No Text Search — each row must already have ``place_id``. Fills NULL gaps (layer 1)
and refreshes time-sensitive fields (layer 2) per product rules.

Run: ``python -m scrapers.refresh_enriched_venues`` (from repo root)

Requires ``env.local`` with ``GOOGLE_MAPS_API_KEY`` (or ``GOOGLE_PLACES_API_KEY``),
``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY``. Optional: ``DATABASE_URL`` for
printed validation queries (same as other data-builder scrapers).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from dotenv import load_dotenv

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Pre-flight: authoritative env file for this run (see operator runbook).
load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger("refresh_enriched_venues")

REQUEST_DELAY_S = 0.3
MAX_RUNTIME_S = 90 * 60
MAX_SPEND_USD = 20.0
COST_PER_DETAILS_USD = 0.04
MAX_PLACES_CALLS = int(MAX_SPEND_USD / COST_PER_DETAILS_USD)

PLACES_GET_TMPL = "https://places.googleapis.com/v1/places/{place_id}"
# ``liveMusic`` included so ``has_live_music`` can be layer-1 filled (Places API field).
DETAILS_FIELD_MASK = (
    "id,displayName,formattedAddress,"
    "websiteUri,nationalPhoneNumber,"
    "rating,userRatingCount,businessStatus,googleMapsUri,"
    "primaryType,types,editorialSummary,priceLevel,"
    "regularOpeningHours,photos,"
    "accessibilityOptions,goodForGroups,servesWine,servesBeer,"
    "parkingOptions,outdoorSeating,liveMusic"
)


def _api_key() -> str:
    k = (
        (os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or "")
        .strip()
    )
    if not k:
        raise RuntimeError(
            "GOOGLE_MAPS_API_KEY or GOOGLE_PLACES_API_KEY is required in env.local"
        )
    return k


def _sleep_between_calls() -> None:
    time.sleep(REQUEST_DELAY_S)


def _place_id_from_resource(name: str | None) -> str:
    if not name:
        return ""
    if name.startswith("places/"):
        return name.split("/", 1)[1]
    return name


def _normalize_place_id_for_url(place_id: str) -> str:
    s = place_id.strip()
    if s.startswith("places/"):
        return s.split("/", 1)[1]
    return s


def _localized_text(t: Any) -> str:
    if isinstance(t, dict):
        return str(t.get("text") or "")
    return ""


def _price_level_to_int(raw: Any) -> int | None:
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


def _bool_py(val: Any) -> bool | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return None


def _parking_bool(park: dict[str, Any] | None, key: str) -> bool | None:
    if not park or not isinstance(park, dict):
        return None
    if key not in park:
        return None
    try:
        return bool(park[key])
    except (TypeError, ValueError):
        return None


def _photo_refs(photos: list[dict[str, Any]] | None) -> tuple[str | None, str | None, str | None, str | None, int]:
    refs: list[str | None] = [None, None, None, None]
    n = len(photos or [])
    if photos:
        for i, ph in enumerate(photos[:4]):
            name = ph.get("name") or ph.get("photo_reference")
            refs[i] = str(name).strip() if name else None
    return refs[0], refs[1], refs[2], refs[3], n


def _layer1_text_empty(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, str) and not val.strip():
        return True
    return False


def _places_details(client: httpx.Client, place_id: str, api_key: str) -> dict[str, Any]:
    pid = quote(_normalize_place_id_for_url(place_id), safe="")
    url = PLACES_GET_TMPL.format(place_id=pid)
    r = client.get(
        url,
        headers={
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": DETAILS_FIELD_MASK,
        },
        timeout=60.0,
    )
    _sleep_between_calls()
    if r.status_code >= 400:
        try:
            body = r.json()
            msg = (body.get("error") or {}).get("message", r.text[:400])
        except Exception:  # noqa: BLE001
            msg = r.text[:400] if r.text else str(r.status_code)
        return {"_error": msg}
    data = r.json()
    if isinstance(data, dict) and "error" in data:
        err = data.get("error") or {}
        return {"_error": err.get("message", str(err))}
    return data if isinstance(data, dict) else {}


def _parse_google_payload(det: dict[str, Any], fallback_place_id: str) -> dict[str, Any]:
    pid = _place_id_from_resource(det.get("name")) or fallback_place_id
    acc = det.get("accessibilityOptions") if isinstance(det.get("accessibilityOptions"), dict) else {}
    park = det.get("parkingOptions") if isinstance(det.get("parkingOptions"), dict) else {}
    photos = det.get("photos") if isinstance(det.get("photos"), list) else None
    p1, p2, p3, p4, ptotal = _photo_refs(photos)
    opening = det.get("regularOpeningHours")
    opening_payload = opening if isinstance(opening, (dict, list)) else None
    rating_f: float | None
    if "rating" in det:
        rating = det.get("rating")
        try:
            rating_f = float(rating) if rating is not None else None
        except (TypeError, ValueError):
            rating_f = None
    else:
        rating_f = None
    urc: int | None
    if "userRatingCount" in det:
        urt = det.get("userRatingCount")
        try:
            urc = int(urt) if urt is not None else None
        except (TypeError, ValueError):
            urc = None
    else:
        urc = None
    if "businessStatus" in det:
        business = str(det.get("businessStatus") or "").strip() or None
    else:
        business = None
    return {
        "place_id_check": pid,
        "website_from_google": str(det.get("websiteUri") or "").strip() or None,
        "phone": str(det.get("nationalPhoneNumber") or "").strip() or None,
        "editorial_summary": _localized_text(det.get("editorialSummary")) or None,
        "price_level": _price_level_to_int(det.get("priceLevel")),
        "opening_hours": opening_payload,
        "wheelchair_accessible_entrance": _bool_py(acc.get("wheelchairAccessibleEntrance")),
        "has_outdoor_seating": _bool_py(det.get("outdoorSeating")),
        "has_live_music": _bool_py(det.get("liveMusic")),
        "good_for_groups": _bool_py(det.get("goodForGroups")),
        "serves_wine": _bool_py(det.get("servesWine")),
        "serves_beer": _bool_py(det.get("servesBeer")),
        "parking_free_lot": _parking_bool(park, "freeParkingLot"),
        "parking_street": _parking_bool(park, "freeStreetParking"),
        "google_rating": rating_f,
        "google_review_count": urc,
        "business_status": business,
        "g_rating_present": "rating" in det,
        "g_review_count_present": "userRatingCount" in det,
        "g_business_present": "businessStatus" in det,
        "photo_ref_1": p1,
        "photo_ref_2": p2,
        "photo_ref_3": p3,
        "photo_ref_4": p4,
        "total_photo_count": ptotal,
    }


def _norm_photo_ref(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _fetch_venues_page(sb: Any, offset: int, limit: int) -> list[dict[str, Any]]:
    sel = (
        "id,place_id,name,suburb,"
        "website_from_google,phone,editorial_summary,price_level,opening_hours,"
        "wheelchair_accessible_entrance,has_outdoor_seating,has_live_music,"
        "good_for_groups,serves_wine,serves_beer,parking_free_lot,parking_street,"
        "google_rating,google_review_count,business_status,"
        "photo_ref_1,photo_ref_2,photo_ref_3,photo_ref_4,total_photo_count,"
        "photos_downloaded_at,enrichment_date,enrichment_run_at"
    )
    q = (
        sb.table("venues")
        .select(sel)
        .not_.is_("place_id", "null")
        .neq("place_id", "")
        .order("id")
        .range(offset, offset + limit - 1)
    )
    resp = q.execute()
    return list(getattr(resp, "data", None) or [])


def fetch_all_venue_rows(sb: Any) -> list[dict[str, Any]]:
    page_size = 1000
    out: list[dict[str, Any]] = []
    off = 0
    while True:
        batch = _fetch_venues_page(sb, off, page_size)
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        off += page_size
    return out


def _build_update_patch(row: dict[str, Any], g: dict[str, Any]) -> tuple[dict[str, Any], list[str], list[str]]:
    """Return patch, list of layer-1 keys applied, list of layer-2 keys that changed value."""
    today_s = date.today().isoformat()
    now_s = datetime.now(timezone.utc).isoformat()
    patch: dict[str, Any] = {}
    filled: list[str] = []
    refreshed_changed: list[str] = []

    def _track_l2(key: str, old: Any, new: Any) -> None:
        patch[key] = new
        if old != new:
            refreshed_changed.append(key)

    # --- Layer 1 (fill NULL / empty text only) ---
    if _layer1_text_empty(row.get("website_from_google")) and g["website_from_google"]:
        patch["website_from_google"] = g["website_from_google"]
        filled.append("website_from_google")

    if row.get("phone") is None and g["phone"]:
        patch["phone"] = g["phone"]
        filled.append("phone")

    if row.get("editorial_summary") is None and g["editorial_summary"]:
        patch["editorial_summary"] = g["editorial_summary"]
        filled.append("editorial_summary")

    if row.get("price_level") is None and g["price_level"] is not None:
        patch["price_level"] = g["price_level"]
        filled.append("price_level")

    if row.get("opening_hours") is None and g["opening_hours"] is not None:
        patch["opening_hours"] = g["opening_hours"]
        filled.append("opening_hours")

    bool_keys = (
        "wheelchair_accessible_entrance",
        "has_outdoor_seating",
        "has_live_music",
        "good_for_groups",
        "serves_wine",
        "serves_beer",
        "parking_free_lot",
        "parking_street",
    )
    for bk in bool_keys:
        if row.get(bk) is None and g[bk] is not None:
            patch[bk] = g[bk]
            filled.append(bk)

    # --- Layer 2 (always refresh from payload) ---
    old_r = row.get("google_rating")
    try:
        old_rf = float(old_r) if old_r is not None else None
    except (TypeError, ValueError):
        old_rf = None
    new_r = g["google_rating"]
    if g.get("g_rating_present"):
        _track_l2("google_rating", old_rf, new_r)

    old_c = row.get("google_review_count")
    try:
        old_ci = int(old_c) if old_c is not None else None
    except (TypeError, ValueError):
        old_ci = None
    new_c = g["google_review_count"]
    if g.get("g_review_count_present"):
        _track_l2("google_review_count", old_ci, new_c)

    if g.get("g_business_present"):
        _track_l2("business_status", row.get("business_status"), g["business_status"])

    old_p1 = _norm_photo_ref(row.get("photo_ref_1"))
    new_p1 = g["photo_ref_1"]
    for i, key in enumerate(
        ("photo_ref_1", "photo_ref_2", "photo_ref_3", "photo_ref_4"),
        start=1,
    ):
        _track_l2(key, _norm_photo_ref(row.get(key)), g[f"photo_ref_{i}"])

    old_tc = row.get("total_photo_count")
    try:
        old_tci = int(old_tc) if old_tc is not None else None
    except (TypeError, ValueError):
        old_tci = None
    _track_l2("total_photo_count", old_tci, g["total_photo_count"])

    if old_p1 != new_p1:
        patch["photos_downloaded_at"] = None
        refreshed_changed.append("photos_downloaded_at(reset)")

    patch["enrichment_date"] = today_s
    patch["enrichment_run_at"] = now_s

    return patch, filled, refreshed_changed


def print_validation_summary() -> None:
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        LOG.warning("DATABASE_URL not set; skipping validation SQL prints.")
        return
    try:
        import psycopg
    except ImportError:
        LOG.warning("psycopg not installed; skipping validation SQL prints.")
        return
    queries = [
        (
            "coverage",
            """
SELECT
    count(*) AS total,
    count(*) FILTER (WHERE website IS NOT NULL OR website_from_google IS NOT NULL) AS websites_now,
    count(*) FILTER (WHERE phone IS NOT NULL) AS phones_now,
    count(*) FILTER (WHERE opening_hours IS NOT NULL) AS hours_now,
    count(*) FILTER (WHERE editorial_summary IS NOT NULL) AS editorial_now,
    count(*) FILTER (WHERE google_rating IS NOT NULL) AS rating_now
  FROM public.venues
  WHERE place_id IS NOT NULL;
""",
        ),
        (
            "refresh_today",
            """
SELECT count(*) AS refreshed_today FROM public.venues
  WHERE place_id IS NOT NULL
    AND enrichment_run_at::date = CURRENT_DATE;
""",
        ),
        (
            "photo_redownload_sample",
            """
SELECT name, suburb, photo_ref_1
  FROM public.venues
  WHERE place_id IS NOT NULL
    AND photos_downloaded_at IS NULL
    AND photo_ref_1 IS NOT NULL
    AND enrichment_run_at::date = CURRENT_DATE
  LIMIT 5;
""",
        ),
        (
            "audit_curated",
            """
SELECT
    count(*) FILTER (WHERE name IS NULL) AS null_names,
    count(*) FILTER (WHERE capacity_max IS NOT NULL) AS capacity_kept,
    count(*) FILTER (WHERE award_2025 IS NOT NULL) AS awards_kept
  FROM public.venues
  WHERE place_id IS NOT NULL;
""",
        ),
    ]
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            for label, sql in queries:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
                LOG.info("Validation [%s]: %s", label, dict(zip(cols, rows[0])) if rows else rows)


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh enriched venues via Place Details.")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Process at most N venues (0 = no limit other than spend/runtime caps).",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Print validation queries only (no Google API calls).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )

    if args.stats_only:
        print_validation_summary()
        return 0

    from data_builder.config import get_settings
    from supabase import create_client

    st = get_settings()
    url = (st.supabase_url or os.getenv("SUPABASE_URL") or "").strip()
    skey = (
        st.supabase_service_role_key or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
    ).strip()
    if not url or not skey:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")
        return 1

    try:
        places_key = _api_key()
    except RuntimeError as e:
        LOG.error("%s", e)
        return 1

    sb = create_client(url, skey)
    rows = fetch_all_venue_rows(sb)
    total_candidates = len(rows)
    if total_candidates == 0:
        LOG.error("No rows with place_id — stopping (operator must alert Richard).")
        return 2

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    t0 = time.monotonic()
    calls = 0
    refreshed = 0
    errors = 0
    photo_resets = 0

    LOG.info(
        "Starting refresh: %s venue(s) queued (hard cap %s API calls ≈ $%s USD, %.0f min runtime).",
        len(rows),
        MAX_PLACES_CALLS,
        MAX_SPEND_USD,
        MAX_RUNTIME_S / 60,
    )

    with httpx.Client() as http:
        for row in rows:
            if calls >= MAX_PLACES_CALLS:
                LOG.warning(
                    "Spend cap reached (~$%s USD, %s calls) — stopping cleanly.",
                    MAX_SPEND_USD,
                    calls,
                )
                break
            if (time.monotonic() - t0) > MAX_RUNTIME_S:
                LOG.warning("Hard runtime cap (%s min) reached — stopping.", MAX_RUNTIME_S / 60)
                break

            place_id = str(row.get("place_id") or "").strip()
            name = str(row.get("name") or "").strip() or place_id
            if not place_id:
                continue

            calls += 1
            try:
                det = _places_details(http, place_id, places_key)
            except Exception as e:  # noqa: BLE001
                errors += 1
                LOG.error("HTTP error place_id=%s name=%s err=%s", place_id, name, e)
                continue

            if det.get("_error"):
                errors += 1
                LOG.error(
                    "Place Details failed place_id=%s name=%s err=%s",
                    place_id,
                    name,
                    det["_error"],
                )
                continue

            try:
                g = _parse_google_payload(det, place_id)
            except Exception:  # noqa: BLE001
                errors += 1
                LOG.error(
                    "Parse error place_id=%s name=%s\n%s",
                    place_id,
                    name,
                    traceback.format_exc(),
                )
                continue

            patch, filled, l2_changed = _build_update_patch(row, g)
            if not patch:
                continue

            old_p1 = _norm_photo_ref(row.get("photo_ref_1"))
            if old_p1 != g["photo_ref_1"]:
                photo_resets += 1

            try:
                sb.table("venues").update(patch).eq("id", row["id"]).execute()
                refreshed += 1
            except Exception as e:  # noqa: BLE001
                errors += 1
                LOG.error("Supabase update failed id=%s name=%s err=%s", row.get("id"), name, e)
                continue

            l2_note = [x for x in l2_changed if not x.startswith("enrichment_")]
            LOG.info(
                "venue=%s | filled=[%s] | refreshed_changed=[%s]",
                name,
                ", ".join(filled) if filled else "-",
                ", ".join(l2_note) if l2_note else "-",
            )

    elapsed_m = (time.monotonic() - t0) / 60.0
    est_spend = round(calls * COST_PER_DETAILS_USD, 2)
    LOG.info(
        "Finished: API calls=%s (~$%s USD est.), updated_rows=%s, errors=%s, photo_ref_1_resets=%s, runtime=%.2f min",
        calls,
        est_spend,
        refreshed,
        errors,
        photo_resets,
        elapsed_m,
    )
    print_validation_summary()
    return 0 if errors == 0 else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
