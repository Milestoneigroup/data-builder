"""Google Places (New) full enrichment for chain-seed venue skeleton rows.

Targets ``public.venues`` where ``data_source = 'chain_seed_csv_v3'`` and ``place_id`` is NULL.
Uses Text Search (first hit) plus fuzzy name match, then Place Details. Augments only —
curated CSV fields (name, region, chain flags, etc.) are never overwritten.

Run: ``python -m scrapers.enrich_chain_seed_venues``

Requires ``env.local`` (``load_dotenv(..., override=True)``) with ``GOOGLE_MAPS_API_KEY``,
``SUPABASE_URL``, and ``SUPABASE_SERVICE_ROLE_KEY``. Optional ``DATABASE_URL`` prints the
validation queries from the operator runbook after the run completes.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from dotenv import load_dotenv
from thefuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / "env.local", override=True)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

LOG = logging.getLogger("enrich_chain_seed_venues")

CHAIN_DATA_SOURCE = "chain_seed_csv_v3"
REQUEST_DELAY_S = 0.3
MAX_RUNTIME_S = 90 * 60
MAX_SPEND_USD = 25.0
# Operator planning figures (Places API New — adjust if Google pricing changes).
COST_TEXT_SEARCH_USD = 0.032
COST_DETAILS_USD = 0.04

TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TEXT_SEARCH_FIELD_MASK = (
    "places.id,places.displayName,places.formattedAddress,places.location"
)
DETAILS_FIELD_MASK = (
    "id,displayName,formattedAddress,location,websiteUri,nationalPhoneNumber,"
    "rating,userRatingCount,businessStatus,googleMapsUri,primaryType,types,"
    "editorialSummary,priceLevel,regularOpeningHours,photos,accessibilityOptions,"
    "goodForGroups,servesWine,servesBeer,parkingOptions,outdoorSeating"
)

LOG_PATH = _ROOT / "logs" / "enrich_chain_seed_venues.log"


def _setup_file_log() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s"))
    LOG.addHandler(fh)
    LOG.setLevel(logging.INFO)


def _api_key() -> str:
    k = (os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or "").strip()
    if not k:
        raise RuntimeError(
            "GOOGLE_MAPS_API_KEY (or GOOGLE_PLACES_API_KEY) is required in env.local"
        )
    return k


def _sleep() -> None:
    time.sleep(REQUEST_DELAY_S)


def _place_id_from_resource(name: str | None) -> str:
    if not name:
        return ""
    s = str(name).strip()
    if s.startswith("places/"):
        return s.split("/", 1)[1]
    return s


def _display_name_text(obj: dict[str, Any] | None) -> str:
    if not obj:
        return ""
    dn = obj.get("displayName")
    if isinstance(dn, dict):
        return str(dn.get("text") or "").strip()
    return str(obj.get("name") or "").strip()


def _localised_editorial_summary(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        t = str(raw.get("text") or "").strip()
        return t or None
    s = str(raw).strip()
    return s or None


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


def _photo_refs_four(
    photos: list[dict[str, Any]] | None,
) -> tuple[str | None, str | None, str | None, str | None, int]:
    refs: list[str | None] = [None, None, None, None]
    n = len(photos or [])
    if photos:
        for i, ph in enumerate(photos[:4]):
            name = ph.get("name")
            refs[i] = str(name).strip() if name else None
    return refs[0], refs[1], refs[2], refs[3], n


def _confidence_from_score(score: int) -> str | None:
    if score >= 85:
        return "HIGH"
    if score >= 70:
        return "MEDIUM"
    if score >= 60:
        return "LOW"
    return None


def _text_search(client: httpx.Client, query: str, api_key: str) -> dict[str, Any]:
    r = client.post(
        TEXT_SEARCH_URL,
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": TEXT_SEARCH_FIELD_MASK,
        },
        json={"textQuery": query},
        timeout=60.0,
    )
    _sleep()
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


def _place_details(client: httpx.Client, place_id: str, api_key: str) -> dict[str, Any]:
    pid = quote(_place_id_from_resource(place_id), safe="")
    url = f"https://places.googleapis.com/v1/places/{pid}"
    r = client.get(
        url,
        headers={"X-Goog-Api-Key": api_key, "X-Goog-FieldMask": DETAILS_FIELD_MASK},
        timeout=60.0,
    )
    _sleep()
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


def _build_address_update(existing: Any, google_formatted: str) -> str | None:
    """Return new address when Google's line should replace the stored CSV value."""
    g = (google_formatted or "").strip()
    if not g:
        return None
    if existing is None:
        return g
    if isinstance(existing, str):
        s = existing.strip()
        if len(s) < 20:
            return g
        return None
    return None


def _details_to_patch(
    det: dict[str, Any],
    *,
    place_id_res: str,
    confidence: str,
    fuzzy_score: int,
    existing_address: Any,
    today_s: str,
    now_iso: str,
) -> dict[str, Any]:
    loc = det.get("location") if isinstance(det.get("location"), dict) else {}
    lat = loc.get("latitude")
    lng = loc.get("longitude")
    try:
        lat_f = float(lat) if lat is not None else None
    except (TypeError, ValueError):
        lat_f = None
    try:
        lng_f = float(lng) if lng is not None else None
    except (TypeError, ValueError):
        lng_f = None

    acc = det.get("accessibilityOptions") if isinstance(det.get("accessibilityOptions"), dict) else {}
    park = det.get("parkingOptions") if isinstance(det.get("parkingOptions"), dict) else {}
    photos = det.get("photos") if isinstance(det.get("photos"), list) else None
    p1, p2, p3, p4, ptotal = _photo_refs_four(photos)

    opening = det.get("regularOpeningHours")
    opening_payload = opening if isinstance(opening, (dict, list)) else None

    rating_f: float | None
    if det.get("rating") is not None:
        try:
            rating_f = float(det["rating"])
        except (TypeError, ValueError):
            rating_f = None
    else:
        rating_f = None

    urc: int | None
    if det.get("userRatingCount") is not None:
        try:
            urc = int(det["userRatingCount"])
        except (TypeError, ValueError):
            urc = None
    else:
        urc = None

    goog_name = _display_name_text(det) or None
    goog_addr = str(det.get("formattedAddress") or "").strip()

    patch: dict[str, Any] = {
        "place_id": _place_id_from_resource(det.get("name")) or place_id_res,
        "google_name": goog_name,
        "lat": lat_f,
        "lng": lng_f,
        "website_from_google": str(det.get("websiteUri") or "").strip() or None,
        "phone": str(det.get("nationalPhoneNumber") or "").strip() or None,
        "google_rating": rating_f,
        "google_review_count": urc,
        "business_status": str(det.get("businessStatus") or "").strip() or None,
        "google_maps_url": str(det.get("googleMapsUri") or "").strip() or None,
        "google_primary_type": str(det.get("primaryType") or "").strip() or None,
        "editorial_summary": _localised_editorial_summary(det.get("editorialSummary")),
        "price_level": _price_level_int(det.get("priceLevel")),
        "opening_hours": opening_payload,
        "photo_ref_1": p1,
        "photo_ref_2": p2,
        "photo_ref_3": p3,
        "photo_ref_4": p4,
        "total_photo_count": ptotal,
        "wheelchair_accessible_entrance": _bool_py(acc.get("wheelchairAccessibleEntrance")),
        "has_outdoor_seating": _bool_py(det.get("outdoorSeating")),
        "good_for_groups": _bool_py(det.get("goodForGroups")),
        "serves_wine": _bool_py(det.get("servesWine")),
        "serves_beer": _bool_py(det.get("servesBeer")),
        "parking_free_lot": _parking_bool(park, "freeParkingLot"),
        "parking_street": _parking_bool(park, "freeStreetParking"),
        "places_match_confidence": confidence,
        "fuzzy_match_score": fuzzy_score,
        "enrichment_date": today_s,
        "enrichment_status": "places_enriched_chain_seed",
        "enrichment_run_at": now_iso,
    }

    new_addr = _build_address_update(existing_address, goog_addr)
    if new_addr is not None:
        patch["address"] = new_addr

    return patch


def _place_id_taken_by_other(sb: Any, place_id: str, own_venue_id: str) -> bool:
    """True when ``place_id`` is already assigned to a different ``public.venues`` row."""
    norm = _place_id_from_resource(place_id)
    if not norm:
        return True
    r = sb.table("venues").select("id").eq("place_id", norm).limit(1).execute()
    rows = getattr(r, "data", None) or []
    if not rows:
        return False
    return str(rows[0].get("id") or "").strip() != str(own_venue_id).strip()


def _pick_place_candidate(
    places: list[dict[str, Any]], *, venue_name: str, sb: Any, own_venue_id: str
) -> tuple[dict[str, Any] | None, int, str | None]:
    """First search hit that passes fuzzy bands and is not ``place_id``-colliding with another row."""
    for cand in places:
        cand_name = _display_name_text(cand)
        fuzzy_score = int(fuzz.token_sort_ratio(venue_name.lower(), (cand_name or "").lower()))
        conf = _confidence_from_score(fuzzy_score)
        if conf is None:
            continue
        raw_id = str(cand.get("name") or cand.get("id") or "")
        pid = _place_id_from_resource(raw_id)
        if not pid:
            continue
        if _place_id_taken_by_other(sb, pid, own_venue_id):
            continue
        return cand, fuzzy_score, conf
    return None, 0, None


def fetch_chain_skeleton_venues(sb: Any) -> list[dict[str, Any]]:
    page = 1000
    off = 0
    out: list[dict[str, Any]] = []
    while True:
        r = (
            sb.table("venues")
            .select("id,name,suburb,state,address")
            .eq("data_source", CHAIN_DATA_SOURCE)
            .is_("place_id", "null")
            .order("id")
            .range(off, off + page - 1)
            .execute()
        )
        batch = list(getattr(r, "data", None) or [])
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        off += page
    return out


def print_validation_summary() -> None:
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if not db_url:
        LOG.warning("DATABASE_URL not set; run validation SQL from the runbook in Supabase SQL.")
        return
    try:
        import psycopg
    except ImportError:
        LOG.warning("psycopg not installed; skipping validation SQL.")
        return
    queries = [
        (
            "Q1_final_state",
            """SELECT
    count(*) FILTER (WHERE data_source = 'chain_seed_csv_v3'
                       AND place_id IS NOT NULL)             AS now_enriched,
    count(*) FILTER (WHERE data_source = 'chain_seed_csv_v3'
                       AND place_id IS NULL)                 AS still_skeletons,
    count(*) FILTER (WHERE data_source = 'chain_seed_csv_v3'
                       AND places_match_confidence = 'HIGH') AS high_conf,
    count(*) FILTER (WHERE data_source = 'chain_seed_csv_v3'
                       AND places_match_confidence = 'MEDIUM') AS medium_conf,
    count(*) FILTER (WHERE data_source = 'chain_seed_csv_v3'
                       AND places_match_confidence = 'LOW')   AS low_conf
  FROM public.venues;""",
        ),
        (
            "Q2_field_coverage",
            """SELECT
    count(*) FILTER (WHERE website_from_google IS NOT NULL) AS with_website,
    count(*) FILTER (WHERE phone IS NOT NULL)                AS with_phone,
    count(*) FILTER (WHERE google_rating IS NOT NULL)        AS with_rating,
    count(*) FILTER (WHERE photo_ref_1 IS NOT NULL)          AS with_photos,
    count(*) FILTER (WHERE editorial_summary IS NOT NULL)    AS with_editorial,
    count(*) FILTER (WHERE opening_hours IS NOT NULL)        AS with_hours
  FROM public.venues
  WHERE data_source = 'chain_seed_csv_v3' AND place_id IS NOT NULL;""",
        ),
        (
            "Q3_spot_check",
            """SELECT name, suburb, state, google_name, places_match_confidence, fuzzy_match_score,
         google_rating, google_review_count
  FROM public.venues
  WHERE data_source = 'chain_seed_csv_v3'
    AND place_id IS NOT NULL
    AND enrichment_run_at::date = CURRENT_DATE
  ORDER BY random() LIMIT 5;""",
        ),
        (
            "Q4_null_names",
            """SELECT count(*) AS null_name_count FROM public.venues
  WHERE data_source = 'chain_seed_csv_v3' AND name IS NULL;""",
        ),
        (
            "Q5_low_conf",
            """SELECT name, suburb, state, google_name, fuzzy_match_score
  FROM public.venues
  WHERE data_source = 'chain_seed_csv_v3'
    AND places_match_confidence = 'LOW'
  ORDER BY fuzzy_match_score;""",
        ),
    ]
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            for label, sql in queries:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description] if cur.description else []
                print(f"\n=== {label} ===")
                for row in rows:
                    line = dict(zip(cols, row))
                    LOG.info("Validation %s: %s", label, line)
                    print(line)


def run() -> int:
    _setup_file_log()
    from supabase import create_client

    url = (os.getenv("SUPABASE_URL") or "").strip()
    skey = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not skey:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
        return 1
    try:
        gkey = _api_key()
    except RuntimeError as e:
        LOG.error("%s", e)
        return 1

    sb = create_client(url, skey)
    rows = fetch_chain_skeleton_venues(sb)
    pending = len(rows)
    LOG.info("Baseline chain-seed skeleton rows loaded: %s", pending)
    print(f"Chain-seed skeleton venues (place_id IS NULL): {pending}")
    if pending == 0:
        print("Nothing to do — pending count is zero.")
        return 0

    start = time.monotonic()
    spend = 0.0
    n_high = n_medium = n_low = n_no_match = n_errors = n_enriched = 0
    attempted = 0
    stopped_reason: str | None = None

    today_s = date.today().isoformat()

    with httpx.Client() as http:
        for i, row in enumerate(rows):
            attempted = i + 1
            elapsed = time.monotonic() - start
            if elapsed > MAX_RUNTIME_S:
                stopped_reason = "90-minute runtime cap"
                LOG.warning("Stopped: hard runtime limit reached.")
                print("Stopped: 90-minute runtime cap reached.")
                break
            if spend + COST_TEXT_SEARCH_USD > MAX_SPEND_USD:
                stopped_reason = "USD spend cap before Text Search"
                LOG.warning("Stopped: spend cap before next Text Search (spent ~$%.2f).", spend)
                print("Stopped: USD spend cap reached (before Text Search).")
                break

            vid = str(row.get("id") or "").strip()
            name = str(row.get("name") or "").strip()
            suburb = str(row.get("suburb") or "").strip()
            state = str(row.get("state") or "").strip()
            if not vid or not name:
                continue

            query = " ".join(p for p in (name, suburb, state, "Australia") if p).strip()

            try:
                ts = _text_search(http, query, gkey)
                spend += COST_TEXT_SEARCH_USD

                if ts.get("_error"):
                    n_errors += 1
                    LOG.info(
                        "name=%r fuzzy_score=N/A confidence=ERROR place_id=NO MATCH err=%s",
                        name,
                        ts["_error"],
                    )
                    continue

                places = ts.get("places") or []
                if not places:
                    n_no_match += 1
                    LOG.info(
                        "name=%r fuzzy_score=0 confidence=NO_MATCH place_id=NO MATCH (empty results)",
                        name,
                    )
                    continue

                chosen, fuzzy_score, conf = _pick_place_candidate(
                    places, venue_name=name, sb=sb, own_venue_id=vid
                )
                if chosen is None or conf is None:
                    n_no_match += 1
                    LOG.info(
                        "name=%r fuzzy_score=%s confidence=NO_MATCH place_id=NO MATCH",
                        name,
                        fuzzy_score,
                    )
                    continue

                raw_id = str(chosen.get("name") or chosen.get("id") or "")
                pid = _place_id_from_resource(raw_id)
                if not pid:
                    n_no_match += 1
                    LOG.info(
                        "name=%r fuzzy_score=%s confidence=NO_MATCH place_id=NO MATCH (missing id)",
                        name,
                        fuzzy_score,
                    )
                    continue

                if spend + COST_DETAILS_USD > MAX_SPEND_USD:
                    stopped_reason = "USD spend cap before Place Details"
                    LOG.warning("Stopped: spend cap before Place Details.")
                    print("Stopped: USD spend cap reached (before Place Details).")
                    break

                det = _place_details(http, pid, gkey)
                spend += COST_DETAILS_USD

                if det.get("_error"):
                    n_errors += 1
                    LOG.info(
                        "name=%r fuzzy_score=%s confidence=%s place_id=%s err=%s",
                        name,
                        fuzzy_score,
                        conf,
                        pid,
                        det["_error"],
                    )
                    continue

                now_iso = datetime.now(timezone.utc).isoformat()
                patch = _details_to_patch(
                    det,
                    place_id_res=pid,
                    confidence=conf,
                    fuzzy_score=fuzzy_score,
                    existing_address=row.get("address"),
                    today_s=today_s,
                    now_iso=now_iso,
                )

                sb.table("venues").update(patch).eq("id", vid).eq(
                    "data_source", CHAIN_DATA_SOURCE
                ).execute()
                n_enriched += 1
                if conf == "HIGH":
                    n_high += 1
                elif conf == "MEDIUM":
                    n_medium += 1
                else:
                    n_low += 1

                LOG.info(
                    "name=%r fuzzy_score=%s confidence=%s place_id=%s",
                    name,
                    fuzzy_score,
                    conf,
                    patch.get("place_id"),
                )

            except Exception as e:  # noqa: BLE001
                n_errors += 1
                LOG.exception("name=%r: %s", name, e)

            if attempted % 25 == 0:
                elapsed_m = (time.monotonic() - start) / 60.0
                print(
                    f"Progress {attempted}/{pending} | enriched={n_enriched} | "
                    f"HIGH={n_high} MEDIUM={n_medium} LOW={n_low} "
                    f"NO_MATCH={n_no_match} errors={n_errors} | "
                    f"spend~${spend:.2f} | {elapsed_m:.1f}m"
                )

    elapsed_m = (time.monotonic() - start) / 60.0
    print("\n--- Run summary ---")
    print(f"Baseline pending: {pending}")
    print(f"Venues attempted this run: {attempted}")
    print(
        f"Match bucket counts - HIGH: {n_high}, MEDIUM: {n_medium}, LOW: {n_low}, "
        f"NO_MATCH: {n_no_match}, API/update errors: {n_errors}"
    )
    print(f"Rows written (Place Details + Supabase update): {n_enriched}")
    print(f"Estimated API spend (planned rates): ${spend:.2f} USD")
    print(f"Elapsed: {elapsed_m:.2f} minutes")
    if stopped_reason:
        print(f"Early stop: {stopped_reason}")

    print_validation_summary()

    return 0 if n_errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
