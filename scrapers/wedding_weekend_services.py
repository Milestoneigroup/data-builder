"""Wedding weekend supplier & services layer (Section C).

Selects 40 regional wedding destinations, enriches with Claude (optional web search),
Google Places Text Search, nearest commercial airport from a reference CSV, and
Distance Matrix or haversine drive-time estimate. Upserts shared.ref_destination_services
and patches shared.ref_destinations.nearest_airport_*.

Run:
  python scrapers/wedding_weekend_services.py

Requires: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, ANTHROPIC_API_KEY, DATABASE_URL (for migration),
GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY (Places + optional Distance Matrix).

Logs: logs/destination_services.log
"""

from __future__ import annotations

import csv
import json
import logging
import math
import os
import re
import sys
import time
from datetime import date
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

LOG_PATH = _ROOT / "logs" / "destination_services.log"
AIRPORTS_CSV = _ROOT / "data" / "australian_airports_reference.csv"
MIGRATION_009 = _ROOT / "supabase" / "migrations" / "009_destination_services.sql"
RUN_COUNT = 40
PROGRESS_EVERY = 5
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TEXT_SEARCH_FIELD_MASK = (
    "places.id,places.displayName,places.websiteUri,places.nationalPhoneNumber,places.rating,places.userRatingCount"
)
REQUEST_DELAY_S = 0.35

PRIORITY_PHRASES: tuple[str, ...] = (
    "hunter valley",
    "yarra valley",
    "barossa valley",
    "margaret river",
    "byron bay",
    "noosa",
    "daylesford",
    "mornington peninsula",
    "blue mountains",
    "southern highlands",
    "kangaroo island",
    "stradbroke",
    "whitsunday",
    "port douglas",
    "grampian",
    "clare valley",
    "mclaren vale",
    "swan valley",
    "pemberton",
    "dunsborough",
    "halls gap",
    "bright",
    "mount hotham",
    "mudgee",
    "orange",
    "bowral",
    "berry",
    "jervis bay",
    "narooma",
    "merimbula",
    "launceston",
    "freycinet",
    "bruny island",
    "queenstown",
    "airlie beach",
    "mission beach",
    "tropical north",
    "hinterland",
    "sunshine coast",
    "gold coast hinterland",
    "adelaide hills",
    "rutherglen",
    "healesville",
    "maleny",
    "montville",
    "port macquarie",
    "coffs harbour",
    "forster",
    "eden",
    "apollo bay",
    "great ocean road",
    "phillip island",
    "wilsons promontory",
    "cradle mountain",
)

METRO_CORE = ("sydney", "melbourne", "brisbane", "perth", "adelaide", "darwin", "canberra", "hobart")


def _setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("destination_services")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    return log


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _apply_migration() -> None:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / "env.local", override=True)
    db = os.getenv("DATABASE_URL", "").strip()
    if not db:
        raise SystemExit("DATABASE_URL required to apply migration 009.")
    import psycopg

    sql = MIGRATION_009.read_text(encoding="utf-8")
    with psycopg.connect(db, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def _norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _estimate_drive_mins_from_km(km: float) -> int:
    if km <= 0:
        return 0
    return max(5, int(round(km / 75.0 * 60)))


def _distance_matrix_mins(
    client: httpx.Client, api_key: str, origin: tuple[float, float], dest: tuple[float, float]
) -> int | None:
    o = f"{origin[0]},{origin[1]}"
    d = f"{dest[0]},{dest[1]}"
    url = (
        "https://maps.googleapis.com/maps/api/distancematrix/json"
        f"?units=metric&origins={o}&destinations={d}&mode=driving&key={quote(api_key, safe='')}"
    )
    try:
        r = client.get(url, timeout=45.0)
        r.raise_for_status()
        data = r.json()
        row = (data.get("rows") or [{}])[0]
        el = (row.get("elements") or [{}])[0]
        if el.get("status") != "OK":
            return None
        sec = el.get("duration", {}).get("value")
        if sec is None:
            return None
        return max(1, int(round(int(sec) / 60)))
    except Exception:  # noqa: BLE001
        return None


def _ensure_airports_csv(log: logging.Logger) -> None:
    AIRPORTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    if AIRPORTS_CSV.exists() and AIRPORTS_CSV.stat().st_size > 50:
        return
    from anthropic import Anthropic

    ak = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not ak:
        raise SystemExit("ANTHROPIC_API_KEY required to generate airports CSV.")
    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5-20250929").strip()
    prompt = """List exactly 28 major commercial passenger airports in Australia with scheduled services.
Return JSON array only, each object: {"iata":"XXX","airport_name":"...","city":"...","state":"NSW|VIC|QLD|WA|SA|TAS|NT|ACT","lat":-33.9,"lng":151.2}
Use real IATA codes. No markdown."""
    ac = Anthropic(api_key=ak)
    msg = ac.messages.create(model=model, max_tokens=4000, messages=[{"role": "user", "content": prompt}])
    text = "".join(
        b.text for b in (msg.content or []) if hasattr(b, "text")
    ).strip()
    m = re.search(r"\[[\s\S]*\]", text)
    if not m:
        raise SystemExit("Could not parse airports JSON from Claude.")
    rows = json.loads(m.group(0))
    with AIRPORTS_CSV.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=["iata", "airport_name", "city", "state", "lat", "lng"],
        )
        w.writeheader()
        for row in rows:
            w.writerow(
                {
                    "iata": _norm(row.get("iata")).upper()[:3],
                    "airport_name": _norm(row.get("airport_name")),
                    "city": _norm(row.get("city")),
                    "state": _norm(row.get("state")),
                    "lat": row.get("lat"),
                    "lng": row.get("lng"),
                }
            )
    log.info("Wrote %s (%s rows)", AIRPORTS_CSV, len(rows))


def _load_airports() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with AIRPORTS_CSV.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            try:
                la = float(row["lat"])
                lo = float(row["lng"])
            except (KeyError, TypeError, ValueError):
                continue
            out.append(
                {
                    "iata": row["iata"].strip().upper(),
                    "airport_name": row.get("airport_name", "").strip(),
                    "lat": la,
                    "lng": lo,
                }
            )
    return out


def _nearest_airport(dest_lat: float, dest_lng: float, airports: list[dict[str, Any]]) -> tuple[str, str, int]:
    best = ("", "", 10**9)
    for ap in airports:
        km = haversine_km(dest_lat, dest_lng, ap["lat"], ap["lng"])
        if km < best[2]:
            best = (ap["iata"], ap["airport_name"], int(round(km)))
    return best[0], best[1], best[2]


def _places_search(client: httpx.Client, query: str, api_key: str) -> dict[str, Any]:
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
    time.sleep(REQUEST_DELAY_S)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        return {}
    data = r.json()
    places = data.get("places") or []
    if not places:
        return {}
    top = places[0]
    pid = str(top.get("id") or "").replace("places/", "")
    dn = top.get("displayName") or {}
    name = dn.get("text") if isinstance(dn, dict) else str(dn or "")
    return {
        "name": name,
        "website": str(top.get("websiteUri") or ""),
        "phone": str(top.get("nationalPhoneNumber") or ""),
        "rating": top.get("rating"),
        "place_id": pid,
    }


def _metro_penalty(slug: str, name: str) -> int:
    s, n = slug.lower(), name.lower()
    pen = 0
    for core in METRO_CORE:
        if core in s or core in n:
            if "greater" in s or "greater" in n:
                pen -= 2
                continue
            if any(t in s for t in ("cbd", "inner-city", "inner_city", "city-centre", "city_centre")):
                pen -= 200
            elif s.split("-")[0] == core and len(s) < 28:
                pen -= 120
            elif n in (core.title(), f"{core.title()} cbd"):
                pen -= 200
    return pen


def _destination_score(row: dict[str, Any]) -> float:
    slug = _norm(row.get("destination_slug")).lower()
    name = _norm(row.get("destination_name")).lower()
    blob = f"{slug} {name}"
    score = 0.0
    try:
        hl = int(row.get("hierarchy_level") or 0)
    except (TypeError, ValueError):
        hl = 0
    if hl <= 1:
        score -= 500
    if row.get("is_destination_wedding_location") is True:
        score += 15
    for p in PRIORITY_PHRASES:
        if p in blob:
            score += 40
    for k in ("wine", "valley", "coast", "island", "hinterland", "mountain", "historic", "peninsula", "ranges"):
        if k in blob:
            score += 6
    score += _metro_penalty(slug, name)
    return score


def _pick_destinations(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    active = [r for r in rows if r.get("is_active") is True]
    scored = sorted(active, key=_destination_score, reverse=True)
    out: list[dict[str, Any]] = []
    for r in scored:
        if len(out) >= n:
            break
        if _destination_score(r) < -400:
            continue
        out.append(r)
    return out[:n]


def _celebrant_crossref(sb: Any, state: str, dest_name: str, log: logging.Logger) -> str:
    try:
        res = (
            sb.table("celebrants")
            .select("celebrant_id,full_name,suburb")
            .eq("state", state)
            .limit(300)
            .execute()
        )
        cand = res.data or []
    except Exception as e:  # noqa: BLE001
        log.debug("Celebrant lookup skipped: %s", e)
        return ""
    if not cand:
        return ""
    names = [str(c.get("full_name") or "") for c in cand]
    hit = fuzz.extractOne(dest_name, names, scorer=fuzz.token_sort_ratio)
    if not hit:
        return ""
    name, score, _ = hit
    if score < 55:
        return ""
    for c in cand:
        if str(c.get("full_name")) == name:
            return f"{c.get('celebrant_id')}|{name}"
    return ""


def _claude_services_bundle(
    *,
    dest: dict[str, Any],
    nearest_iata: str,
    nearest_name: str,
    nearest_km: int,
    places_hints: dict[str, Any],
    api_key: str,
    model: str,
    log: logging.Logger,
) -> dict[str, Any] | None:
    from anthropic import Anthropic

    ctx = {
        "destination_name": _norm(dest.get("destination_name")),
        "state_code": _norm(dest.get("state_code")),
        "postcode": _norm(dest.get("postcode")),
        "lat": dest.get("lat"),
        "lng": dest.get("lng"),
        "nearest_airport_iata": nearest_iata,
        "nearest_airport_name": nearest_name,
        "nearest_airport_distance_km": nearest_km,
        "places_top_results": places_hints,
    }
    prompt = f"""You are researching practical services for wedding guests in Australia.
Use web search when needed. Destination context (JSON):\n{json.dumps(ctx, ensure_ascii=False)}

Return JSON only (no markdown) with keys matching this template (use empty string or false or null where unknown):
{{
  "nearest_airport_name": "",
  "nearest_airport_iata": "",
  "nearest_airport_distance_km": null,
  "nearest_airport_drive_mins": null,
  "airport_transfer_name": "", "airport_transfer_url": "", "airport_transfer_phone": "",
  "local_taxi_name": "", "local_taxi_phone": "",
  "rideshare_available": false,
  "bus_coach_services": "",
  "train_station_name": "", "train_station_distance_km": null,
  "accommodation_budget_name": "", "accommodation_budget_url": "",
  "accommodation_mid_name": "", "accommodation_mid_url": "",
  "accommodation_luxury_name": "", "accommodation_luxury_url": "",
  "accommodation_capacity_note": "",
  "primary_booking_platform": "",
  "babysitter_service_name": "", "babysitter_service_url": "",
  "child_activity_1": "", "child_activity_2": "",
  "nearest_hospital_name": "", "nearest_hospital_distance_km": null,
  "nearest_pharmacy_name": "", "nearest_pharmacy_address": "",
  "florist_name": "", "florist_url": "", "florist_instagram": "", "florist_google_rating": null,
  "hairmakeup_name": "", "hairmakeup_url": "", "hairmakeup_instagram": "",
  "dj_band_name": "", "dj_band_url": "",
  "photobooth_name": "", "photobooth_url": "",
  "cake_maker_name": "", "cake_maker_url": "",
  "caterer_name": "", "caterer_url": "",
  "marquee_hire_name": "", "marquee_hire_url": "",
  "rehearsal_dinner_venue": "", "rehearsal_dinner_url": "",
  "morning_after_cafe": "", "morning_after_cafe_url": "",
  "hens_bucks_bar": "", "hens_bucks_bar_url": "",
  "local_food_speciality": "",
  "rainy_day_activity_1_name": "", "rainy_day_activity_1_type": "", "rainy_day_activity_1_url": "",
  "rainy_day_activity_2_name": "", "rainy_day_activity_2_type": "", "rainy_day_activity_2_url": "",
  "rainy_day_venue_hire_name": "", "rainy_day_venue_hire_url": "",
  "data_confidence": "medium",
  "notes": ""
}}
Fill with concise factual items for this region. Prefer Australian businesses."""
    tools = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 8}]
    ac = Anthropic(api_key=api_key)
    try:
        msg = ac.messages.create(
            model=model,
            max_tokens=6000,
            messages=[{"role": "user", "content": prompt}],
            tools=tools,
        )
    except Exception as e1:  # noqa: BLE001
        log.warning("Claude web_search tool failed (%s); retrying without tools.", e1)
        msg = ac.messages.create(model=model, max_tokens=6000, messages=[{"role": "user", "content": prompt}])
    parts: list[str] = []
    for b in msg.content or []:
        if hasattr(b, "text"):
            parts.append(b.text)
        elif isinstance(b, dict) and b.get("type") == "text":
            parts.append(str(b.get("text", "")))
    text = "".join(parts).strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fence:
        text = fence.group(1).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        log.error("Claude JSON parse failed for %s", ctx["destination_name"])
        return None


def _row_for_supabase(
    dest: dict[str, Any],
    data: dict[str, Any],
    *,
    nearest_iata: str,
    nearest_name: str,
    nearest_km: int,
    drive_mins: int,
    celebrant_x: str,
    photographer_x: str,
    florist_rating: Any,
) -> dict[str, Any]:
    today = date.today().isoformat()

    def g(key: str, default: str = "") -> str:
        v = data.get(key)
        if v is None or v is False:
            return default
        return _norm(v)

    def gi(key: str) -> int | None:
        v = data.get(key)
        if v is None or v == "":
            return None
        try:
            return int(round(float(v)))
        except (TypeError, ValueError):
            return None

    def gb(key: str) -> bool:
        v = data.get(key)
        if isinstance(v, bool):
            return v
        s = str(v).lower().strip()
        return s in ("true", "yes", "y", "1")

    return {
        "destination_id": _norm(dest.get("destination_id")),
        "state_code": _norm(dest.get("state_code")),
        "nearest_airport_name": g("nearest_airport_name") or nearest_name,
        "nearest_airport_iata": g("nearest_airport_iata") or nearest_iata,
        "nearest_airport_distance_km": gi("nearest_airport_distance_km") or nearest_km,
        "nearest_airport_drive_mins": gi("nearest_airport_drive_mins") or drive_mins,
        "airport_transfer_name": g("airport_transfer_name"),
        "airport_transfer_url": g("airport_transfer_url"),
        "airport_transfer_phone": g("airport_transfer_phone"),
        "local_taxi_name": g("local_taxi_name"),
        "local_taxi_phone": g("local_taxi_phone"),
        "rideshare_available": gb("rideshare_available"),
        "bus_coach_services": g("bus_coach_services"),
        "train_station_name": g("train_station_name"),
        "train_station_distance_km": gi("train_station_distance_km"),
        "accommodation_budget_name": g("accommodation_budget_name"),
        "accommodation_budget_url": g("accommodation_budget_url"),
        "accommodation_mid_name": g("accommodation_mid_name"),
        "accommodation_mid_url": g("accommodation_mid_url"),
        "accommodation_luxury_name": g("accommodation_luxury_name"),
        "accommodation_luxury_url": g("accommodation_luxury_url"),
        "accommodation_capacity_note": g("accommodation_capacity_note"),
        "primary_booking_platform": g("primary_booking_platform"),
        "babysitter_service_name": g("babysitter_service_name"),
        "babysitter_service_url": g("babysitter_service_url"),
        "child_activity_1": g("child_activity_1"),
        "child_activity_2": g("child_activity_2"),
        "nearest_hospital_name": g("nearest_hospital_name"),
        "nearest_hospital_distance_km": gi("nearest_hospital_distance_km"),
        "nearest_pharmacy_name": g("nearest_pharmacy_name"),
        "nearest_pharmacy_address": g("nearest_pharmacy_address"),
        "florist_name": g("florist_name"),
        "florist_url": g("florist_url"),
        "florist_instagram": g("florist_instagram"),
        "florist_google_rating": florist_rating,
        "hairmakeup_name": g("hairmakeup_name"),
        "hairmakeup_url": g("hairmakeup_url"),
        "hairmakeup_instagram": g("hairmakeup_instagram"),
        "dj_band_name": g("dj_band_name"),
        "dj_band_url": g("dj_band_url"),
        "photobooth_name": g("photobooth_name"),
        "photobooth_url": g("photobooth_url"),
        "cake_maker_name": g("cake_maker_name"),
        "cake_maker_url": g("cake_maker_url"),
        "caterer_name": g("caterer_name"),
        "caterer_url": g("caterer_url"),
        "marquee_hire_name": g("marquee_hire_name"),
        "marquee_hire_url": g("marquee_hire_url"),
        "celebrant_crossref": celebrant_x,
        "photographer_crossref": photographer_x,
        "rehearsal_dinner_venue": g("rehearsal_dinner_venue"),
        "rehearsal_dinner_url": g("rehearsal_dinner_url"),
        "morning_after_cafe": g("morning_after_cafe"),
        "morning_after_cafe_url": g("morning_after_cafe_url"),
        "hens_bucks_bar": g("hens_bucks_bar"),
        "hens_bucks_bar_url": g("hens_bucks_bar_url"),
        "local_food_speciality": g("local_food_speciality"),
        "rainy_day_activity_1_name": g("rainy_day_activity_1_name"),
        "rainy_day_activity_1_type": g("rainy_day_activity_1_type"),
        "rainy_day_activity_1_url": g("rainy_day_activity_1_url"),
        "rainy_day_activity_2_name": g("rainy_day_activity_2_name"),
        "rainy_day_activity_2_type": g("rainy_day_activity_2_type"),
        "rainy_day_activity_2_url": g("rainy_day_activity_2_url"),
        "rainy_day_venue_hire_name": g("rainy_day_venue_hire_name"),
        "rainy_day_venue_hire_url": g("rainy_day_venue_hire_url"),
        "data_confidence": g("data_confidence") or "medium",
        "scraped_date": today,
        "notes": g("notes"),
        "updated_at": today,
    }


def main() -> None:
    _load_env()
    log = _setup_logging()
    logging.getLogger("httpx").setLevel(logging.WARNING)

    from data_builder.config import get_settings
    from supabase import create_client

    if MIGRATION_009.is_file():
        log.info("Applying migration %s", MIGRATION_009.name)
        _apply_migration()
        log.info("Migration 009 applied.")

    _ensure_airports_csv(log)
    airports = _load_airports()
    log.info("Loaded %s airports from CSV", len(airports))

    settings = get_settings()
    sb_url = (settings.supabase_url or "").strip()
    sb_key = (settings.supabase_service_role_key or "").strip()
    anth = (settings.anthropic_api_key or "").strip()
    gkey = (
        os.getenv("GOOGLE_PLACES_API_KEY")
        or os.getenv("GOOGLE_MAPS_API_KEY")
        or settings.google_places_api_key
        or settings.google_maps_api_key
        or ""
    ).strip()
    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5-20250929").strip()

    if not sb_url or not sb_key or not anth:
        raise SystemExit("SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, ANTHROPIC_API_KEY required.")
    if not gkey:
        raise SystemExit("GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY required for Places search.")

    sb = create_client(sb_url, sb_key)
    dest_tbl = sb.schema("shared").table("ref_destinations")
    svc_tbl = sb.schema("shared").table("ref_destination_services")

    all_rows: list[dict[str, Any]] = []
    offset = 0
    page = 500
    while True:
        chunk = dest_tbl.select(
            "destination_id,destination_slug,destination_name,hierarchy_level,parent_destination_id,"
            "state_code,suburb_or_area,postcode,lat,lng,is_destination_wedding_location,is_active"
        ).range(offset, offset + page - 1).execute().data or []
        if not chunk:
            break
        all_rows.extend(chunk)
        if len(chunk) < page:
            break
        offset += page

    picked = _pick_destinations(all_rows, RUN_COUNT)
    log.info("Selected %s destinations for run (requested %s).", len(picked), RUN_COUNT)

    complete = 0
    failed = 0
    with httpx.Client() as http:
        for i, dest in enumerate(picked, start=1):
            did = _norm(dest.get("destination_id"))
            dname = _norm(dest.get("destination_name"))
            st = _norm(dest.get("state_code"))
            try:
                dlat = float(dest.get("lat"))
                dlng = float(dest.get("lng"))
            except (TypeError, ValueError):
                log.error("Bad lat/lng for %s", did)
                failed += 1
                continue

            try:
                iata, aname, km = _nearest_airport(dlat, dlng, airports)
                ap_row = next((a for a in airports if a["iata"] == iata), None)
                drive_mins: int | None = None
                if ap_row:
                    drive_mins = _distance_matrix_mins(http, gkey, (dlat, dlng), (ap_row["lat"], ap_row["lng"]))
                if drive_mins is None:
                    drive_mins = _estimate_drive_mins_from_km(float(km))

                qbase = f"{dname} {st} Australia"
                flor = _places_search(http, f"florist {qbase}", gkey)
                time.sleep(REQUEST_DELAY_S)
                hm = _places_search(http, f"bridal hair makeup {qbase}", gkey)
                time.sleep(REQUEST_DELAY_S)
                xfer = _places_search(http, f"airport shuttle transfer {qbase}", gkey)
                places_hints = {"florist": flor, "hair_makeup": hm, "airport_transfer": xfer}

                data = _claude_services_bundle(
                    dest=dest,
                    nearest_iata=iata,
                    nearest_name=aname,
                    nearest_km=km,
                    places_hints=places_hints,
                    api_key=anth,
                    model=model,
                    log=log,
                )
                time.sleep(REQUEST_DELAY_S)
                if not data:
                    failed += 1
                else:
                    celeb = _celebrant_crossref(sb, st, dname, log)
                    photo_x = ""
                    fr = (
                        flor.get("rating")
                        if isinstance(flor.get("rating"), (int, float))
                        else data.get("florist_google_rating")
                    )
                    row = _row_for_supabase(
                        dest,
                        data,
                        nearest_iata=iata,
                        nearest_name=aname,
                        nearest_km=km,
                        drive_mins=drive_mins,
                        celebrant_x=celeb,
                        photographer_x=photo_x,
                        florist_rating=fr,
                    )
                    if not row.get("florist_name") and flor.get("name"):
                        row["florist_name"] = str(flor.get("name"))
                        row["florist_url"] = str(flor.get("website") or "")
                        row["florist_google_rating"] = flor.get("rating")
                    if not row.get("hairmakeup_name") and hm.get("name"):
                        row["hairmakeup_name"] = str(hm.get("name"))
                        row["hairmakeup_url"] = str(hm.get("website") or "")
                    if not row.get("airport_transfer_name") and xfer.get("name"):
                        row["airport_transfer_name"] = str(xfer.get("name"))
                        row["airport_transfer_url"] = str(xfer.get("website") or "")
                        row["airport_transfer_phone"] = str(xfer.get("phone") or "")

                    svc_tbl.upsert(row, on_conflict="destination_id").execute()
                    dest_tbl.update(
                        {"nearest_airport_iata": iata, "nearest_airport_drive_mins": drive_mins}
                    ).eq("destination_id", did).execute()
                    complete += 1
            except Exception as e:  # noqa: BLE001
                log.exception("Destination failed %s: %s", did, e)
                failed += 1

            if i % PROGRESS_EVERY == 0 or i == len(picked):
                print(f"Progress: {i}/{len(picked)} | Complete: {complete} | Failed: {failed}")
            log.info(
                "Progress: %s/%s | Complete: %s | Failed: %s | last=%s",
                i,
                len(picked),
                complete,
                failed,
                did,
            )

    print(f"Done. Complete: {complete} | Failed: {failed} | Log: {LOG_PATH}")


if __name__ == "__main__":
    main()
