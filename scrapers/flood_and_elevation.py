"""One-off enrichment: Google Elevation per venue + flood hazard (Geoscience Australia WFS / fallback API).

Run from repo root::

    python -m scrapers.flood_and_elevation

Requires: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY), GOOGLE_MAPS_API_KEY
(or GOOGLE_PLACES_API_KEY as fallback for the same Google key).

Supabase ``venues`` must have ``id``, ``name``, and coordinates ``latitude``/``longitude`` or ``lat``/``lng``.
Apply ``supabase/migrations/002_venues_elevation_flood.sql`` before running.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger(__name__)
ELEVATION_DELAY_S = 0.3
FLOOD_WFS_DELAY_S = 0.1
ELEVATION_URL = "https://maps.googleapis.com/maps/api/elevation/json"
WFS_BASE_URLS = (
    "https://services.ga.gov.au/gis/rest/services/NationalFloodHazard/MapServer/WFSServer",
    "https://services.ga.gov.au/gis/services/NationalFloodHazard/MapServer/WFSServer",
)
FLOOD_API_URL = "https://flood.ga.gov.au/api/1.0/floodzones"
GA_FLOOD_SOURCE = "Geoscience Australia National Flood Hazard"
FLOOD_API_SOURCE = "flood.ga.gov.au API (fallback)"
BBOX_DEG = 0.02

SEVERITY = {
    "Highly Frequently": 5,
    "Frequently": 4,
    "Occasionally": 3,
    "Rarely": 2,
    "Unknown": 1,
    "CHECK_MANUALLY": 0,
}


def _sleep_elevation() -> None:
    time.sleep(ELEVATION_DELAY_S)


def _sleep_flood_wfs() -> None:
    time.sleep(FLOOD_WFS_DELAY_S)


def _venue_coords(row: dict[str, Any]) -> tuple[float | None, float | None]:
    lat = row.get("latitude")
    if lat is None:
        lat = row.get("lat")
    lng = row.get("longitude")
    if lng is None:
        lng = row.get("lng")
    try:
        if lat is not None and lng is not None:
            return float(lat), float(lng)
    except (TypeError, ValueError):
        pass
    return None, None


def _parse_typenames_from_capabilities(xml_text: str) -> list[str]:
    names = re.findall(r"<(?:[^:>/]+:)?Name>([^<]+)</(?:[^:>/]+:)?Name>", xml_text)
    out: list[str] = []
    for n in names:
        n = n.strip()
        if not n or n.startswith("EPSG:") or n.startswith("http"):
            continue
        if ":" in n and n not in out:
            out.append(n)
    return out


def _infer_category_from_text(blob: str) -> str | None:
    t = blob.lower()
    if "check" in t and "manual" in t:
        return "CHECK_MANUALLY"
    if "highly" in t and "frequent" in t:
        return "Highly Frequently"
    if "high" in t and "frequent" in t:
        return "Highly Frequently"
    if "very high" in t or "extreme" in t or "catastrophic" in t:
        return "Highly Frequently"
    if "frequent" in t or "likely inundation" in t:
        return "Frequently"
    if "occasional" in t or "moderate" in t:
        return "Occasionally"
    if "rare" in t or "low likelihood" in t or "minor" in t:
        return "Rarely"
    return None


def _normalize_category(label: str | None, *, in_mapped_zone: bool) -> str:
    if not label or not str(label).strip():
        return "Unknown" if in_mapped_zone else "Rarely"
    raw = str(label).strip()
    inferred = _infer_category_from_text(raw)
    if inferred:
        return inferred
    for canon in ("Highly Frequently", "Frequently", "Occasionally", "Rarely", "Unknown"):
        if canon.lower() == raw.lower():
            return canon
    return "Unknown" if in_mapped_zone else "Rarely"


def _pick_worse(a: str, b: str) -> str:
    return a if SEVERITY.get(a, 0) >= SEVERITY.get(b, 0) else b


def _category_from_polygon_attributes(row: Any) -> str:
    """Build a blob from non-geometry attributes on a joined flood row."""
    if row is None or not hasattr(row, "items"):
        return "Unknown"
    parts: list[str] = []
    skip = {"geometry", "index_right"}
    for k, v in row.items():
        if k in skip:
            continue
        if v is None or (isinstance(v, float) and str(v) == "nan"):
            continue
        parts.append(str(v))
    blob = " ".join(parts).lower()
    return _infer_category_from_text(blob) or "Unknown"


def _discover_wfs_typename(client: httpx.Client, base: str) -> str | None:
    caps_url = f"{base}?service=WFS&request=GetCapabilities&version=2.0.0"
    try:
        r = client.get(caps_url, timeout=120.0)
        if r.status_code != 200:
            LOG.warning("WFS GetCapabilities HTTP %s for %s", r.status_code, base)
            return None
        names = _parse_typenames_from_capabilities(r.text)
        if not names:
            LOG.warning("No typenames parsed from WFS capabilities: %s", base)
            return None
        LOG.info("WFS %s typenames (first 5): %s", base, names[:5])
        return names[0]
    except Exception:  # noqa: BLE001
        LOG.warning("WFS GetCapabilities failed for %s: %s", base, traceback.format_exc())
        return None


def _wfs_bbox_geojson_gdf(
    client: httpx.Client,
    base: str,
    typename: str,
    lat: float,
    lng: float,
) -> Any | None:
    import geopandas as gpd

    minx, miny, maxx, maxy = lng - BBOX_DEG, lat - BBOX_DEG, lng + BBOX_DEG, lat + BBOX_DEG
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": typename,
        "outputFormat": "application/json",
        "count": "200",
        "bbox": f"{minx},{miny},{maxx},{maxy},EPSG:4326",
    }
    url = f"{base}?{urlencode(params)}"
    try:
        r = client.get(url, timeout=120.0)
        _sleep_flood_wfs()
        if r.status_code != 200:
            return None
        buf = io.BytesIO(r.content)
        try:
            return gpd.read_file(buf, driver="GeoJSON")
        except Exception:  # noqa: BLE001
            buf.seek(0)
            try:
                payload = json.loads(buf.getvalue().decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                return None
            if isinstance(payload, dict) and isinstance(payload.get("features"), list):
                return gpd.GeoDataFrame.from_features(payload["features"], crs="EPSG:4326")
            return None
    except Exception:  # noqa: BLE001
        LOG.debug("WFS GetFeature bbox failed: %s", traceback.format_exc())
        return None


def _flood_category_from_wfs_bboxes(
    client: httpx.Client,
    base: str,
    typename: str,
    lat: float,
    lng: float,
) -> tuple[str, str | None]:
    import geopandas as gpd
    from shapely.geometry import Point

    gdf = _wfs_bbox_geojson_gdf(client, base, typename, lat, lng)
    if gdf is None:
        return "Unknown", GA_FLOOD_SOURCE
    if gdf.empty:
        return "Rarely", GA_FLOOD_SOURCE

    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    pt = gpd.GeoDataFrame(geometry=[Point(lng, lat)], crs="EPSG:4326")
    joined = gpd.sjoin(pt, gdf, how="inner", predicate="within")
    if joined.empty:
        return "Rarely", GA_FLOOD_SOURCE

    worst = "Unknown"
    for _, rrow in joined.iterrows():
        cat = _category_from_polygon_attributes(rrow)
        worst = _pick_worse(worst, cat)
    return worst, GA_FLOOD_SOURCE


def _flood_category_from_api(lat: float, lng: float) -> tuple[str | None, bool]:
    """Returns (category or None, request_ok)."""
    try:
        import requests

        r = requests.get(
            FLOOD_API_URL,
            params={"lat": lat, "lng": lng},
            timeout=30,
        )
        ok = r.status_code == 200
        if not ok:
            return None, False
        data = r.json()
        blob = json.dumps(data) if not isinstance(data, str) else data
        cat = _infer_category_from_text(blob.lower())
        if cat:
            return cat, True
        if isinstance(data, dict):
            for key in ("category", "risk", "floodRisk", "classification", "hazard", "name", "title"):
                v = data.get(key)
                if isinstance(v, str) and v.strip():
                    return _normalize_category(v, in_mapped_zone=True), True
        return "Unknown", True
    except Exception:  # noqa: BLE001
        LOG.debug("Flood API error: %s", traceback.format_exc())
        return None, False


def _elevation_metres(client: httpx.Client, api_key: str, lat: float, lng: float) -> int | None:
    params = {"locations": f"{lat},{lng}", "key": api_key}
    try:
        r = client.get(ELEVATION_URL, params=params, timeout=60.0)
        _sleep_elevation()
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get("status") != "OK":
            LOG.warning("Elevation non-OK status: %s", data.get("status"))
            return None
        results = data.get("results")
        if not isinstance(results, list) or not results:
            return None
        el = results[0].get("elevation")
        if el is None:
            return None
        return int(round(float(el)))
    except Exception:  # noqa: BLE001
        LOG.warning("Elevation request failed: %s", traceback.format_exc())
        return None


def _fetch_venues(supabase: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table("venues")
            .select("id, name, lat, lng")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = getattr(resp, "data", None) or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def _filter_with_coords(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        lat, lng = _venue_coords(row)
        if lat is None or lng is None:
            continue
        out.append(row)
    return out


def main() -> None:
    log_path = _ROOT / "logs" / "flood_elevation.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    from data_builder.config import get_settings

    settings = get_settings()
    sb_url = (settings.supabase_url or "").strip()
    sb_key = (settings.supabase_service_role_key or settings.supabase_key or "").strip()
    maps_key = (
        os.getenv("GOOGLE_PLACES_API_KEY")
        or os.getenv("GOOGLE_MAPS_API_KEY")
        or (settings.google_maps_api_key or settings.google_places_api_key or "").strip()
    ).strip()

    if not sb_url or not sb_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")
    if not maps_key:
        raise RuntimeError("Set GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY for elevation.")

    from supabase import create_client

    supabase = create_client(sb_url, sb_key)

    raw = _fetch_venues(supabase)
    venues = _filter_with_coords(raw)

    wfs_base: str | None = None
    wfs_typename: str | None = None
    ga_endpoints_reachable = False

    with httpx.Client() as http:
        for base in WFS_BASE_URLS:
            tn = _discover_wfs_typename(http, base)
            if tn:
                probe = _wfs_bbox_geojson_gdf(http, base, tn, -33.8688, 151.2093)
                if probe is not None:
                    wfs_base = base
                    wfs_typename = tn
                    ga_endpoints_reachable = True
                    LOG.info("Using WFS base=%s typename=%s", base, tn)
                    break
                LOG.warning("WFS typename %s probe returned no readable GeoJSON from %s", tn, base)

        api_probe_ok = False
        if not ga_endpoints_reachable:
            _, api_probe_ok = _flood_category_from_api(-33.8688, 151.2093)
            if api_probe_ok:
                LOG.info("Geoscience WFS unavailable; using flood.ga.gov.au API fallback.")
            else:
                LOG.error(
                    "All Geoscience Australia flood endpoints tested unavailable "
                    "(WFS + flood.ga.gov.au). Setting flood_risk_category=CHECK_MANUALLY for all venues. "
                    "See %s",
                    log_path,
                )

        processed = elev_ok = flood_ok = errors = 0
        high_freq_names: list[str] = []

        for row in venues:
            processed += 1
            vid = row.get("id")
            name = str(row.get("name") or "").strip() or str(vid)
            lat, lng = _venue_coords(row)
            assert lat is not None and lng is not None

            elev = _elevation_metres(http, maps_key, lat, lng)
            if elev is not None:
                elev_ok += 1
            else:
                errors += 1

            flood_cat: str
            flood_src: str | None

            if wfs_base and wfs_typename:
                flood_cat, flood_src = _flood_category_from_wfs_bboxes(http, wfs_base, wfs_typename, lat, lng)
            elif api_probe_ok:
                api_cat, _ = _flood_category_from_api(lat, lng)
                flood_cat = api_cat or "Unknown"
                flood_src = FLOOD_API_SOURCE
            else:
                flood_cat = "CHECK_MANUALLY"
                flood_src = None

            if flood_cat in ("Frequently", "Highly Frequently"):
                high_freq_names.append(name)

            if flood_cat != "CHECK_MANUALLY":
                flood_ok += 1

            payload = {
                "elevation_metres": elev,
                "flood_risk_category": flood_cat,
                "flood_data_source": flood_src,
            }
            try:
                supabase.table("venues").update(payload).eq("id", str(vid)).execute()
            except Exception:  # noqa: BLE001
                LOG.exception("Supabase update failed venue_id=%s", vid)
                errors += 1

        print("")
        print("--- flood_and_elevation summary ---")
        print(f"Venues processed: {processed}")
        print(f"Elevation captured: {elev_ok}")
        print(f"Flood risk captured: {flood_ok}")
        shown = high_freq_names[:50]
        suffix = " …" if len(high_freq_names) > 50 else ""
        print(
            f"High/Frequent flood risk venues: {len(high_freq_names)} "
            f"({', '.join(shown) or 'none'}{suffix})"
        )
        print(f"Errors: {errors}")
        print("")


if __name__ == "__main__":
    main()
