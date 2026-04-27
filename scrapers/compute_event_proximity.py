"""Compute haversine distance bands between destinations and major events.

Inserts/upserts ``shared.ref_destination_event_proximity`` for pairs within 200 km.

Run: ``python -m scrapers.compute_event_proximity``

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY``).
"""

from __future__ import annotations

import logging
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _band(d_km: float) -> str | None:
    if d_km <= 50:
        return "within_50km"
    if d_km <= 100:
        return "within_100km"
    if d_km <= 200:
        return "within_200km"
    return None


def main() -> None:
    _load_env()
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) required.")

    from supabase import create_client

    sb = create_client(url, key)
    sh = sb.schema("shared")

    destinations: list[dict[str, Any]] = []
    off = 0
    while True:
        res = (
            sh.table("ref_destinations")
            .select("destination_id,lat,lng")
            .eq("is_active", True)
            .range(off, off + 999)
            .execute()
        )
        part = res.data or []
        destinations.extend(part)
        if len(part) < 1000:
            break
        off += 1000

    events: list[dict[str, Any]] = []
    off = 0
    while True:
        res = (
            sh.table("ref_major_events")
            .select("major_event_id,event_lat,event_lng")
            .eq("is_active", True)
            .range(off, off + 999)
            .execute()
        )
        part = res.data or []
        events.extend(part)
        if len(part) < 1000:
            break
        off += 1000

    rows: list[dict[str, Any]] = []
    for d in destinations:
        try:
            lat1 = float(d["lat"])
            lon1 = float(d["lng"])
        except (TypeError, ValueError, KeyError):
            continue
        for e in events:
            if e.get("event_lat") is None or e.get("event_lng") is None:
                continue
            try:
                lat2 = float(e["event_lat"])
                lon2 = float(e["event_lng"])
            except (TypeError, ValueError):
                continue
            dist = _haversine_km(lat1, lon1, lat2, lon2)
            band = _band(dist)
            if not band:
                continue
            rows.append(
                {
                    "destination_id": d["destination_id"],
                    "major_event_id": e["major_event_id"],
                    "distance_km": round(dist, 2),
                    "proximity_band": band,
                }
            )

    LOG.info("Upserting %s proximity rows", len(rows))
    batch = 500
    for i in range(0, len(rows), batch):
        sh.table("ref_destination_event_proximity").upsert(
            rows[i : i + batch],
            on_conflict="destination_id,major_event_id",
        ).execute()

    LOG.info("Done at %s", datetime.now(timezone.utc).isoformat())


if __name__ == "__main__":
    main()
