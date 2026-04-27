"""SILO top-up: last 14 days for every active grid cell -> daily + monthly refresh.

Run manually when scheduled (not activated on Railway in this repo session).

Run: ``python -m scrapers.silo_weather_topup``

Requires: ``SILO_API_EMAIL``, ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY``.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from scrapers.silo_weather_backfill import (  # noqa: E402
    CELL_PAUSE_S,
    UPSERT_BATCH,
    _brisbane_yesterday,
    _fetch_silo_csv,
    _grid_cell_id,
    _parse_csv_rows,
)


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _refresh_cell_totals(supabase: Any, gid: str) -> None:
    tbl = supabase.schema("shared").table("ref_weather_daily")
    r_min = tbl.select("observation_date").eq("grid_cell_id", gid).order("observation_date", desc=False).limit(1).execute()
    r_max = tbl.select("observation_date").eq("grid_cell_id", gid).order("observation_date", desc=True).limit(1).execute()
    r_cnt = tbl.select("weather_daily_id", count="exact", head=True).eq("grid_cell_id", gid).execute()
    dmin = (r_min.data or [{}])[0].get("observation_date")
    dmax = (r_max.data or [{}])[0].get("observation_date")
    cnt = getattr(r_cnt, "count", None)
    if not dmin or not dmax:
        return
    supabase.schema("shared").table("ref_weather_grid_cells").update(
        {
            "first_observed_date": dmin,
            "last_observed_date": dmax,
            "total_observations": int(cnt or 0),
        }
    ).eq("grid_cell_id", gid).execute()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("silo_weather_topup")
    _load_env()

    email = (os.getenv("SILO_API_EMAIL") or "").strip()
    if not email:
        raise RuntimeError("SILO_API_EMAIL is required.")

    from data_builder.config import get_settings

    settings = get_settings()
    sb_url = (settings.supabase_url or "").strip()
    sb_key = (settings.supabase_service_role_key or settings.supabase_key or "").strip()
    if not sb_url or not sb_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")

    from supabase import create_client

    supabase = create_client(sb_url, sb_key)
    finish = _brisbane_yesterday()
    start = finish - timedelta(days=13)

    resp = (
        supabase.schema("shared")
        .table("ref_weather_grid_cells")
        .select("grid_cell_id,silo_lat,silo_lng,requested_lat,requested_lng,coverage_label")
        .eq("is_active", True)
        .execute()
    )
    cells = getattr(resp, "data", None) or []
    if not cells:
        log.warning("no active ref_weather_grid_cells rows")
        return

    tbl_daily = supabase.schema("shared").table("ref_weather_daily")

    with httpx.Client(headers={"User-Agent": settings.scraper_user_agent}) as http:
        for row in cells:
            gid = str(row["grid_cell_id"])
            silo_lat = float(row["silo_lat"])
            silo_lng = float(row["silo_lng"])
            try:
                csv_text = _fetch_silo_csv(http, email, silo_lat, silo_lng, start, finish)
                r_silo_lat, r_silo_lng, daily = _parse_csv_rows(csv_text)
                if _grid_cell_id(r_silo_lat, r_silo_lng) != gid:
                    log.warning(
                        "SILO centroid drift for %s: db=%s api=%s — using API id path",
                        gid,
                        gid,
                        _grid_cell_id(r_silo_lat, r_silo_lng),
                    )
                batch: list[dict[str, Any]] = []
                for r in daily:
                    batch.append(
                        {
                            "grid_cell_id": gid,
                            "observation_date": r["observation_date"].isoformat(),
                            "daily_rain_mm": r["daily_rain_mm"],
                            "temp_max_c": r["temp_max_c"],
                            "temp_min_c": r["temp_min_c"],
                            "humidity_pct": r["humidity_pct"],
                            "data_source": "silo",
                        }
                    )
                    if len(batch) >= UPSERT_BATCH:
                        tbl_daily.upsert(
                            batch,
                            on_conflict="grid_cell_id,observation_date",
                            ignore_duplicates=True,
                        ).execute()
                        batch.clear()
                if batch:
                    tbl_daily.upsert(
                        batch,
                        on_conflict="grid_cell_id,observation_date",
                        ignore_duplicates=True,
                    ).execute()
                _refresh_cell_totals(supabase, gid)
                log.info("topup ok grid_cell_id=%s days=%s", gid, len(daily))
            except Exception as e:  # noqa: BLE001
                log.exception("topup failed grid_cell_id=%s: %s", gid, e)
            time.sleep(CELL_PAUSE_S)

    from scrapers.silo_weather_monthly_refresh import main as monthly_main

    monthly_main()


if __name__ == "__main__":
    main()
