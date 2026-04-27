"""Aggregate shared.ref_weather_daily -> shared.ref_weather_monthly_stats (12 months per cell).

Run: ``python -m scrapers.silo_weather_monthly_refresh``

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY``).
"""

from __future__ import annotations

import logging
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _risk_rating(rain_days_avg: float) -> str:
    if rain_days_avg < 6:
        return "low"
    if rain_days_avg < 10:
        return "medium"
    if rain_days_avg < 14:
        return "high"
    return "very_high"


def _build_monthly_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    """df columns: grid_cell_id, observation_date, daily_rain_mm, temp_max_c, temp_min_c, humidity_pct."""
    df = df.copy()
    dt = pd.to_datetime(df["observation_date"])
    df["year"] = dt.dt.year
    df["month_of_year"] = dt.dt.month.astype(int)

    out: list[dict[str, Any]] = []
    for (gid, m), g in df.groupby(["grid_cell_id", "month_of_year"], sort=True):
        yearly_rain = g.groupby("year", sort=True)["daily_rain_mm"].sum(min_count=1)
        wet = (g["daily_rain_mm"].fillna(0) > 1.0).astype(int)
        g2 = g.assign(_wet=wet)
        yearly_rain_days = g2.groupby("year", sort=True)["_wet"].sum()
        years_in_sample = int(yearly_rain.index.nunique())
        if years_in_sample <= 0:
            continue
        avg_rain = float(yearly_rain.mean())
        med_rain = float(statistics.median(yearly_rain.tolist()))
        rain_days_avg = float(yearly_rain_days.mean())

        avg_tmax = float(g["temp_max_c"].mean()) if g["temp_max_c"].notna().any() else None
        avg_tmin = float(g["temp_min_c"].mean()) if g["temp_min_c"].notna().any() else None
        avg_rh = float(g["humidity_pct"].mean()) if g["humidity_pct"].notna().any() else None

        out.append(
            {
                "grid_cell_id": gid,
                "month_of_year": int(m),
                "years_in_sample": years_in_sample,
                "avg_rainfall_mm": round(avg_rain, 2) if avg_rain == avg_rain else None,
                "median_rainfall_mm": round(med_rain, 2) if med_rain == med_rain else None,
                "rain_days_avg": round(rain_days_avg, 1),
                "avg_temp_max_c": round(avg_tmax, 1) if avg_tmax is not None else None,
                "avg_temp_min_c": round(avg_tmin, 1) if avg_tmin is not None else None,
                "avg_humidity_pct": round(avg_rh, 2) if avg_rh is not None else None,
                "risk_rating": _risk_rating(rain_days_avg),
                "last_refreshed_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out


def _fetch_all_daily(supabase: Any) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    page = 1000
    offset = 0
    while True:
        resp = (
            supabase.schema("shared")
            .table("ref_weather_daily")
            .select("grid_cell_id,observation_date,daily_rain_mm,temp_max_c,temp_min_c,humidity_pct")
            # Stable ordering is required for range() pagination; without it, PostgREST can
            # repeat or omit rows across pages and corrupt climatology aggregates.
            .order("weather_daily_id", desc=False)
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = getattr(resp, "data", None) or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    if not rows:
        return pd.DataFrame(
            columns=[
                "grid_cell_id",
                "observation_date",
                "daily_rain_mm",
                "temp_max_c",
                "temp_min_c",
                "humidity_pct",
            ]
        )
    return pd.DataFrame(rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("silo_weather_monthly_refresh")
    _load_env()

    from data_builder.config import get_settings

    settings = get_settings()
    sb_url = (settings.supabase_url or "").strip()
    sb_key = (settings.supabase_service_role_key or settings.supabase_key or "").strip()
    if not sb_url or not sb_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")

    from supabase import create_client

    supabase = create_client(sb_url, sb_key)
    t0 = time.perf_counter()
    df = _fetch_all_daily(supabase)
    if df.empty:
        log.warning("ref_weather_daily is empty; nothing to aggregate")
        return
    for col in ("daily_rain_mm", "temp_max_c", "temp_min_c", "humidity_pct"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    monthly = _build_monthly_rows(df)
    tbl = supabase.schema("shared").table("ref_weather_monthly_stats")
    batch_size = 500
    for i in range(0, len(monthly), batch_size):
        chunk = monthly[i : i + batch_size]
        tbl.upsert(chunk, on_conflict="grid_cell_id,month_of_year").execute()

    log.info("monthly refresh: %s rows upserted in %.1fs", len(monthly), time.perf_counter() - t0)


if __name__ == "__main__":
    main()
