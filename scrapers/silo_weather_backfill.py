"""SILO DataDrill backfill: seed cells -> shared.ref_weather_grid_cells + ref_weather_daily.

Run: ``python -m scrapers.silo_weather_backfill``

Requires: ``SILO_API_EMAIL``, ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY``).

SILO CSV ``comment`` codes (verified against live ``DataDrillDataset.php`` response, 2026-04-27):
``RXNH`` → ``daily_rain``, ``max_temp``, ``min_temp``, ``rh_tmax``. (``RXNT`` returns ``et_tall_crop``, not humidity.)

Fair use: 2s pause between cells; retries on 5xx/timeout with 2s/4s/8s backoff.

If a seed row already exists in ``shared.ref_weather_grid_cells`` with the same
``coverage_label`` and ``requested_lat`` / ``requested_lng`` (6 dp) and
``total_observations`` meets the same 95% day threshold as a fresh run, that row
is **skipped** (no SILO request, no sleep) so reruns only touch new or moved anchors.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

SILO_URL = "https://www.longpaddock.qld.gov.au/cgi-bin/silo/DataDrillDataset.php"
# R=daily_rain, X=max_temp, N=min_temp, H=rh_tmax (SILO CSV probe; not RXNT).
SILO_COMMENT = "RXNH"
START_DATE = date(2000, 1, 1)
BRISBANE_TZ = ZoneInfo("Australia/Brisbane")
REQUEST_TIMEOUT_S = 120.0
MAX_ATTEMPTS = 3
BACKOFF_S = (2, 4, 8)
CELL_PAUSE_S = 2.0
UPSERT_BATCH = 800
MIN_COVERAGE_RATIO = 0.95
SEED_PATH = _ROOT / "data" / "seed_weather_test_cells.json"


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _brisbane_yesterday() -> date:
    now_bne = datetime.now(BRISBANE_TZ).date()
    return now_bne - timedelta(days=1)


def _grid_cell_id(silo_lat: float, silo_lng: float) -> str:
    return f"WGC-{silo_lat:.4f}_{silo_lng:.4f}"


def _parse_float(raw: str) -> float | None:
    s = (raw or "").strip()
    if not s or s in (".", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_silo_csv(
    client: httpx.Client,
    email: str,
    lat: float,
    lon: float,
    start: date,
    finish: date,
) -> str:
    params = {
        "lat": f"{lat:.6f}",
        "lon": f"{lon:.6f}",
        "start": start.strftime("%Y%m%d"),
        "finish": finish.strftime("%Y%m%d"),
        "format": "csv",
        "comment": SILO_COMMENT,
        "username": email,
    }
    url = f"{SILO_URL}?{urlencode(params)}"
    last_err: str | None = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = client.get(url, timeout=REQUEST_TIMEOUT_S)
        except (httpx.TimeoutException, httpx.TransportError) as e:
            last_err = str(e)
            if attempt + 1 < MAX_ATTEMPTS:
                time.sleep(BACKOFF_S[attempt])
            continue
        if r.status_code >= 500:
            last_err = f"HTTP {r.status_code}"
            if attempt + 1 < MAX_ATTEMPTS:
                time.sleep(BACKOFF_S[attempt])
            continue
        r.raise_for_status()
        return r.text
    raise RuntimeError(f"SILO request failed after {MAX_ATTEMPTS} attempts: {last_err}")


def _parse_csv_rows(
    text: str,
) -> tuple[float, float, list[dict[str, Any]]]:
    """Returns (silo_lat, silo_lng, daily_rows)."""
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    if not reader.fieldnames:
        raise ValueError("SILO CSV: no header row")
    rows_out: list[dict[str, Any]] = []
    silo_lat = silo_lng = None
    for row in reader:
        if silo_lat is None:
            la = _parse_float(row.get("latitude", "") or "")
            lo = _parse_float(row.get("longitude", "") or "")
            if la is None or lo is None:
                raise ValueError("SILO CSV: missing latitude/longitude on first data row")
            silo_lat, silo_lng = la, lo
        ds = (row.get("YYYY-MM-DD") or row.get("Date") or "").strip()
        if not ds:
            continue
        obs = date.fromisoformat(ds)
        rain = _parse_float(row.get("daily_rain", "") or "")
        tmax = _parse_float(row.get("max_temp", "") or "")
        tmin = _parse_float(row.get("min_temp", "") or "")
        rh = _parse_float(row.get("rh_tmax", "") or "")
        rows_out.append(
            {
                "observation_date": obs,
                "daily_rain_mm": rain,
                "temp_max_c": tmax,
                "temp_min_c": tmin,
                "humidity_pct": rh,
            }
        )
    if silo_lat is None or silo_lng is None:
        raise ValueError("SILO CSV: no data rows")
    return silo_lat, silo_lng, rows_out


def _expected_days(start: date, finish: date) -> int:
    return (finish - start).days + 1


def _coverage_ratio(unique_dates: set[date], start: date, finish: date) -> float:
    exp = _expected_days(start, finish)
    if exp <= 0:
        return 0.0
    return len(unique_dates) / float(exp)


def _norm_coord6(v: float) -> float:
    return round(float(v), 6)


def _min_observations_for_skip(finish: date) -> int:
    exp = _expected_days(START_DATE, finish)
    return max(1, int(MIN_COVERAGE_RATIO * exp))


def _fetch_existing_seed_observations(tbl_cells: Any, log: logging.Logger) -> dict[tuple[str, float, float], int]:
    """(coverage_label lower, requested_lat, requested_lng) -> total_observations."""
    out: dict[tuple[str, float, float], int] = {}
    offset = 0
    page = 1000
    while True:
        resp = (
            tbl_cells.select("coverage_label,requested_lat,requested_lng,total_observations")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = getattr(resp, "data", None) or []
        if not batch:
            break
        for row in batch:
            lab = str(row.get("coverage_label") or "").strip().lower()
            try:
                rlat = float(row["requested_lat"])
                rlng = float(row["requested_lng"])
            except (KeyError, TypeError, ValueError):
                continue
            tobs = int(row.get("total_observations") or 0)
            key = (lab, _norm_coord6(rlat), _norm_coord6(rlng))
            out[key] = max(out.get(key, 0), tobs)
        if len(batch) < page:
            break
        offset += page
    log.info("loaded %s existing grid cell seed keys from Supabase for skip check", len(out))
    return out


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger("silo_weather_backfill")
    _load_env()

    email = (os.getenv("SILO_API_EMAIL") or "").strip()
    if not email:
        raise RuntimeError("SILO_API_EMAIL is required (SILO fair-use attribution / username query param).")

    from data_builder.config import get_settings

    settings = get_settings()
    sb_url = (settings.supabase_url or "").strip()
    sb_key = (settings.supabase_service_role_key or settings.supabase_key or "").strip()
    if not sb_url or not sb_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")

    import json
    from supabase import create_client

    cells = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    if not isinstance(cells, list):
        raise RuntimeError("seed_weather_test_cells.json must be a JSON array")

    finish = _brisbane_yesterday()
    supabase = create_client(sb_url, sb_key)
    tbl_cells = supabase.schema("shared").table("ref_weather_grid_cells")
    tbl_daily = supabase.schema("shared").table("ref_weather_daily")

    existing_obs = _fetch_existing_seed_observations(tbl_cells, log)
    min_obs_skip = _min_observations_for_skip(finish)

    t0 = time.perf_counter()
    total_inserted = 0
    failed = 0
    processed = 0
    skipped = 0

    with httpx.Client(headers={"User-Agent": settings.scraper_user_agent}) as http:
        for spec in cells:
            if not isinstance(spec, dict):
                continue
            label = str(spec.get("coverage_label") or "").strip()
            req_lat = float(spec["requested_lat"])
            req_lng = float(spec["requested_lng"])
            skip_key = (label.lower(), _norm_coord6(req_lat), _norm_coord6(req_lng))
            prev_obs = existing_obs.get(skip_key)
            if prev_obs is not None and prev_obs >= min_obs_skip:
                log.info(
                    "cell skip label=%r (already in Supabase: total_observations=%s >= %s)",
                    label,
                    prev_obs,
                    min_obs_skip,
                )
                skipped += 1
                continue
            cell_t0 = time.perf_counter()
            try:
                csv_text = _fetch_silo_csv(http, email, req_lat, req_lng, START_DATE, finish)
                silo_lat, silo_lng, daily = _parse_csv_rows(csv_text)
                gid = _grid_cell_id(silo_lat, silo_lng)
                udates = {r["observation_date"] for r in daily}
                cov = _coverage_ratio(udates, START_DATE, finish)
                if cov < MIN_COVERAGE_RATIO:
                    raise RuntimeError(
                        f"Gap/overlap check failed for {label!r}: coverage {cov:.2%} "
                        f"({len(udates)} unique days vs {_expected_days(START_DATE, finish)} expected); "
                        "threshold 95%."
                    )

                tbl_cells.upsert(
                    {
                        "grid_cell_id": gid,
                        "silo_lat": silo_lat,
                        "silo_lng": silo_lng,
                        "requested_lat": req_lat,
                        "requested_lng": req_lng,
                        "coverage_label": label or None,
                        "is_active": True,
                    },
                    on_conflict="grid_cell_id",
                ).execute()

                inserted_here = 0
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
                        inserted_here += len(batch)
                        batch.clear()
                if batch:
                    tbl_daily.upsert(
                        batch,
                        on_conflict="grid_cell_id,observation_date",
                        ignore_duplicates=True,
                    ).execute()
                    inserted_here += len(batch)

                dates_sorted = sorted(udates)
                first_d = dates_sorted[0]
                last_d = dates_sorted[-1]
                tbl_cells.update(
                    {
                        "first_observed_date": first_d.isoformat(),
                        "last_observed_date": last_d.isoformat(),
                        "total_observations": len(udates),
                    }
                ).eq("grid_cell_id", gid).execute()

                elapsed = time.perf_counter() - cell_t0
                log.info(
                    "cell ok label=%r grid_cell_id=%s rows=%s inserted_batches~=%s time=%.1fs",
                    label,
                    gid,
                    len(daily),
                    inserted_here,
                    elapsed,
                )
                total_inserted += inserted_here
                processed += 1
            except Exception as e:  # noqa: BLE001
                failed += 1
                log.exception("cell FAILED label=%r: %s", label, e)

            time.sleep(CELL_PAUSE_S)

    total_s = time.perf_counter() - t0
    log.info(
        "TOTAL cells_fetched_ok=%s cells_skipped=%s cells_failed=%s rows_upsert_batches=%s runtime=%.1fs",
        processed,
        skipped,
        failed,
        total_inserted,
        total_s,
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
