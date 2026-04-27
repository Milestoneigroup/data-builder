"""Aggregate NASA FIRMS VIIRS hotspots per destination × month-of-year (shared schema).

Run after ``python -m scrapers.firms_hotspot_download`` (combined CSV present).

Run: ``python -m scrapers.firms_aggregate_by_destination``

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY``),
combined CSV at ``data/firms_archive/AU_VIIRS_2012_to_present.csv``.
"""

from __future__ import annotations

import logging
import sys
from collections import defaultdict
from collections.abc import Iterator
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyproj import Transformer

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from data_builder.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("firms_aggregate")

_CRS_METRIC = "EPSG:3577"
_CRS_GEO = "EPSG:4326"
_CHUNK_ROWS = 80_000
_COMBINED = _ROOT / "data" / "firms_archive" / "AU_VIIRS_2012_to_present.csv"
_MONTH_ABB = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _confidence_ok(raw: Any) -> bool:
    s = str(raw).strip().lower()
    if s in ("", "nan", "none"):
        return False
    if s in ("low", "l"):
        return False
    if s in ("nominal", "high", "n", "h"):
        return True
    try:
        v = float(s)
    except ValueError:
        return False
    return v >= 30.0


def _norm_lon_col(df: pd.DataFrame) -> pd.Series:
    for c in ("longitude", "lon", "lng"):
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    raise KeyError("no longitude column (expected longitude/lon/lng)")


def _norm_lat_col(df: pd.DataFrame) -> pd.Series:
    for c in ("latitude", "lat"):
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    raise KeyError("no latitude column (expected latitude/lat)")


def _norm_conf_col(df: pd.DataFrame) -> pd.Series:
    for c in ("confidence", "conf"):
        if c in df.columns:
            return df[c]
    raise KeyError("no confidence column")


def _norm_date_col(df: pd.DataFrame) -> pd.Series:
    for c in ("acq_date", "acquired_date", "date"):
        if c in df.columns:
            return pd.to_datetime(df[c], errors="coerce")
    raise KeyError("no acquisition date column (expected acq_date)")


def _cdist_m(h_xy: np.ndarray, d_xy: np.ndarray) -> np.ndarray:
    """Euclidean distance in metres (projected CRS), shapes (nh,2) and (nd,2)."""
    diff = h_xy[:, None, :] - d_xy[None, :, :]
    return np.sqrt((diff * diff).sum(axis=2))


def _year_range(dmin: date, dmax: date) -> list[int]:
    return list(range(dmin.year, dmax.year + 1))


def _relative_labels(avg_by_month: dict[int, float]) -> dict[int, str]:
    """Rank 12 months by avg 25km; assign peak/high/medium/low/lowest (2+2+4+2+2)."""
    months_sorted = sorted(avg_by_month.keys(), key=lambda m: (-avg_by_month[m], m))
    label_by_month: dict[int, str] = {}
    buckets = (
        ("peak", 2),
        ("high", 2),
        ("medium", 4),
        ("low", 2),
        ("lowest", 2),
    )
    idx = 0
    for label, n in buckets:
        for _ in range(n):
            if idx >= len(months_sorted):
                break
            label_by_month[months_sorted[idx]] = label
            idx += 1
    return label_by_month


def _absolute_labels_from_ranks(values: list[float]) -> list[str]:
    """Map each value to AU-wide bucket using mid-rank percentile (handles ties)."""
    if not values:
        return []
    arr = np.array(values, dtype=np.float64)
    n = len(arr)
    rk = pd.Series(arr).rank(method="average", ascending=True).to_numpy()
    prop_high = (rk - 0.5) / max(n, 1)
    out: list[str] = []
    for ph in prop_high:
        if ph >= 0.95:
            out.append("extreme")
        elif ph >= 0.80:
            out.append("high")
        elif ph >= 0.50:
            out.append("medium")
        else:
            out.append("low")
    return out


def _read_csv_chunks(path: Path) -> Iterator[pd.DataFrame]:
    for chunk in pd.read_csv(path, chunksize=_CHUNK_ROWS, low_memory=False):
        yield chunk


def main() -> int:
    _load_env()
    if not _COMBINED.is_file():
        log.error("Missing combined CSV: %s", _COMBINED)
        return 1

    st = get_settings()
    url = (st.supabase_url or "").strip()
    key = (st.supabase_service_role_key or st.supabase_key or "").strip()
    if not url or not key:
        log.error("SUPABASE_URL and service key required.")
        return 1

    from supabase import create_client

    sb = create_client(url, key)

    dest_res = (
        sb.schema("shared")
        .table("ref_destinations")
        .select("destination_id,lat,lng")
        .eq("is_active", True)
        .execute()
    )
    dest_rows = dest_res.data or []
    if not dest_rows:
        log.error("No active destinations from Supabase.")
        return 1

    dest_ids: list[str] = []
    d_lng: list[float] = []
    d_lat: list[float] = []
    for r in dest_rows:
        lat, lng = r.get("lat"), r.get("lng")
        if lat is None or lng is None:
            continue
        try:
            d_lat.append(float(lat))
            d_lng.append(float(lng))
        except (TypeError, ValueError):
            continue
        dest_ids.append(str(r["destination_id"]))

    if not dest_ids:
        log.error("No destinations with lat/lng.")
        return 1

    trans = Transformer.from_crs(_CRS_GEO, _CRS_METRIC, always_xy=True)
    dx, dy = trans.transform(np.array(d_lng), np.array(d_lat))
    d_xy = np.column_stack([dx, dy])

    triple: dict[tuple[str, int, int], list[int]] = defaultdict(lambda: [0, 0, 0])
    dmin: date | None = None
    dmax: date | None = None
    total_rows = 0
    kept_rows = 0

    for chunk in _read_csv_chunks(_COMBINED):
        total_rows += len(chunk)
        try:
            lat_s = _norm_lat_col(chunk)
            lon_s = _norm_lon_col(chunk)
            conf = _norm_conf_col(chunk)
            dt = _norm_date_col(chunk)
        except KeyError as e:
            log.error("%s", e)
            return 2
        mask = lat_s.notna() & lon_s.notna() & dt.notna()
        chunk = chunk.loc[mask].copy()
        lat_s = lat_s.loc[mask].to_numpy(dtype=np.float64)
        lon_s = lon_s.loc[mask].to_numpy(dtype=np.float64)
        dt = dt.loc[mask]
        conf_vals = conf.loc[mask]

        keep = np.array([_confidence_ok(v) for v in conf_vals], dtype=bool)
        lat_s = lat_s[keep]
        lon_s = lon_s[keep]
        dt = dt[keep].reset_index(drop=True)
        if len(lat_s) == 0:
            continue
        kept_rows += len(lat_s)

        hx, hy = trans.transform(lon_s, lat_s)
        h_xy = np.column_stack([hx, hy])
        years = dt.dt.year.to_numpy(dtype=np.int32)
        months = dt.dt.month.to_numpy(dtype=np.int32)
        dates = dt.dt.date
        local_min = dates.min()
        local_max = dates.max()
        dmin = local_min if dmin is None else min(dmin, local_min)
        dmax = local_max if dmax is None else max(dmax, local_max)

        step = 12_000
        for i0 in range(0, len(h_xy), step):
            sl = slice(i0, i0 + step)
            dist_m = _cdist_m(h_xy[sl], d_xy)
            ysub = years[sl]
            msub = months[sl]
            for i in range(dist_m.shape[0]):
                row = dist_m[i]
                js = np.flatnonzero(row <= 50_000.0)
                if js.size == 0:
                    continue
                yi = int(ysub[i])
                mi = int(msub[i])
                ri = row[js]
                for k, j in enumerate(js):
                    did = dest_ids[int(j)]
                    key = (did, yi, mi)
                    t = triple[key]
                    t[2] += 1
                    if ri[k] <= 25_000.0:
                        t[1] += 1
                    if ri[k] <= 10_000.0:
                        t[0] += 1

    if dmin is None or dmax is None:
        log.error("No valid hotspot rows after filtering.")
        return 3

    years_list = _year_range(dmin, dmax)
    years_in_sample = len(years_list)
    log.info(
        "loaded hotspots: rows=%s kept_confidence=%s period=%s..%s years=%s destinations=%s",
        total_rows,
        kept_rows,
        dmin,
        dmax,
        years_in_sample,
        len(dest_ids),
    )

    # Per destination × month: yearly counts for 10/25/50 km
    rows_out: list[dict[str, Any]] = []
    all_avgs_25: list[float] = []

    for did in dest_ids:
        avg_by_month: dict[int, float] = {}
        for month in range(1, 13):
            yvec10: list[int] = []
            yvec25: list[int] = []
            yvec50: list[int] = []
            for y in years_list:
                t = triple.get((did, y, month), [0, 0, 0])
                yvec10.append(t[0])
                yvec25.append(t[1])
                yvec50.append(t[2])
            avg10 = float(np.mean(yvec10)) if yvec10 else 0.0
            avg25 = float(np.mean(yvec25)) if yvec25 else 0.0
            avg50 = float(np.mean(yvec50)) if yvec50 else 0.0
            max25 = int(max(yvec25)) if yvec25 else 0
            sig_years = sum(1 for v in yvec25 if v > 10)
            avg_by_month[month] = avg25
            all_avgs_25.append(avg25)
            rows_out.append(
                {
                    "destination_id": did,
                    "month_of_year": month,
                    "years_in_sample": years_in_sample,
                    "avg_hotspots_per_year_10km": round(avg10, 1),
                    "avg_hotspots_per_year_25km": round(avg25, 1),
                    "avg_hotspots_per_year_50km": round(avg50, 1),
                    "max_month_hotspots_25km": max25,
                    "years_with_significant_activity_25km": sig_years,
                    "data_source": "NASA FIRMS VIIRS_SNPP",
                    "data_period_start": dmin.isoformat(),
                    "data_period_end": dmax.isoformat(),
                }
            )

    abs_by_row = _absolute_labels_from_ranks(all_avgs_25)
    log.info(
        "AU absolute risk: mid-rank percentiles on avg_25km across all destination×month rows (n=%s)",
        len(all_avgs_25),
    )

    for i, r in enumerate(rows_out):
        r["absolute_risk_label"] = abs_by_row[i]

    by_dest: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows_out:
        by_dest[str(r["destination_id"])].append(r)

    for did, rlist in by_dest.items():
        avg_by_month = {int(r["month_of_year"]): float(r["avg_hotspots_per_year_25km"]) for r in rlist}
        rel = _relative_labels(avg_by_month)
        for r in rlist:
            m = int(r["month_of_year"])
            r["relative_risk_label"] = rel.get(m)

    batch: list[dict[str, Any]] = []
    for r in rows_out:
        batch.append(r)
        if len(batch) >= 400:
            (
                sb.schema("shared")
                .table("ref_destination_fire_activity_monthly")
                .upsert(batch, on_conflict="destination_id,month_of_year")
                .execute()
            )
            batch.clear()
    if batch:
        (
            sb.schema("shared")
            .table("ref_destination_fire_activity_monthly")
            .upsert(batch, on_conflict="destination_id,month_of_year")
            .execute()
        )

    # Denormalise ref_destinations
    for did, rlist in by_dest.items():
        rel_order = sorted(
            rlist,
            key=lambda x: (
                -{"peak": 5, "high": 4, "medium": 3, "low": 2, "lowest": 1}.get(
                    str(x.get("relative_risk_label")), 0
                ),
                -float(x["avg_hotspots_per_year_25km"]),
                int(x["month_of_year"]),
            ),
        )
        peak_ms = [
            _MONTH_ABB[int(x["month_of_year"]) - 1]
            for x in rel_order
            if x.get("relative_risk_label") == "peak"
        ][:2]
        low_ms = [
            _MONTH_ABB[int(x["month_of_year"]) - 1]
            for x in sorted(
                rlist,
                key=lambda x: (
                    float(x["avg_hotspots_per_year_25km"]),
                    int(x["month_of_year"]),
                ),
            )
            if x.get("relative_risk_label") == "lowest"
        ][:2]
        peak_s = ", ".join(peak_ms) if peak_ms else ""
        low_s = ", ".join(low_ms) if low_ms else ""
        (
            sb.schema("shared")
            .table("ref_destinations")
            .update(
                {
                    "peak_fire_months": peak_s,
                    "lowest_fire_months": low_s,
                    "fire_activity_data_period_end": dmax.isoformat(),
                }
            )
            .eq("destination_id", did)
            .execute()
        )

    log.info(
        "done: destinations=%s monthly_rows=%s AU thresholds p50/p80/p95 on 25km avg",
        len(by_dest),
        len(rows_out),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
