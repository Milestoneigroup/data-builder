"""Ingest public ABS Tourist Accommodation (STA) small-area cubes → ``shared.ref_accommodation_pressure_monthly``.

The ABS publishes establishment-level cubes with **Tourism Region (TR)** sub-totals and monthly
room occupancy, takings-based ADR-like rates, and RevPAR-like rates. This is the same TR geography
used with TRA STAR reporting (ASGS Tourism Regions).

**Sources (verify periodically):**

* ABS *Tourist Accommodation, Australia* — data cubes by state, e.g. 2015–16 financial year:
  https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/

Default download set is the 2015–16 cubes (eight jurisdictions). Add more FY URLs via
``ABS_STA_XLS_URLS`` (comma-separated) to extend history.

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY``),
``httpx``, ``pandas``, ``xlrd``, ``thefuzz``, ``python-dotenv``.

Run: ``python -m scrapers.tra_accommodation_ingest``
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from thefuzz import fuzz, process

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scrapers.tra_abs_sta_workbook import TraMonthlyObservation, parse_sta_workbook

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

# Known label drift between older ABS cubes and ASGS Edition 3 TR names (ABS TR_2021_AUST.xlsx).
_LABEL_ALIASES: dict[tuple[str, str], str] = {
    ("VIC", "Western"): "Great Ocean Road",
    ("VIC", "Melbourne East"): "Melbourne",
    ("VIC", "Upper Yarra"): "Yarra Valley and the Dandenong Ranges",
    ("VIC", "Geelong"): "Geelong and the Bellarine",
}

_DEFAULT_ABS_STA_URLS: dict[str, str] = {
    "NSW": "https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/att1zear.xls",
    "VIC": "https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/86350do003_201516.xls",
    "QLD": "https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/86350do004_201516.xls",
    "SA": "https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/86350do005_201516.xls",
    "WA": "https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/86350do006_201516.xls",
    "TAS": "https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/86350do007_201516.xls",
    "NT": "https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/86350do008_201516.xls",
    "ACT": "https://www.abs.gov.au/statistics/industry/tourism-and-transport/tourist-accommodation-australia/2015-16/86350do009_201516.xls",
}

_TR_REGIONS_PATH = _ROOT / "data" / "ref_tra_regions_asgs2021.json"


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _load_tra_region_catalog() -> list[dict[str, str]]:
    if not _TR_REGIONS_PATH.is_file():
        raise FileNotFoundError(
            f"Missing {_TR_REGIONS_PATH}. Regenerate from ABS TR_2021_AUST.xlsx or restore from repo."
        )
    data = json.loads(_TR_REGIONS_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("ref_tra_regions_asgs2021.json must be a list")
    au_states = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}
    return [r for r in data if isinstance(r, dict) and str(r.get("state_code", "")).upper() in au_states]


def _match_tra_code(
    state_code: str,
    region_label: str,
    catalog: list[dict[str, str]],
) -> tuple[str, str, str] | None:
    """Return (tra_region_code, tra_region_name, confidence_bucket) or None if unmapped."""
    key = (state_code, region_label)
    target_name = _LABEL_ALIASES.get(key, region_label)
    choices = [r for r in catalog if r.get("state_code") == state_code]
    if not choices:
        return None
    names = [c["tra_region_name"] for c in choices]
    best = process.extractOne(
        target_name,
        names,
        scorer=fuzz.token_sort_ratio,
    )
    if not best:
        return None
    if len(best) == 3:
        name_hit, score, _ = best
    else:
        name_hit, score = best
    if score < 72:
        LOG.warning("Low fuzzy score %s for %s / %s — skipping", score, state_code, region_label)
        return None
    row = next(c for c in choices if c["tra_region_name"] == name_hit)
    conf = "exact" if score >= 95 else "strong" if score >= 85 else "approximate"
    return row["tra_region_code"], row["tra_region_name"], conf


def _relative_labels(avg_by_month: dict[int, float]) -> dict[int, str]:
    """Assign peak/high/medium/low/lowest by occupancy rank (2+2+4+2+2 months)."""
    months_sorted = sorted(avg_by_month.keys(), key=lambda m: (-avg_by_month[m], m))
    label_by_month: dict[int, str] = {}
    buckets = (("peak", 2), ("high", 2), ("medium", 4), ("low", 2), ("lowest", 2))
    i = 0
    for label, n in buckets:
        for _ in range(n):
            if i >= len(months_sorted):
                break
            label_by_month[months_sorted[i]] = label
            i += 1
    return label_by_month


def _peak_month(avg_by_month: dict[int, float]) -> int:
    return max(avg_by_month.keys(), key=lambda m: (avg_by_month[m], -m))


def _download_urls(client: httpx.Client, urls: Iterable[tuple[str, str]]) -> list[TraMonthlyObservation]:
    all_obs: list[TraMonthlyObservation] = []
    for state_code, url in urls:
        LOG.info("Downloading %s %s", state_code, url)
        r = client.get(url, timeout=120.0)
        r.raise_for_status()
        obs = parse_sta_workbook(r.content, state_code=state_code)
        LOG.info("  Parsed %s TR-month rows", len(obs))
        all_obs.extend(obs)
    return all_obs


def _aggregate_rows(
    observations: list[TraMonthlyObservation],
    catalog: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Collapse observations to one row per (tra_region_code, month_of_year)."""
    # (code, name, state, month_of_year) -> lists
    occ: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    adr: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    rev: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    years: dict[tuple[str, str, str, int], set[int]] = defaultdict(set)
    dates: dict[tuple[str, str, str, int], list[date]] = defaultdict(list)

    skipped = 0
    for o in observations:
        if o.occupancy_pct is None:
            continue
        hit = _match_tra_code(o.state_code, o.region_label, catalog)
        if hit is None:
            skipped += 1
            continue
        code, name, _conf = hit
        key = (code, name, o.state_code, o.obs_month)
        occ[key].append(o.occupancy_pct)
        years[key].add(o.obs_year)
        dates[key].append(date(o.obs_year, o.obs_month, 1))
        if o.adr_aud is not None:
            adr[key].append(o.adr_aud)
        if o.revpar_aud is not None:
            rev[key].append(o.revpar_aud)

    if skipped:
        LOG.warning("Skipped %s observations with no TR code match", skipped)

    # group by tra_region_code for relative labels
    by_region_month: dict[str, dict[int, float]] = defaultdict(dict)
    meta: dict[tuple[str, int], dict[str, Any]] = {}
    for key, occ_vals in occ.items():
        code, name, st, mo = key
        by_region_month[code][mo] = sum(occ_vals) / len(occ_vals)
        meta[(code, mo)] = {
            "tra_region_code": code,
            "tra_region_name": name,
            "state_code": st,
            "month_of_year": mo,
            "years_in_sample": len(years[key]),
            "avg_occupancy_pct": round(sum(occ_vals) / len(occ_vals), 2),
            "avg_adr_aud": round(sum(adr[key]) / len(adr[key]), 2) if adr[key] else None,
            "avg_revpar_aud": round(sum(rev[key]) / len(rev[key]), 2) if rev[key] else None,
            "data_period_start": min(dates[key]).isoformat(),
            "data_period_end": max(dates[key]).isoformat(),
        }

    out: list[dict[str, Any]] = []
    for code, month_avgs in by_region_month.items():
        labels = _relative_labels(month_avgs)
        peak_m = _peak_month(month_avgs)
        for mo in range(1, 13):
            row = meta.get((code, mo))
            if not row:
                continue
            row = dict(row)
            row["relative_pressure_label"] = labels.get(mo)
            row["peak_month_for_region"] = mo == peak_m
            row["data_source"] = "Tourism Research Australia STAR"
            row["last_refreshed_at"] = datetime.now(timezone.utc).isoformat()
            out.append(row)
    return out


def main() -> None:
    _load_env()
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) required.")

    extra = (os.getenv("ABS_STA_XLS_URLS") or "").strip()
    urls: list[tuple[str, str]] = []
    if extra:
        for chunk in extra.split(","):
            chunk = chunk.strip()
            if not chunk or "|" not in chunk:
                continue
            st, u = chunk.split("|", 1)
            urls.append((st.strip().upper(), u.strip()))
    else:
        urls = list(_DEFAULT_ABS_STA_URLS.items())

    catalog = _load_tra_region_catalog()
    with httpx.Client(follow_redirects=True, headers={"User-Agent": "milestonei-data-builder/tra-ingest"}) as client:
        observations = _download_urls(client, urls)

    if not observations:
        raise SystemExit("No observations parsed — check ABS URLs or workbook format.")

    rows = _aggregate_rows(observations, catalog)
    LOG.info("Upserting %s rows into shared.ref_accommodation_pressure_monthly", len(rows))

    from supabase import create_client

    sb = create_client(url, key)
    sh = sb.schema("shared")
    batch = 200
    for i in range(0, len(rows), batch):
        part = rows[i : i + batch]
        sh.table("ref_accommodation_pressure_monthly").upsert(part, on_conflict="tra_region_code,month_of_year").execute()

    LOG.info("Done at %s", datetime.now(timezone.utc).isoformat())


if __name__ == "__main__":
    main()
