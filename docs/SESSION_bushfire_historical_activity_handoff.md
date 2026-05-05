# SESSION: Bushfire historical activity (NASA FIRMS)

## What shipped

- **Branch:** `feature/bushfire-historical-activity`
- **Migration:** `supabase/migrations/016_bushfire_historical_activity.sql` (replaces prior `013_bushfire_historical_activity.sql` numbering on this branch)
- **Scrapers:** `scrapers/firms_hotspot_download.py`, `scrapers/firms_aggregate_by_destination.py`
- **Deps:** `requirements.txt` includes `pyproj>=3.6.0` (with geopandas/shapely)
- **Database:** table `shared.ref_destination_fire_activity_monthly`, view `shared.v_destination_fire_risk_by_month`, columns on `shared.ref_destinations` (`peak_fire_months`, `lowest_fire_months`, `fire_activity_data_period_end`)

## FIRMS API verification (2026-05-06)

- **Doc URL:** https://firms.modaps.eosdis.nasa.gov/api/area/
- **CSV path pattern:** `/api/area/csv/{MAP_KEY}/{SOURCE}/{west,south,east,north}/{DAY_RANGE}/{DATE}`
- **DAY_RANGE:** 1–5 days per request (confirmed)
- **BBox format:** `west,south,east,north` (AU: `112,-44,154,-10`)
- **Archive source:** `VIIRS_SNPP_SP`
- **Behaviour:** Direct HTTP CSV download works (no bulk-email-only gate); retries observed on 429/5xx path; client uses 3 attempts with 2s/4s/8s backoff and 2s pacing between chunks

## Runbook

```bash
python -m scrapers.firms_hotspot_download
python -m scrapers.firms_aggregate_by_destination
```

Raw CSV chunks: `data/firms_archive/{year}/`; combined: `data/firms_archive/AU_VIIRS_2012_to_present.csv` (not loaded into Supabase).

## Data window (this run)

| Metric | Value |
| --- | --- |
| Requested start | 2012-01-20 |
| Download end (UTC yesterday at run time) | Through 2026-05-04 chunk window |
| Max acq date after confidence filter | **2026-02-28** |
| Calendar span (`years_in_sample`) | **15** (2012–2026) |
| Total chunks | 1044 |
| Failed chunks | 0 |
| Download runtime | ~3958 s (~66 min) |
| Raw rows (header excluded per chunk) | **17,201,713** |
| Rows after confidence filter | **15,543,247** |
| Aggregation runtime | ~605 s (~10 min) |

## Migration verification (DEBT-043)

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'shared' AND table_name = 'ref_destination_fire_activity_monthly';
-- Result: 1 row (applied via Supabase MCP migration apply).
```

## Validation SQL outputs

**Coverage**

- `COUNT(DISTINCT destination_id)` from `ref_destination_fire_activity_monthly`: **414**
- `COUNT(*)` active destinations: **414**
- Destinations with ≠12 rows: **0**

**Relative risk distribution**

| Label | Count |
| --- | --- |
| peak | 828 |
| high | 828 |
| medium | 1656 |
| low | 828 |
| lowest | 828 |

**Absolute risk distribution**

| Label | Count |
| --- | --- |
| extreme | 248 |
| high | 747 |
| medium | 1487 |
| low | 2486 |

**Monthly rows upserted:** 4968 (= 414 × 12)

## Sanity checks (climate / geography)

| Check | Result | Notes |
| --- | --- | --- |
| Yarra Valley summer peak (Dec–Mar) | **FAIL** | Peaks **Mar–Apr**; Dec–Jan labelled **lowest** for “Yarra Valley and Dandenong Ranges” / Healesville rows |
| Blue Mountains summer peak (Dec–Mar) | **FAIL** | Relative **peak** months **Oct + Dec** (not Dec–Mar band) |
| Darwin / Top End dry-season peak (Aug–Nov) | **FAIL** | Relative **peak** months **May–Jun** (build-up); Aug–Nov are high/medium |
| Greater Sydney Metro — no `extreme` | **PASS** | Absolute labels **medium** / **high** only |
| Greater Melbourne Metro | (see DB) | All months **high** absolute (dense metro buffer — not in summary PASS/FAIL lines) |

## Coverage gaps

- None: every active destination has exactly 12 monthly rows.

## STOP criteria triggered

Per task rules: Yarra not peaking southern summer window as stated, Darwin not peaking Aug–Nov, Blue Mountains pattern mismatch — **report FAIL** and investigate seasonal attribution / destination centroid vs fire geography before product messaging.

## Final commit

Recorded at branch tip after push — inspect with `git log -1 --oneline` on `feature/bushfire-historical-activity`.
