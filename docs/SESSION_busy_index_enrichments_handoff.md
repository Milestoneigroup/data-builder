# Session handoff — Busy Index enrichments (Migration 015)

## Migration 015 (`supabase/migrations/015_busy_index_enrichments.sql`)

- **Applied in Supabase SQL Editor (project `cxifxnsbaknjwtlstsly`):** *Pending — paste and run the migration file; this agent cannot access your project.*
- **Pre-req:** Migration `014_busy_index_calendar.sql` must already be applied (`ref_school_holidays`, `ref_public_holidays`, `ref_major_events`, `shared.set_updated_at()`).

## TRA / ABS accommodation ingestion

- **Script:** `scrapers/tra_accommodation_ingest.py` (uses `scrapers/tra_abs_sta_workbook.py`).
- **Source used:** Public ABS *Tourist Accommodation, Australia* 2015–16 **small-area data cubes** (tourism region monthly room occupancy, ADR-like and RevPAR-like columns). URLs are embedded as defaults; extend history with env `ABS_STA_XLS_URLS` as `STATE|URL,STATE|URL,...`.
- **Official TR geography:** `data/ref_tra_regions_asgs2021.json` from ABS **TR_2021_AUST.xlsx** (Tourism Regions, ASGS Ed.3).
- **Data window (defaults):** One financial year of cubes (2015–16); `years_in_sample` per `(tra_region_code, month_of_year)` will be **1** until additional FY URLs are added.
- **Blocked:** If ABS changes URLs or workbook layout, the parser may return zero rows — fix URLs or adjust `tra_abs_sta_workbook.py` and re-run.

## TRA region mapping seed

- **File:** `data/seed_destination_to_tra_region.json` (committed as an **empty array** until generated).
- **Generator:** `python -m scrapers.build_destination_tra_region_seed` (Supabase) or `python -m scrapers.build_destination_tra_region_seed --csv your_export.csv`.
- **Loader:** `python -m scrapers.load_destination_tra_region_seed`
- **Counts:** *Run generator locally against production `ref_destinations` — expect ~414 mapped rows; review `approximate` matches.*

## Event proximity

- **Script:** `python -m scrapers.compute_event_proximity`
- **Pairs / band distribution:** *Run locally after migration 015 + valid coordinates on destinations and events.*

## Views (sanity)

- **Long weekends:** `SELECT * FROM shared.v_long_weekend_windows WHERE window_start >= CURRENT_DATE LIMIT 10;`
- **Overlap peak dates:** Query `shared.v_school_holiday_overlap_daily` for overlap_intensity = `peak`.
- **Yarra Valley:** `SELECT * FROM shared.v_destination_busy_signal_monthly WHERE destination_name ILIKE '%yarra%';`

## Git

- **Branch:** `feature/busy-index-enrichments`
- **Commit:** `b11b67b`
