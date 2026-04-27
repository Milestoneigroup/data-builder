# Session handoff: Bushfire historical fire activity (Migration 013)

## 1. What shipped

| Item | Path |
|------|------|
| Migration 013 | `supabase/migrations/013_bushfire_historical_activity.sql` |
| FIRMS downloader | `scrapers/firms_hotspot_download.py` |
| Spatial aggregation + Supabase upsert | `scrapers/firms_aggregate_by_destination.py` |
| Annual refresh stub | `scrapers/firms_annual_refresh.py` |
| Dependency | `pyproj>=3.6.0` added to `requirements.txt` |

Branch: `feature/bushfire-historical-activity` (based off `main` at time of work).

## 2. Data window

| Field | Value |
|-------|--------|
| Planned start | 2012-01-20 (VIIRS S-NPP archive operational date) |
| Planned end | Yesterday (UTC) in downloader |
| Source | `VIIRS_SNPP_SP` (Standard Processing), Australia bounding box |

Downloader and aggregation were **not executed in this environment** (no `NASA_FIRMS_MAP_KEY`, no Supabase credentials in repo, Supabase CLI not on global PATH). After you run locally, fill in:

- Total years in sample, total hotspots downloaded, total rows after confidence filter.

## 3. API verification notes (2026-04-27)

- **Index:** [https://firms.modaps.eosdis.nasa.gov/api/](https://firms.modaps.eosdis.nasa.gov/api/) — Area service documented for CSV hotspot extracts.
- **Area CSV path:** `/api/area/csv/{MAP_KEY}/{SOURCE}/{west,south,east,north}/{DAY_RANGE}/{DATE}`
  - Without `DATE`, returns most recent window.
  - With `DATE`, returns `[DATE .. DATE + DAY_RANGE - 1]` inclusive.
- **DAY_RANGE:** Documentation states **1–5 days** per request (not 10). The downloader uses **5**.
- **AU bbox (west,south,east,north):** `112,-44,154,-10` (equivalent to lat −44..−10, lng 112..154).
- **Historical bulk:** `VIIRS_SNPP_SP` (archive). `VIIRS_SNPP_NRT` is near-real-time, not the right primary source for long archive backfill.
- **VIIRS confidence (FAQ):** Values are categorical **low / nominal / high**. The aggregator keeps **nominal and high** only; numeric confidence values ≥ **30** are treated as nominal-or-better; **low** / **l** / values &lt; 30 are dropped.
- **Rate limits:** Public docs do not quote a fixed RPM; the downloader uses a **2 s** pause between requests and retries on 429/5xx. If failure rate exceeds **5%** of chunks, the script exits before building the combined CSV.

## 4. Validation results

Run after `supabase db push` and `python -m scrapers.firms_aggregate_by_destination`:

```sql
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_schema = 'shared' AND table_name = 'ref_destination_fire_activity_monthly';
```

Paste outputs of the Step 5 queries from the task brief (coverage, `HAVING COUNT(*) != 12`, Yarra Valley, Blue Mountains, Sydney Metro, Darwin, label distributions) **here after local run**.

## 5. Sanity check results

| Check | Result |
|-------|--------|
| Yarra Valley summer peak | **Pending** (needs aggregate run) |
| Blue Mountains summer peak | **Pending** |
| Sydney metro mostly low absolute | **Pending** |
| Darwin dry-season peak | **Pending** |

Do **not** treat the pipeline as production-valid until these pass.

## 6. Distribution audit

**Pending** — run:

```sql
SELECT relative_risk_label, COUNT(*) FROM shared.ref_destination_fire_activity_monthly GROUP BY relative_risk_label;
SELECT absolute_risk_label, COUNT(*) FROM shared.ref_destination_fire_activity_monthly GROUP BY absolute_risk_label;
```

## 7. Coverage gaps

**Pending** — after aggregation, confirm no destinations with `COUNT(*) != 12` and review any destination with all-zero hotspots (possible coordinate issues).

## 8. Git state

**Pending** — after `git push -u origin feature/bushfire-historical-activity`, record:

- Commit hash: `________________`
- Push: confirmed / not run

## 9. Next move

- Revisit **confidence** rules if industrial false positives persist (nominal-only vs nominal+high).
- **MODIS** pre-2012 or **per-venue** buffers as a v2 track.
- Wire **read-only** consumption from `shared.v_destination_fire_risk_by_month` in product APIs (outside this repo).
