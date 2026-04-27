# Session handoff: SILO weather expansion (~100 grid cells)

## 0. Repo / branch hygiene

- **Remote:** `github.com/Milestoneigroup/data-builder.git`
- **Branch:** `feature/silo-weather-backfill` (aligned to `origin/feature/silo-weather-backfill` at `53ff0a8` before this commit; local branch had previously pointed at a busy-index commit — use `git fetch` + `git reset --hard origin/feature/silo-weather-backfill` if you see drift).

## 1. What shipped in this session

| Item | Detail |
|------|--------|
| Seed | `data/seed_weather_test_cells.json` — **100** unique SILO 0.05° grid anchors (was **10**) |
| Builder | `scripts/build_seed_weather_expansion.py` — loads active `shared.ref_destinations`, applies geographic rules, preserves the original **10** locked coordinates/labels, fills **Priority 1** (remaining `hierarchy_level=1` after WA exclusions), then **Priority 2** (`hierarchy_level=2` under NSW/VIC/QLD/WA h1) until **100** unique cells; if still under 80, adds h2 under SA/TAS/ACT h1. **Does not** modify migration `012`. |

**Geographic discipline (implemented in builder):**

- **WA:** Excludes `Broome and the Kimberley`, `Coral Coast (Geraldton, …)`, `Esperance and Goldfields` h1 trees entirely.
- **QLD / NSW:** Regex drops known inland / far-west place names (Mount Isa, Longreach, Broken Hill, etc.).
- **NT:** All three h1 regions kept; **h2+** only if under **Darwin and Top End** (Alice / Uluru / Red Centre sub-areas stay at h1 anchors only).

**Locked seed rows (unchanged lat/lng/labels):** the same 10 as the original test seed (Greater Sydney Metro, Melbourne, Hunter, Yarra, Margaret River, Sunshine Coast, Whitsundays, Hobart, Barossa, Byron Region).

## 2. Seed statistics (offline, from sorted file)

| Metric | Value |
|--------|-------|
| Cells before | 10 |
| Cells after | 100 |
| Coverage by state (label sort order / composition) | **ACT=1**, **NSW=44**, **NT=3**, **QLD=21**, **SA=8**, **TAS=6**, **VIC=11**, **WA=6** |

## 3. Steps still to run locally (operator)

This environment did not have `SILO_API_EMAIL` (or `DATABASE_URL`) in `.env.local`, so **backfill, monthly refresh, destination `grid_cell_id` UPDATE, and DB validation were not executed here.** Run on your machine (same pattern as the 10-cell test).

### 3.1 Backfill (~10–30 min, ~90 new SILO pulls + 2s pacing)

```bash
cd /path/to/data-builder
# Set SILO_API_EMAIL (and Supabase keys) in .env.local
python -m scrapers.silo_weather_backfill
```

Expect on success roughly **~960k–1M** rows in `shared.ref_weather_daily` (existing 10 cells upsert in place; ~90 net-new grids × ~9.6k days). If any cell reports **<95%** day coverage, the scraper **exits non-zero** — investigate that label before continuing.

### 3.2 Monthly refresh

```bash
python -m scrapers.silo_weather_monthly_refresh
```

Expect **~1,200** rows in `shared.ref_weather_monthly_stats` when **100** cells each have **12** months.

### 3.3 Populate `grid_cell_id` on destinations

Requires direct SQL (e.g. Supabase SQL editor or `psql` / `DATABASE_URL`):

```sql
UPDATE shared.ref_destinations d
SET grid_cell_id = (
  SELECT g.grid_cell_id
  FROM shared.ref_weather_grid_cells g
  WHERE g.is_active = true
  ORDER BY ((g.silo_lat - d.lat)^2 + (g.silo_lng - d.lng)^2) ASC
  LIMIT 1
)
WHERE d.is_active = true
  AND d.grid_cell_id IS NULL;
```

### 3.4 Validation SQL (from runbook)

Re-run the counts and spot checks from the autonomous session brief:

- `COUNT(*)` on `shared.ref_weather_daily` and `ref_weather_grid_cells`
- Cells with `total_observations < 9000` (expect **0** rows)
- `COUNT(DISTINCT grid_cell_id)` on `ref_weather_monthly_stats` vs cell count
- Linked / unlinked active destinations
- Optional: Tweed/Coffs and Mudgee/Orange/Clare monthly sanity selects

**After you run these**, paste the numeric results into this doc (or append a dated subsection) so the canonical handoff matches production.

## 4. Commit / push (after local validation)

```bash
git add data/seed_weather_test_cells.json scripts/build_seed_weather_expansion.py docs/SESSION_silo_weather_expansion_handoff.md
git commit -m "feat(weather): expand SILO backfill to ~100 cells"
git push origin feature/silo-weather-backfill
```

- **Commits (pushed to `origin/feature/silo-weather-backfill`):** `dc564ec` (feat: 100-cell seed + builder + handoff draft), `1cb7634` (docs: handoff commit hash).

## 5. SILO 100-CELL EXPANSION SUMMARY (fill post-run)

```
SILO 100-CELL EXPANSION SUMMARY
- Cells before: 10
- Cells after: 100 (seed); [N] after backfill in DB
- New rows added: [run delta on ref_weather_daily]
- Total rows in ref_weather_daily: [N]
- Coverage by state: NSW=44, VIC=11, QLD=21, SA=8, WA=6, TAS=6, ACT=1, NT=3 (seed labels)
- Destinations linked to cells: [N] / 414 active
- Coverage gaps: [N] cells with <9000 days
- Climate spot-checks: PASS / FAIL with brief note
- Commit: `1cb7634` (branch tip; feat in `dc564ec`)
```

## 6. Gippsland Coast fix and 100-cell finalisation (2026-04-27)

### 6.1 Seed change (`data/seed_weather_test_cells.json`)

The **Gippsland Coast** anchor was moved from `(-38.4, 147.5)` to **Lakes Entrance** `(-37.876, 147.989)`. The old centroid sat offshore in Bass Strait; SILO DataDrill is land-only and returned **0%** day coverage. A `_seed_note` field on that JSON object documents the reason (ignored by the scraper).

### 6.2 Backfill re-run (incremental)

`silo_weather_backfill` now **skips** SILO HTTP when a row already exists in `shared.ref_weather_grid_cells` with the same `coverage_label` and requested coordinates (6 dp) and `total_observations` ≥ 95% of the expected calendar span. The second full-seed run therefore issued **one** SILO pull (Gippsland only) plus **99** skips (~11 s).

### 6.3 Monthly refresh — stable pagination

`silo_weather_monthly_refresh` previously paged `ref_weather_daily` with `.range()` and **no `ORDER BY`**, which can duplicate or omit rows under PostgREST. That skewed `ref_weather_monthly_stats` (e.g. Hunter February inflated). The fetch now uses **`.order("weather_daily_id")`** before each range window.

### 6.4 Destinations

`UPDATE shared.ref_destinations … SET grid_cell_id = (nearest active grid cell)` for active rows with `grid_cell_id IS NULL` was executed via `DATABASE_URL` (psycopg). Result: **414** linked, **0** unlinked.

### 6.5 Validation query outputs (production snapshot)

```
UPDATE ref_destinations (grid_cell_id): rowcount = 0   -- second run: already linked

COUNT ref_weather_daily: 961300
COUNT ref_weather_grid_cells: 100
COUNT grid_cells total_observations < 9000: 0
DISTINCT monthly grid_cell_id: 100
destinations linked: 414
destinations unlinked: 0

-- Hunter regression --
(1, 72.71, 7.2)
(2, 93.73, 8.5)
(3, 95.72, 9.1)
(4, 64.54, 6.5)
(5, 42.25, 5.0)
(6, 60.55, 5.8)
(7, 35.22, 4.8)
(8, 34.47, 4.3)
(9, 40.89, 4.8)
(10, 50.93, 6.1)
(11, 76.98, 7.8)
(12, 72.32, 7.3)

-- Gippsland Coast monthly (Lakes Entrance anchor) --
(1, 44.03, 5.9)
(2, 50.69, 6.1)
(3, 57.70, 6.7)
(4, 63.72, 7.3)
(5, 44.94, 7.1)
(6, 75.01, 8.1)
(7, 47.30, 7.2)
(8, 53.96, 8.2)
(9, 48.99, 7.5)
(10, 61.27, 8.0)
(11, 77.50, 8.2)
(12, 66.92, 7.7)

-- Gippsland Coast daily rows --
gippsland_daily_rows: 9613
```

**Hunter regression:** PASS (February **~94 mm**, July **~35 mm**). **Gippsland:** **9,613** daily rows; winter/summer pattern plausible for East Gippsland coast.

### 6.6 Final summary block

```
SILO 100-CELL BACKFILL — FINALISED
- Total daily rows: 961300
- Cells loaded: 100 / 100 (target: 100)
- Coverage gaps: 0 cells under 9000 days
- Monthly stats cells: 100 / 100
- Destinations linked: 414 / 414
- Destinations still unlinked: 0
- Hunter regression: PASS (Feb ~94mm, Jul ~35mm)
- Gippsland Coast (new coord) coverage: 9613 daily rows
- Working tree clean: YES
- Final commit: c56b6127013769796b67cc30b2fa8f5ae48f05d7
```
