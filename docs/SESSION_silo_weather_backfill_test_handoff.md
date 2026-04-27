# Session handoff: SILO weather backfill (10 cells × ~26 years)

## 1. What shipped

| Item | Detail |
|------|--------|
| Branch | `feature/silo-weather-backfill` |
| Migrations | `012_weather_silo_tables.sql`, `013_weather_sequence_grants.sql` (sequence USAGE for `service_role` / PostgREST upserts) |
| Seed | `data/seed_weather_test_cells.json` |
| Scrapers | `scrapers/silo_weather_backfill.py`, `scrapers/silo_weather_monthly_refresh.py`, `scrapers/silo_weather_topup.py` |
| View | `shared.v_rain_predictor_monthly` |

**SILO CSV `comment`:** use **`RXNH`** for `daily_rain`, `max_temp`, `min_temp`, `rh_tmax` (not `RXNT`).

## 2. Validation results (Step 5 SQL)

Executed 2026-04-27 against project `cxifxnsbaknjwtlstsly` via `DATABASE_URL` (same DB as PostgREST).

### Total daily rows

```
cnt
96130
```

### Per-cell coverage

```
coverage_label	silo_lat	silo_lng	first_observed_date	last_observed_date	total_observations
Barossa Valley	-34.5333	138.9500	2000-01-01	2026-04-26	9613
Byron Region (Northern Rivers)	-28.7500	153.4500	2000-01-01	2026-04-26	9613
Greater Melbourne Metro	-37.8136	144.9631	2000-01-01	2026-04-26	9613
Greater Sydney Metro	-33.8150	151.0010	2000-01-01	2026-04-26	9613
Hobart & Southern Tasmania	-42.8821	147.3272	2000-01-01	2026-04-26	9613
Hunter Valley Wine Country	-32.7796	151.2900	2000-01-01	2026-04-26	9613
Margaret River & South West WA	-33.9556	115.0736	2000-01-01	2026-04-26	9613
Sunshine Coast & Noosa	-26.4000	153.0500	2000-01-01	2026-04-26	9613
Whitsundays	-20.2700	148.7200	2000-01-01	2026-04-26	9613
Yarra Valley & Dandenong Ranges	-37.6500	145.5500	2000-01-01	2026-04-26	9613
```

### Hunter Valley regression

```
month_of_year	avg_rainfall_mm	rain_days_avg	risk_rating
1	72.71	7.2	medium
2	93.73	8.5	medium
3	95.72	9.1	medium
4	64.54	6.5	medium
5	42.25	5.0	low
6	60.55	5.8	low
7	35.22	4.8	low
8	34.47	4.3	low
9	40.89	4.8	low
10	50.93	6.1	medium
11	76.98	7.8	medium
12	72.32	7.3	medium
```

### Whitsundays

```
month_of_year	avg_rainfall_mm	rain_days_avg	risk_rating
1	339.97	14.6	very_high
2	380.06	16.1	very_high
3	238.84	15.2	very_high
4	137.59	13.5	high
5	76.50	10.2	high
6	52.31	7.3	medium
7	45.78	5.2	low
8	22.45	3.1	low
9	20.87	2.8	low
10	34.12	4.0	low
11	69.64	6.5	medium
12	167.68	9.2	medium
```

### Hobart

```
month_of_year	avg_rainfall_mm	rain_days_avg
1	38.88	5.0
2	30.79	5.0
3	40.20	6.8
4	35.09	6.3
5	44.05	7.2
6	48.72	7.4
7	42.91	7.5
8	57.47	9.2
9	52.75	9.7
10	58.45	9.0
11	50.31	7.7
12	50.75	7.4
```

### Margaret River

```
month_of_year	avg_rainfall_mm	rain_days_avg
1	17.86	2.6
2	12.37	2.1
3	27.60	4.1
4	65.51	8.2
5	126.38	12.0
6	182.12	15.3
7	202.06	19.0
8	162.27	16.9
9	104.67	13.3
10	57.37	9.5
11	37.10	5.6
12	17.03	2.8
```

### Gap detection (`total_observations < 9000`)

```
(no rows)
```

## 3. Hunter regression — pass/fail

- **Feb ~100 mm:** `avg_rainfall_mm` February = **93.73** → **PASS** (within ~10% of 100).
- **July driest ~25 mm:** July = **35.22**, August = **34.47** (driest). Not ~25 mm; shape is dry mid-winter → **PARTIAL / soft FAIL** vs literal 25 mm benchmark; seasonal ordering **PASS**.

**Overall Hunter:** **PASS** on wet-season magnitude; **PARTIAL** on July-only 25 mm target.

## 4. Climate zone sanity — pass/fail

| Check | Result |
|--------|--------|
| **Whitsundays** wet Jan–Mar ≥200 mm; Jul–Sep &lt;50 mm | Jan–Mar 340 / 380 / 239 mm **PASS**; Jul–Sep 46 / 22 / 21 mm **PASS** |
| **Hobart** “flat” 40–60 mm year-round | **Jan 38.88, Feb 30.79** below 40 mm → **FAIL** strict band |
| **Margaret River** Mediterranean (winter wet, summer dry) | Low Jan–Mar, peak Jun–Aug **PASS** |

## 5. Issues encountered

| Issue | Resolution |
|-------|------------|
| PostgREST upsert: `permission denied for sequence ref_weather_daily_weather_daily_id_seq` | `GRANT USAGE, SELECT` on both `ref_weather_*` sequences to `service_role` (applied in DB; captured in `013_weather_sequence_grants.sql`) |
| Wrong commit on local `feature/silo-weather-backfill` | `git reset --hard origin/feature/silo-weather-backfill` before shipping |

## 6. Git state

- **Commit hash:** `0ba6e77af7936377b5c0eb3cd96ed66c585e3efe`
- **Branch:** `feature/silo-weather-backfill`
- **Push:** completed to `origin/feature/silo-weather-backfill` with this handoff + migration 013.

## 7. Next move

- Apply **`013_weather_sequence_grants.sql`** on any environment where 012 was applied without sequence grants (if not already run).
- Revisit **Hobart 40–60 mm** criterion (grid vs station climate) or accept wider band for validation.
- Expand grid coverage after sign-off.
