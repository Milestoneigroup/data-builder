# Session handoff — Busy Index v1 (calendar layer)

## 1. What shipped

| Item | Detail |
|------|--------|
| Branch | `feature/busy-index-v1` |
| Migration | `supabase/migrations/014_busy_index_calendar.sql` (tables + `verified_at` + RLS + `shared.v_busy_signal_daily`) |
| Seeds | `data/seed_school_holidays_AU.json`, `data/seed_public_holidays_AU.json`, `data/seed_major_events_AU.json` — **forward-looking only** (`start_date` / `observed_date` ≥ verification date); every row includes `verified_at` and `source_url` |
| Generator | `scripts/_gen_busy_forward_seeds.py` — regenerates the three JSON files from curated constants |
| Loader | `scrapers/load_busy_index_seeds.py` — upserts into `shared.*`, computes `creates_long_weekend`, skips rows before **today** at runtime |
| Annual stub | `scrapers/refresh_busy_index.py` — placeholder only |
| Scheduler hook | Comment-only block in `scrapers/monthly_snapshot.py` (not activated; no Railway change) |

## 2. Seed data sources (school holidays)

Official URLs (verified **2026-04-27**). NT calendar listing uses the live URL (`school-term-dates-in-nt`); the older `school-term-dates-and-holidays` path returned 404 at verification time.

| State | Agency | `source_url` (school) |
|-------|--------|------------------------|
| NSW | NSW Department of Education | `https://www.education.nsw.gov.au/schooling/calendars/2026` + `https://www.education.nsw.gov.au/schooling/calendars/future-and-past-nsw-term-and-vacation-dates` |
| VIC | Victorian Government | `https://www.vic.gov.au/school-term-dates-and-holidays-victoria` |
| QLD | Queensland Department of Education | `https://education.qld.gov.au/about-us/calendar/term-dates` + `https://education.qld.gov.au/about-us/calendar/future-dates` |
| SA | Department for Education, South Australia | `https://www.education.sa.gov.au/parents-and-families/term-dates-south-australian-state-schools` |
| WA | WA Department of Education (future term dates) | `https://www.education.wa.edu.au/future-term-dates` |
| TAS | DECYP | `https://www.decyp.tas.gov.au/learning/term-dates/` |
| NT | Northern Territory Government | `https://nt.gov.au/learning/primary-and-secondary-students/school-term-dates-in-nt` |
| ACT | ACT Government | `https://www.act.gov.au/living-in-the-act/public-holidays-school-terms-and-daylight-saving` |

**Division note (NSW):** Seeds use **Eastern division** dates only.

## 3. Public holidays

| Source | URL |
|--------|-----|
| Fair Work Ombudsman | `https://www.fairwork.gov.au/employment-conditions/public-holidays/2026-public-holidays` |

Seed file includes a **small forward subset** (observed date ≥ 2026-04-27) drawn from the Fair Work 2026 state lists. Expand next refresh; do not treat the seed as exhaustive.

## 4. Major events

| Count | Notes |
|------:|-------|
| 3 | `vivid-sydney-2026`, `australian-open-2027`, `melbourne-cup-day-2026` |

**Excluded / gaps**

- **Splendour in the Grass** — excluded (hiatus / on–off status not verified for a forward instance).
- **AFL Grand Final public holiday (VIC)** — Fair Work lists date TBC; not seeded.
- **Australian Open 2026** — not seeded (tournament before the forward cutover for net-new product use is superseded by published **2027** dates).

## 5. Forward window per source (audit)

| Source | Latest calendar year published (at 2026-04-27) |
|--------|-----------------------------------------------|
| NSW DoE | 2027 school year block on future/past page; 2026 detail page |
| VIC | 2027 + future blocks (2030) on vic.gov.au |
| QLD | 2026 + 2027–2029 on future-dates page |
| SA | 2030 term grid on education.sa.gov.au |
| WA | 2031 term tables on future-term-dates |
| TAS (DECYP) | **2027** student terms on site; **no 2028+** on same page → **no TAS Summer 2027–28 row** in seed |
| NT | 2032 accordion on nt.gov.au |
| ACT | 2027 table + 2026–2030 PDF links on act.gov.au |
| Fair Work PH | 2026 multi-state list (dedicated page); 2027 not bulk-imported in this seed |
| Vivid Sydney | 2026 festival dates on operator site |
| Australian Open | 2027 dates article on ausopen.com |

## 6. Validation (SQL)

After `supabase db push` and `python -m scrapers.load_busy_index_seeds`:

```sql
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_schema = 'shared'
  AND table_name IN ('ref_school_holidays','ref_public_holidays','ref_major_events');
-- expect 3 rows

SELECT COUNT(*) FROM shared.ref_school_holidays WHERE end_date < CURRENT_DATE;
SELECT COUNT(*) FROM shared.ref_public_holidays WHERE observed_date < CURRENT_DATE;
SELECT COUNT(*) FROM shared.ref_major_events WHERE end_date < CURRENT_DATE;
-- all three must return 0 after forward-only load
```

**Reload cleanup (if a prior load had past rows):**

```sql
DELETE FROM shared.ref_school_holidays WHERE end_date < CURRENT_DATE;
DELETE FROM shared.ref_public_holidays WHERE observed_date < CURRENT_DATE;
DELETE FROM shared.ref_major_events WHERE end_date < CURRENT_DATE;
```

Paste coverage outputs into PR / ticket as needed:

```sql
SELECT state_code, year, COUNT(*) FROM shared.ref_school_holidays GROUP BY 1,2 ORDER BY 1,2;
SELECT state_code, year, COUNT(*) FROM shared.ref_public_holidays GROUP BY 1,2 ORDER BY 1,2;
SELECT event_type, COUNT(*) FROM shared.ref_major_events GROUP BY 1;
```

## 7. Coverage gaps

- **Public holidays:** Seed is a minimal forward subset, not a full Fair Work extract per state/year.
- **TAS:** No summer row for the 2027–28 break — DECYP page did not publish 2028 term starts at verification time, so end of that summer was not fixed from primary material.
- **Major events:** Small curated set; many high-traffic events intentionally omitted until v1.1.

## 8. Git state

- Branch: `feature/busy-index-v1`
- Latest commit: `a60ff83`
- Remote: `https://github.com/Milestoneigroup/data-builder.git`
- Push: **done** — `feature/busy-index-v1` tracking `origin/feature/busy-index-v1`

## 9. Open question for Richard

Should **v1.1** add **Western NSW division** rows (or a `division` column) so destination matching can pick Eastern vs Western school calendars?

## 10. Next move

1. **v1.1** — Expand public-holiday coverage (Fair Work 2027 page + state mirrors), extend `ref_major_events`, consider materialised view if `v_busy_signal_daily` is hot.
2. **v2** — Commercial demand/pricing layer (per original scope parking lot).
3. **Ship v1** — Wire read models in product; keep loader manual on a seasonal cadence until refresh automation lands.
