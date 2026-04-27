# Busy Index — Major Events Seed Expansion (handoff)

## Summary

Expanded `data/seed_major_events_AU.json` to **38** forward-dated, source-linked major events (target band 35–50). Loader `python -m scrapers.load_busy_index_seeds` was executed successfully against Supabase (`ref_major_events` upsert).

## Final event count

| Metric | Value |
| --- | ---: |
| Active major events in `shared.ref_major_events` | **38** |

## Coverage by `state_code`

| State | Count |
| --- | ---: |
| ACT | 2 |
| NSW | 8 |
| NT | 2 |
| QLD | 8 |
| SA | 6 |
| TAS | 3 |
| VIC | 5 |
| WA | 4 |

## Coverage by `event_type`

| `event_type` | Count |
| --- | ---: |
| `sport` | 15 |
| `festival` | 10 |
| `arts_culture` | 7 |
| `music` | 4 |
| `food_wine` | 1 |
| `seasonal` | 1 |

## Pre-ship / validation (executed)

- **Forward-only (`end_date < CURRENT_DATE`)**: **PASS** — `0` rows among active seeds at verification time (`2026-04-27` local).
- **Sydney NYE (`v_busy_signal_daily`, NSW, `2026-12-31`)**: **PASS** — `major_events_count = 1` (was `0` in v1 thin seed).
- **Easter NSW window (`2027-03-26`–`2027-03-29`)**: **PASS** — at least one day with `major_events_count >= 1` (Easter long-weekend seasonal row aligned to NSW Government 2027 gazetted Easter dates).
- **AFL Grand Final VIC window (`2026-09-25`–`2026-09-27`)**: **PASS** — max `major_events_count >= 1` on `2026-09-26` (Grand Final row).
- **Splendour exclusion**: **PASS** — `COUNT(*) WHERE event_slug ILIKE '%splendour%'` → `0`.

## Events excluded (with reason)

| Event | Reason |
| --- | --- |
| **Splendour in the Grass** | Official site messaging indicates an extended pause / no confirmed forward return window suitable for a verified `start_date`/`end_date` pair; excluded per hiatus-style rule. |
| **Bluesfest Byron Bay** | Official `bluesfest.com.au` states **2026 is not proceeding** (cancellation / liquidation context); no verified forward festival dates to seed. |
| **Mona Foma** | Official `monafoma.net.au` documents the festival’s conclusion; no forward program. |
| **Enlighten Festival (ACT)** | Official `enlightencanberra.com.au` confirms **2026** ran **27 Feb – 9 Mar 2026** (already past relative to seed cutoff **2026-04-27**); **2027 dates not published** on the official site at verification time. |
| **National Multicultural Festival (ACT)** | **2026** festival dates were Feb **2026** (past relative to cutoff); **2027** dates not published on `multiculturalfestival.com.au` at verification time. |
| **Sydney Festival (next edition)** | No official **2027** program window published on `sydneyfestival.org.au` at verification time (site still oriented to the concluded **2026** season). |
| **Melbourne International Comedy Festival (next edition)** | **2026** season concluded; **2027** dates not published on `comedyfestival.com.au` at verification time. |
| **Formula 1 Australian Grand Prix (next edition)** | Could not lock a **single official** `grandprix.com.au` / F1 page that states the next Melbourne race **calendar date** with the certainty required by the forward-only rule during this session. |
| **White Night Melbourne** | Could not reach a definitive **current** official program page with verified **future** dated window during this session (sources were inconclusive / stale). |
| **Cairns Festival** | Official pages used for verification were **blocked (403)** / **timed out** from the fetch environment; excluded rather than guess. |
| **Margaret River Pro (WSL)** | Official WSL event URL **errored (500)** during verification; excluded. |
| **Mindil Beach Sunset Markets** | Seasonal market series without a single clean official “season window” URL suitable for this seed schema in the time available; excluded. |

## One-year-only / “TBA next year” notes

- **Sydney Mardi Gras 2027**: **Parade date** is published (`2027-03-06`); the broader **2027 festival** pages still read as “dropping soon” — wider festival window may need a follow-up seed row once official dates are posted.
- **Sculpture by the Sea, Cottesloe 2027**: Official home page includes a “please check with our office prior to making travel plans” caveat alongside published dates.

## Commit

`9415791bba4f93e09cb765afea6990db5c8aa25f`
