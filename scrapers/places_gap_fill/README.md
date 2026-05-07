# Places gap-fill (Tier 1 richness)

Australian English operator notes throughout.

Railway-hosted worker (`scrapers/places_gap_fill/run_places_gap_fill.py`) that **only hydrates gaps** — Text Search (`$0.032` nominal per call) followed by enriched Place Details (`$0.017` nominal planning rate used for budgeting).

## Difference vs `monthly_snapshot`

`snapshot`-style pipelines capture rolling venue signals (reviews, AQ, pollen, Claude sentiment snapshots, etc.). This worker is a **steady-state backlog filler** targeting vendors still missing authoritative Google identifiers and rich-but-static factual fields (`lat/lng`, `business_status`, address components JSON, editorial blurbs, venues-only reviews + structured opening hours blob).

Nothing in `scrapers/monthly_snapshot.py`, `scrapers/places_enrichment.py`, nor `scrapers/directory_enrichment/*` is mutated by this service.

## Governance

- Env loaded from repo-root `env.local` via `load_dotenv(..., override=True)` in `_framework.py`, matching sibling scrapers.
- **Augmentation discipline:** curated identity/contact columns (`name`, curated `website`, curated `phone`, `email`, social handles wherever present) remain untouched unless the dedicated Google-ingest siblings (`website_from_google`, etc.) permit NULL‑only enrichment.
- **Budget:** `BudgetTracker` halts cleanly before breaches; scheduled runs intentionally cap USD spend at **50.00**.
- Delay between outbound calls: **200 ms**.
- Matcher: `thefuzz.token_sort_ratio` must meet **≥ 0.70** post-normalisation versus the Places listing display name — otherwise `"low confidence"` is logged without writes.

### Rich capture matrices

**Universal everywhere:** identity (`google_*` mirrors), geographic (`lat/lng`, formatted address lineage in `google_address` + `google_address_components_json`), `business_status`, `google_types_json`, `google_editorial_summary`, telecom + Maps links.

**Venues-only:** merges `regularOpeningHours`/`currentOpeningHours` into JSONB `opening_hours_json`; five Places reviews hydrate `review_text_n`/`review_author_n`/`review_rating_n`/`review_date_n` (matching `scrapers/places_enrichment.py` nomenclature — see DECISIONS if your verification SQL mentions an alternate shorthand).

Celebrants/person suppliers skip reviews + opening blobs because they are irrelevant and would double-charge SKU variety.

### Cohort scale (counts as-of 07 May 2026 briefing)

| Cohort                                    | Estimated backlog |
| ----------------------------------------- | ----------------: |
| Venues lacking `place_id`                 |               627 |
| Active celebrants without Google linkage¹ |               470 |
| Photographers without `google_place_id`   |               451 |

¹ Active + directory URL sentinel logic described in `_query_builder.py` / DECISIONS.

## Local invocation

PowerShell snippet (repo root assumed `C:\Users\richa\dev\Milestoneigroup\data-builder`):

```powershell
cd C:\Users\richa\dev\Milestoneigroup\data-builder
python -m scrapers.places_gap_fill.run_places_gap_fill --vendor-type venues --limit 5 --max-budget-usd 1.0
python -m scrapers.places_gap_fill.run_places_gap_fill --vendor-type venues --limit 3 --dry-run
python -m scrapers.places_gap_fill.run_places_gap_fill --vendor-type celebrants --limit 3 --max-budget-usd 1.0
python -m scrapers.places_gap_fill.run_places_gap_fill --vendor-type photographers --limit 3 --max-budget-usd 1.0
```

**Before any live enrichment:** paste `_migrations/001_richer_places_capture.sql` inside the Supabase SQL editor and execute; confirm `information_schema.columns` reports `venues.lat`.

## Scheduling mode

Pass `--schedule` (Railway default) — APScheduler blocks on UTC cron `15th / 02:00`, sweeps `--vendor-type all`, `--limit 9999`, USD cap **50.00**.
