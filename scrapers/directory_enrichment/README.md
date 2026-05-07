# Directory enrichment — Service A

Append-only enrichment from **Easy Weddings** and **Hello May** into ``public.venues``, ``public.photographers``, and ``public.celebrants``.  
This worker is intentionally separate from the monthly_snapshot Railway service — different process, schedules, and risk profile.

## What it does

- Scrapes Easy Weddings listing pages for ``WeddingVenues``, ``MarriageCelebrant``, and ``WeddingPhotography`` (pagination matches ``celebrant_active_enrichment``: ``/{Type}/{n}/`` for page ``n > 1``).
- Scrapes Hello May ``/directory/{type}/{state}/`` listings (Sub-A types: venues, photographers, cinematographers, celebrant) and follows profile URLs for vendor website + social anchors.
- **Match**: exact name + state, then fuzzy (``token_sort_ratio`` ≥ 85) within the same state.
- **Augment**: only fills **currently NULL** fields on matched rows — never overwrites curated name, suburb, structured contact, etc.
- **Insert**: unmatched vendors get a deterministic synthetic ``source_directory_synthetic_id`` (e.g. ``VEN-EWDIR-000042``, ``PHO-HMDIR-000012``).

## Schema

Apply ``_migrations/001_listing_seen_at.sql`` in the Supabase SQL Editor (Richard: before first live scrape in each environment):

- ``*_listing_seen_at`` timestamps per directory
- ``source_directory_synthetic_id`` anchor for synthetic keys

Canonical directory columns already on all three vendor tables:

- Easy Weddings: ``easy_weddings_*``, ``easy_weddings_listing_seen_at``
- Hello May: ``hello_may_*``, ``hello_may_listing_seen_at``
- ``last_directory_check_at``

**Note:** ``public.venues`` and ``public.celebrants`` use ``data_source`` as the lineage field; ``public.photographers`` uses ``data_source_primary`` (matches loaders). Synthetic rows set ``is_active_market=true`` and ``active_signal_sources='{directory}_listing_2026_05'``.

## Local run

From repo root (``env.local`` with ``SUPABASE_URL`` + ``SUPABASE_SERVICE_ROLE_KEY``):

```powershell
cd C:\Users\richa\dev\Milestoneigroup\data-builder
python -m scrapers.directory_enrichment.run_directory_enrichment --directory easy_weddings --vendor-type venues --limit 50
```

Dry run (parses directories; UPDATE/INSERT suppressed to logging only):

```powershell
python -m scrapers.directory_enrichment.run_directory_enrichment --directory easy_weddings --vendor-type venues --limit 50 --dry-run
```

One-shot jobs default to a **30-minute** ceiling (``--max-runtime-seconds``). Do **not** run full sweeps without explicit approval — use modest ``--limit`` first.

### CLI reference

| Argument | Meaning |
|---------|---------|
| ``--directory`` | ``easy_weddings`` \| ``hello_may`` \| ``all`` |
| ``--vendor-type`` | ``venues`` \| ``photographers`` \| ``celebrants`` \| ``all`` |
| ``--limit`` | Max profiles to touch per run |
| ``--dry-run`` | Skip Supabase writes |
| ``--start-page`` | Easy Weddings pagination start (default ``1``) |
| ``--schedule`` | Railway mode: BlockingScheduler daemon |
| ``--max-runtime-seconds`` | Soft cap for one-shot runs (ignored with ``--schedule``) |

## Polite scraping

- 2–3 s jittered delay via ``time.sleep`` between hits.
- Branded UA and Australian headers via ``_framework.RAILWAY_DIRECTORY_HEADERS``.
- ``robots.txt`` checked per-origin per session cache.
- Retry on HTTP 429/503 with capped exponential backoff (3 tries).

Logs: ``logs/directory_enrichment_{timestamp}.log``.

## Verification SQL (Richard)

Coverage by directory:

```sql
SELECT 'venues' AS tbl,
       count(*) FILTER (WHERE easy_weddings_url IS NOT NULL) AS ew,
       count(*) FILTER (WHERE hello_may_url IS NOT NULL) AS hm
FROM public.venues
UNION ALL SELECT 'celebrants', ...,
UNION ALL SELECT 'photographers', ...;
```

Synthetic rows:

```sql
SELECT count(*) FROM public.venues WHERE source_directory_synthetic_id IS NOT NULL;
```

Augmentation sanity:

```sql
SELECT count(*) FROM public.venues WHERE name IS NULL;  -- expect 0
```

## Railway

See ``RAILWAY_SETUP.md``. **Do not modify** the repo root ``railway.json`` (monthly_snapshot); this service uses ``scrapers/directory_enrichment/railway.json`` only.
