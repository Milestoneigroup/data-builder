# Places gap-fill — design decisions

## API client

Reuse **Google Places API (New)** over HTTP with `httpx` and field masks (`X-Goog-FieldMask`), identical in spirit to `scrapers/enrich_chain_seed_venues.py`, `scrapers/celebrant_places_supabase.py`, and `scrapers/enrich_photographers_places.py`.

The repo lists `googlemaps` for legacy or other tooling; this service matches the Scrapers Places (New) pattern rather than introducing a parallel Legacy client.

## Schema drift (venues ↔ celebrants/photographers)

**Canonical-rename debt (deferred):** `public.venues` uses `place_id` and `enrichment_date`; `celebrants` and `photographers` use `google_place_id` and `places_enriched_date` (+ `places_enriched_date`-style auditing). Explicit note per **Schema Convention v1.2 Section 8** — do not unify in this workstream.

## Celebrant gap predicate vs `VERIFY_REQUIRED`

`003_celebrants.sql` defined `google_place_id` NOT NULL default `VERIFY_REQUIRED`. Operators often treat “unset” as that sentinel rather than SQL NULL.

The cohort filter therefore uses `(google_place_id IS NULL OR google_place_id = 'VERIFY_REQUIRED')` in addition to the directory URL predicates and **`is_active_market = true`** (active-market cohort).

## Venue review column names

The operator brief uses `review_1_*` in one checklist; Excel + `scrapers/places_enrichment.py` use **`review_text_n`**, `review_author_n`, `review_rating_n`, `review_date_n`. This scraper writes the `review_*` pattern to stay aligned with the wedding-venues workbook and Snapshot naming.

Verification example:

```sql
SELECT review_text_1 IS NOT NULL AS has_reviews FROM public.venues ...
```

## Per-table writable fields

See `_places_client.extract_universal_fields` and `venues.extract_venue_specific_fields` mappings; enrichment-only columns honour **NULL-only** augmentation (never overwrite curated identity: name, address book, curated website/phone/email/social URLs on the curated columns).

`website_from_google` / `website_from_places` are separate ingestion columns — still NULL-only filled.
