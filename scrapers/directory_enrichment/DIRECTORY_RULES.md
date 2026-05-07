# Directory Rules — what each scraper needs to know

Reference document for all directory enrichment scrapers. Captures per-directory
quirks discovered during build and operation. Update as new patterns emerge.

## Easy Weddings

- **URL pattern:** `/{TypePath}/{City}/{BusinessSlug}/`
- **Pagination:** `/{Type}/` for page 1, `/{Type}/{n}/` for n>1
- **Listing card selector:** `.supplier-card` with `data-json` attribute
- **Listing pages provide:** name, suburb, state, profile_url, rating,
  review_count, award badge
- **Per-vendor page provides:** website, phone, sometimes social handles
- **Categories:** Standard wedding verticals (WeddingVenues, MarriageCelebrant,
  WeddingPhotography, WeddingCars, WeddingFlowers, WeddingMusic, WeddingPlanner,
  WeddingCakes, WeddingCaterers, WeddingHire)
- **Location confidence:** HIGH — suburb + state reliably populated
- **Ethics:** Polite scrape allowed (respects robots.txt + crawl-delay)
- **Rate limit signal:** Never observed at our scale; monitor for 429
- **State value format:** Full state names (e.g. "New South Wales") — normalise to
  NSW/VIC/etc. via `_framework.normalise_au_state` before matching

## Hello May

- **URL pattern:** `/{type}/{state-slug}/{vendor-slug}/` — STATE LEVEL ONLY
- **Listing pages provide:** name (from anchor text), profile URL
- **Per-vendor page provides:** website, **social handles in plain text** (no
  enquiry-form gate) — THIS IS THE KEY VALUE FROM HELLO MAY
- **Categories:** Includes Hello May-specific buckets like 'destination-wedding',
  'luxe-stays' that don't exist on Easy Weddings
- **Sub-category capture:** Stored in `hello_may_category` column for future
  segmentation
- **Location confidence:** LOW — state only, no suburb. ACCEPTABLE — augment
  via Google Places later. Do NOT engineer suburb extraction from page content.
- **Ethics:** Polite scrape allowed; lower volume site than Easy Weddings,
  lighter touch
- **Special handling:** Reject `l.instagram.com` redirect links (Hello May's
  own tracker, not the vendor's actual Instagram)
- **State slug mapping:** new-south-wales → NSW, victoria → VIC, queensland → QLD,
  western-australia → WA, south-australia → SA, tasmania → TAS,
  australian-capital-territory → ACT, northern-territory → NT, international → null

## Wedshed

- **URL pattern:** `/planning/vendors/{slug}` OR `/planning/venues/{slug}` —
  flat namespace, no regional/category hierarchy in URL
- **Search functionality:** Doesn't generate fresh URLs (search results don't
  create indexable pages)
- **ETHICS:** HEAD-CHECK ONLY — Wedshed is a potential partner. No content
  scraping under any circumstances.
- **Strategy:** Sitemap-first for URL discovery; HEAD-check for liveness only.
- **Slug pattern:** `{name-lower-hyphenated}` sometimes with location suffix
  (e.g. `days-like-these-photography-sydney-nsw`)
- **Location confidence:** N/A — we never load page content

## Wedlockers

- **URL patterns:**
  - Per-vendor profile (canonical): `/b/{business-slug}`
  - Geographic: `/{type-vendor}/{region-slug}/` (aggregator, not individual)
  - Listing aggregator: `/listing/{type}` (aggregator, not individual)
- **ETHICS:** HEAD-CHECK ONLY — Wedlockers is a potential partner. No content
  scraping.
- **Strategy:** HEAD-check `/b/{constructed-slug}` only.

## AG Register

- **STATUS:** DONE — do not re-visit
- 11,238 celebrants already loaded as foundation extract
- One-shot Playwright crawl from earlier work; not part of recurring enrichment

---

## Universal scraping discipline (all directories)

- **User-Agent:** `MilestoneDataBuilder/1.0 (+https://milestonei.com.au; directory-enrichment)`
- **Accept-Language:** `en-AU,en;q=0.9`
- **Referer:** `https://www.google.com.au/`
- **Polite delay:** 2-3 seconds between requests, random jitter
- **Robots.txt:** Fetch once per session per domain, cache, honour crawl-delay
- **Retry:** Exponential backoff on 429/503 (max 3 attempts)
- **Hard stop:** If a directory returns sustained 403/429, stop scraping that
  directory; do NOT escalate to harder evasion (no fake_useragent, no proxies,
  no captcha solving)
- **Logging:** Every request logged with URL, status, fuzzy score on match,
  action taken (augment/insert/skip)

## Augmentation rules (per Schema Convention v1.2)

- NEVER overwrite curated fields (name, state, suburb, postcode, region, address,
  website, phone, email, social handles)
- Match logic: exact (name + state) → augment NULL fields; fuzzy ≥85 (name +
  state) → augment NULL fields; no match → INSERT with synthetic ID
- Synthetic ID format: `{PREFIX}-{DIR_CODE}-{6-digit-seq}` (VEN-EWDIR-000001,
  PHO-HMDIR-000001, etc.)

## Incremental philosophy

- This is INCREMENTAL — multiple scrape runs over time will tighten data
- Missing data on first pass is acceptable
- Augment-on-each-pass: each run only fills NULL fields; doesn't overwrite
- If parser misses something (e.g. Hello May suburb), Google Places enrichment
  or other sources can fill the gap later
- Never delete vendor rows. New rows added; existing rows augmented.

---

## Document history

| Version | Date | Change |
|---|---|---|
| v1.0 | 7 May 2026 | Initial rules document — 4 active directories + AG Register |
