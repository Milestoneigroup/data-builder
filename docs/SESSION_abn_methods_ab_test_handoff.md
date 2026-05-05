# Session handoff — ABN enrichment A/B test (40 curated venues)

**Branch:** `feature/abn-ab-test`  
**Repos touched:** `Milestoneigroup/data-builder` only (no `milestone-platform` changes).  
**Execution:** Sequential — Method B (ABR) then Method A (website), both with `TEST_VENUE_IDS` from `tmp/test_abn_venue_ids.env`.

## Incident recovery (pre-flight deviation)

1. **Accidental broad website scrape:** An initial Method A run started without `TEST_VENUE_IDS` set (the env file path was missing on disk) and processed the full queue before being terminated. **Recovery:** Reset `abn_scrape_attempted_at` for the 46 non-test rows that had been stamped `2026-05-05` (UTC date from the scraper); no `abn_from_website` values had been written for those rows.  
2. **DB check constraint:** `venues_abn_name_search_confidence` originally rejected values we needed for honest reporting. **Change:** Extended the constraint to allow `NULL` semantics and added `api_error` via migration `venues_abn_name_search_classification_api_error` (repo copy: `supabase/migrations/013_venues_abn_name_search_confidence_api_error.sql`).  
3. **“No records found” vs API fault:** ABR returns this as an XML `<exception>` in some cases. The scraper maps that to **`no_match`**, not `api_error`.

## Architectural decisions vs brief

| Topic | Decision |
| --- | --- |
| **Shared checksum** | `scrapers/abn_util.py` centralises mod-89 so both methods stay aligned. |
| **Method B endpoint** | `ABRSearchByNameAdvancedSimpleProtocol2017` HTTP form POST to `https://abr.business.gov.au/abrxmlsearch/AbrXmlSearch.asmx/ABRSearchByNameAdvancedSimpleProtocol2017` — supports `activeABNsOnly`, `businessName`, `maxSearchResults`, state scoping. Docs: [Forms page](https://abr.business.gov.au/abrxmlsearch/Forms/ABRSearchByNameAdvancedSimpleProtocol2017.aspx) and [Web services overview](https://abr.business.gov.au/Documentation/WebServiceMethods). |
| **Method B confidence strings** | Map heuristics onto existing enum values (`exact_name_match`, `strong_state_match`, `fuzzy`, `multiple_candidates`, `no_match`, `api_error`) to satisfy `venues_abn_name_search_confidence_check`. |
| **Method A writes** | Only `abn_from_website`, `abn_website_source_url`, `abn_scrape_attempted_at` (no verified `abn`, no legacy `abn_lookup_*` / `abn_website_confidence` columns). Internal scrape tiers (`HIGH` / `MEDIUM`) remain log-only. |
| **ABN checksum reference** | ABR’s published weighting (subtract 1 from first digit; weights `10,1,3,5,7,9,11,13,15,17,19`; sum divisible by 89) per [AbnFormat help](https://abr.business.gov.au/Help/AbnFormat). |

## Headline results (n = 40)

| Method | Metric | Count | % |
| --- | --- | ---: | ---: |
| **A — Website** | Valid ABN extracted | 9 | 22.5% |
|  | No valid ABN surfaced | 28 | 70.0% |
|  | Blocked / transport sentinels | 3 | 7.5% (1 Cloudflare-style block, 2 off-domain hijacks) |
| **B — ABR name** | Stored ABN (`exact_name_match` + `strong_state_match` + `fuzzy`) | 21 | 52.5% |
|  | `exact_name_match` | 19 | 47.5% |
|  | `strong_state_match` | 0 | 0% |
|  | `fuzzy` | 2 | 5.0% |
|  | `multiple_candidates` (no ABN stored) | 2 | 5.0% |
|  | `no_match` | 17 | 42.5% |
|  | `api_error` | 0 | 0% |

### Confusion matrix (same 40 venues)

| Segment | Count |
| --- | ---: |
| Both methods agree (same ABN) | 4 |
| Both found, different ABN | 2 |
| Method A only | 3 |
| Method B only | 15 |
| Both missed | 16 |

**Agreement venues:** Old Parliament House (ACT), Coriole Vineyards (SA), All Saints Estate (VIC), Collingwood Childrens Farm (VIC).

**Disagreement venues (both non-null, different ABN):**

1. **COMO The Treasury (WA)** — Website scraped the global `comohotels.com` contact/legal stack (`61416216590`); ABR resolved a different operating entity (`23167213407`). Classic parent/regional operating company split.  
2. **Cable Beach Club Resort (WA)** — Website privacy policy exposed `63008698708` while ABR’s best match (`51653549396`) reflects another entity in the resort/ownership stack.

**Method A–only successes (website yes, ABR null/ambiguous):** Australian Museum (`85407224698`), Coronation Hall (`80690785443` via council site), All Saints Chapel Hamilton Island (`61009946909` via Hamilton Island umbrella site).

## Per-state hit counts (test cohort)

| State | Method A hits | Method B hits | Venues |
| --- | ---: | ---: | ---: |
| ACT | 1 | 3 | 3 |
| NSW | 2 | 2 | 10 |
| NT | 0 | 1 | 3 |
| QLD | 1 | 3 | 5 |
| SA | 1 | 2 | 4 |
| TAS | 0 | 2 | 4 |
| VIC | 2 | 4 | 6 |
| WA | 2 | 4 | 5 |

## Categorised misses

### Method A — `no_valid_abn` / structural failures (31 outcome rows)

Common themes observed in logs:

- Marketing homepages without any checksum-valid 11-digit ABN in crawl budget (hotels, large attractions).  
- Venues whose `website` seed redirects to a marketplace / directory / unrelated registrable domain (`off_domain_redirect`).  
- Cloudflare / bot challenges on luxury hotel chains (`blocked`).  
- Government-managed or multi-tenant domains where ABN lives on a parent department page not linked within the crawl frontier.

### Method B — `no_match`

Frequently tied to **listing labels that are not legal trading names** (e.g. shortened marketing names, “Precinct”, “Resort” fragments) or **strict state filters** when the venue record’s postcode/state does not align with ABR’s primary business address.

### Method B — `multiple_candidates`

- **Bendooley Estate** — Six high-scoring businesses at the same postcode (wine, hospitality, property vehicles).  
- **Australian Museum** — Multiple similarly named cultural entities; scorer refused to auto-pick.

## ABR operations & politeness notes

- **Throughput:** Test stayed at 40 calls; scraper paces `~1.5s` between requests. For scale (~750 venues), consult the official **ActivityReport** tool and latest registration wording for any published daily ceiling; historical community guidance often cites ~5k GUID invocations/day — re-verify before batch automation.  
- **Guideline:** [Web services registration / limitations](https://abr.business.gov.au/Documentation/UserGuideWebRegistration.aspx).

## Railway / command triggers

Set `TEST_VENUE_IDS` to the comma-separated list (or rely on `NULL` queue columns for full runs).

```bash
cd /app # Railway service root with repo checked out
export $(grep -v '^#' env.local | xargs) # however you inject secrets; never print them
export TEST_VENUE_IDS="$(cat tmp/test_abn_venue_ids.env)"
python -m scrapers.abr_name_search_for_venues
python -m scrapers.scrape_venue_websites_for_abn
```

> Railway’s UI for one-off jobs changes frequently — prefer a **one-off `python` command** attached to the existing `data-builder` service with the same env vars as production.

## Recommendations for scaling to ~750 venues

1. **Lead with Method B** for coverage and cost predictability, but expect meaningful `no_match` and `multiple_candidates` volumes when marketing names drift from legal names.  
2. **Use Method A as precision / trust copy** when a human-readable cite is required (“ABN disclosed on `/privacy`”). Budget for **Cloudflare failures** on premium hotel domains; plan for Playwright only where simple HTTP/HTML fails at scale (brief threshold >30% failures).  
3. **Disagreement protocol:** When both methods return different valid ABNs, treat as **manual review with entity graph context** (parent operator vs venue-specific trust).  
4. **Queue hygiene:** Always assert `TEST_VENUE_IDS` is non-empty in CI or wrapper scripts before running destructive-width scrapes.  
5. **Future improvement:** Add optional second-pass ABR queries using the **website ABN as `SearchByABNv202001`** anchor to disambiguate `multiple_candidates` without guessing trading names.

## Comparison SQL (repeatable)

```sql
WITH test AS (
  SELECT unnest(array[<your forty uuid literals>]::uuid[]) AS id
)
SELECT
  count(*) FILTER (
    WHERE v.abn_from_website IS NOT NULL
      AND v.abn_from_name_search IS NOT NULL
      AND v.abn_from_website = v.abn_from_name_search
  ) AS both_agree,
  count(*) FILTER (
    WHERE v.abn_from_website IS NOT NULL
      AND v.abn_from_name_search IS NOT NULL
      AND v.abn_from_website <> v.abn_from_name_search
  ) AS both_disagree,
  count(*) FILTER (
    WHERE v.abn_from_website IS NOT NULL AND v.abn_from_name_search IS NULL
  ) AS a_only,
  count(*) FILTER (
    WHERE v.abn_from_website IS NULL AND v.abn_from_name_search IS NOT NULL
  ) AS b_only,
  count(*) FILTER (
    WHERE v.abn_from_website IS NULL AND v.abn_from_name_search IS NULL
  ) AS both_miss
FROM public.venues v
JOIN test t ON t.id = v.id;
```
