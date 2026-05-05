# Session 1 — Venue website ABN scraper (test batch)

## Pre-flight note (important)

On Richard’s machine the **`data-builder` working tree was not clean** (WIP on `feature/bushfire-historical-activity`). Before branching from `main`, those changes were **stashed** as:

`git stash list` → **`pre-abn-website-scraper WIP (bushfire branch)`**

Restore with:

`git stash pop`

when ready to continue that work.

---

## What shipped

| Deliverable | Path / action |
|-------------|----------------|
| Scraper module | `scrapers/scrape_venue_websites_for_abn.py` |
| Shared checksum helper | `scrapers/abn_util.py` (ABR mod-89; cite [ABR ABN format](https://www.abr.business.gov.au/HelpAbnFormat.aspx)) |
| DB migration (Session 1 confidence column) | `supabase/migrations/012_venues_abn_website_confidence.sql` — **applied** to project `cxifxnsbaknjwtlstsly` |

**Why a new column:** `abn_lookup_confidence` has a Postgres `CHECK` allowing only API-oriented values (`exact_abn`, `strong_match`, etc.). HIGH/MEDIUM website heuristics cannot be stored there. Session 1 uses `abn_website_confidence` instead and leaves `abn_lookup_confidence` for Session 2 ABR output.

---

## Test run results (40 UUIDs, local agent run on 2026-05-06)

Totals from scraper `Summary` line (matches Supabase spot-check: `with_abn = 9`, `blocked = 1` on the test id set).

| Metric | Count |
|--------|------:|
| **ABN found (checksum-valid)** | **9** (**22.5%**) |
| no valid ABN on crawled pages | 28 |
| Site blocked / challenge (Railway-class detection) | 1 |
| Homepage `registrable_domain` redirect off venue cluster | 2 |
| `no_website` (no HTTP seed after field choice) | 0 |
| **Invalid-checksum candidates seen** (not stored) | **6** (aggregate across runs) |

**Where ABNs were found (page-level bucket in logs):**

- `contact/about`: 4  
- `terms/privacy/legal`: 3  
- `home`: 2  

---

## Venues with stored `abn_from_website` (9)

Checksum-valid only; **Session 2 must confirm legal entity == venue** (especially council / island operator / museum).

1. Old Parliament House — source: policy/reporting page on `moadoph.gov.au`  
2. Australian Museum — `australian.museum` (home)  
3. Coronation Hall Newtown — `bayside.nsw.gov.au` (likely **council** ABN, not a private hall trust)  
4. All Saints Chapel Hamilton Island — `hamiltonisland.com.au` privacy (operator ABN)  
5. Coriole Vineyards — `coriole.com` contact  
6. All Saints Estate — `allsaintswine.com.au` terms  
7. Collingwood Childrens Farm — `farm.org.au` contact  
8. COMO The Treasury — `comohotels.com` contact (**MEDIUM** confidence — parent hotel group)  
9. Cable Beach Club — `cablebeachclub.com` privacy  

**Blocked (1):** Park Hyatt Sydney — Cloudflare-style challenge on `hyatt.com` homepage.

**Off-domain homepage redirects (2):** Bawley Bush Retreat (`bawleybushretreat.com.au` → `ohwow.directory`), Kingsford The Barossa (`kingsfordbarossa.com.au` → `worldsapart.club`). Treated as scrape miss; no storage beyond `abn_scrape_attempted_at` (and no `abn_lookup_source` for these — only `blocked_railway` sets `abn_lookup_source`).

---

## Misses — themes (28 no_valid_abn + 2 off_domain + 1 blocked)

- **No ABN in footer/terms/contact** on operator domains (common for SMB marketing sites).  
- **Large / franchised / precinct** sites with group legal elsewhere (MONA, Darwin Waterfront, Customs House, etc.).  
- **TLS / server quirks:** occasional `peer closed connection` / `forcibly closed` on subpages (Pialligo, Peppers) — we skip the page and continue; can end as no match.  
- **Data model:** many rows use `website_from_google` as the real crawl target when `website` is a marketplace URL — Wedshed listings no longer pollute results.  
- **Chain hotels:** one Hyatt hit Cloudflare; others may need different IP or manual follow-up.

---

## Architectural decisions (vs brief / discovered constraints)

1. **`abn_website_confidence` + migration 012** — unavoidable given `venues_abn_lookup_confidence_check`.  
2. **Marketplace seed override** — if `website` is on `wedshed.com.au` / `easyweddings.com.au` / `hitched.com.au`, prefer `website_from_google` when present so we scrape the operator domain.  
3. **Registrable-domain clustering** — treat subdomains of the same `*.com.au` bundle as one site; exclude cross-domain links so marketplace terms pages are not used.  
4. **Redirect guards** — (a) reject homepage that jumps to a different registrable domain than the seed URL; (b) skip inner fetches whose final URL leaves the seed domain (stops `/terms` → third-party legal).  
5. **`abn_scrape_attempted_at` is type `DATE`** in Supabase — we store UTC calendar date only (coarser than brief’s “attempted at” wording).  
6. **Blocked marker** — `abn_lookup_source = 'blocked_railway'` on challenge/blocked responses; other outcomes do not set lookup source (website vs API separation).

---

## Session 2 recommendations

1. **ABR XML lookup** for the 9 stored ABNs first (cheap, high precision); then name-search remaining venues with **strict entity + state + address** scoring.  
2. **Disambiguation rules** for government / council / island operator / museum rows — surface `abn_entity_legal_name` vs marketing name in UI.  
3. **Expand marketplace domain list** as new listing partners appear.  
4. **Retry policy** for venues with `blocked_railway` or TLS flake — optional local / residential IP re-run after Session 2 bulk API pass.  
5. Consider **`abn_scrape_attempted_at` → `timestamptz`** if you need ordering multiple passes on one day.

---

## Railway — how Richard should run the 40-venue job

**Prerequisite:** deploy this branch (or `main` after merge) so the image includes `scrapers/scrape_venue_websites_for_abn.py` and `012` is already applied on Supabase (migration was applied in this session).

1. **Dashboard:** project *distinguished-gentleness* → **data-builder** service → **Variables**.  
2. Add **`TEST_VENUE_IDS`** = one comma-separated line of the 40 UUIDs (no spaces; same list as the brief).  
3. **Deploy** the service if needed so the new code is live.  
4. **Run in the container (recommended):**  
   - Install CLI: `npm i -g @railway/cli` (or `npx @railway/cli`).  
   - `railway login` → `railway link` (select project + **data-builder** service).  
   - **One-shot:** `railway ssh -- python -m scrapers.scrape_venue_websites_for_abn`  
   - ([Railway SSH](https://docs.railway.com/cli/ssh) runs a command inside the deployed service; env vars from the dashboard, including `TEST_VENUE_IDS`, are available there.)  
5. **Logs:** service → **Deployments / Logs** — confirm the `Summary:` line and spot-check a few `FOUND` rows.  
6. **Cleanup:** remove or blank **`TEST_VENUE_IDS`** after the run so future deploys do not accidentally scope to the test list.

**Note:** `railway run` (without SSH) executes **locally** with env injected from Railway — useful for debugging, but it does **not** reproduce Railway egress IP behaviour.

---

## Final commit

Recorded after push:

`git rev-parse HEAD` on branch `feature/abn-website-scraper` → **(fill when pushed)**

---

## Working tree

Expected **clean** at commit time (`git status`).
