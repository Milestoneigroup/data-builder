# Railway — Directory enrichment Service A

This mirrors the Milestone monthly_snapshot pattern but stays **physically isolated**: do **not** change the repo root ``railway.json``.

## Steps for Richard

1. Open the Milestone Railway project alongside the existing monthly_snapshot service.
2. Click **New service** → **GitHub Repo** → select the same ``data-builder`` repository.
3. After the service is created, open **Settings → Build / Deploy** (or Railway’s service config UI) and set the **Railway Config file path** (or equivalent) to:
   ```text
   scrapers/directory_enrichment/railway.json
   ```
4. Add environment variables (**same credentials** as monthly_snapshot):
   - ``SUPABASE_URL``
   - ``SUPABASE_SERVICE_ROLE_KEY``
5. Deploy. The start command runs ``run_directory_enrichment --schedule``.

## Schedule (UTC)

- **Monday 02:00** — Easy Weddings, all vendor types.
- **Tuesday 02:00** — Hello May, all vendor types.

Configured in Python via APScheduler (`BlockingScheduler`, ``pytz.utc``).

## Operational notes

- Apply ``scrapers/directory_enrichment/_migrations/001_listing_seen_at.sql`` in Supabase SQL Editor on each fresh environment **before** the worker writes data.
- Use one-shot CLI runs locally before promoting broad limits in Railway.
- Keep polite delays and backoff — rate limits trigger hard failures if scraped aggressively.
