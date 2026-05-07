# Railway — Places gap-fill worker

Standalone service that lives **alongside** `monthly_snapshot` and `directory_enrichment_service_a`. Do **not** change those artefacts when touching this scraper bundle.

## Steps

1. Open the Milestoneigroup Railway project dashboard.
2. **Add Service** → connect the same GitHub repository as existing scrapers.
3. Under service settings → **Railway Config File**, point to `scrapers/places_gap_fill/railway.json`, or mirror the equivalent path in Railway’s UI builder.
4. Ensure the deployment **root directory** resolves to this repository checkout (the `requirements.txt` path is assumed at repo root relative to checkout).
5. Add environment variables (production):
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `GOOGLE_PLACES_API_KEY`
6. Deploy. The template start command invokes `python -m scrapers.places_gap_fill.run_places_gap_fill --schedule`, which wakes on calendar day **15** at **02:00 UTC**, runs `vendor_type=all` with a USD **50.00** cap per invocation, and honours the Tier‑1 augmentation rules outlined in README.
