# LLM data verifier

Live HTTP verification for multi-LLM-enriched photographer and celebrant CSVs. The tool **does not write to Supabase** (read-only REST lookup is optional when a row has no usable business name).

## Inputs

Place:

- `master_photographers_enriched.csv`
- `master_celebrants_enriched.csv`

in `scrapers/llm_data_verifier/inputs/` (gitignored).

Expected columns include (names are tolerant — see `verify_vendors.py` for fallbacks):

- `vendor_id` (or `celebrant_id` / `photographer_id`)
- `merge_tier` — `HIGH`, `MEDIUM`, `YELLOW`, `RED` (verification order)
- `website_found`, `instagram_found`, `facebook_found`
- `notes_combined` — `Trading name: …` regex for display name
- `trading_name_known`, `business_name_known`
- Email / phone in any of: `email_found`, `email`, `primary_email`, … / `phone_found`, `phone`, …

## Outputs

Under `scrapers/llm_data_verifier/outputs/` (gitignored):

- `master_photographers_VERIFIED_<ISO_DATE>.csv`
- `master_celebrants_VERIFIED_<ISO_DATE>.csv`
- `verification_summary_<ISO_DATE>.md`

New columns include live-check results (`website_alive`, `instagram_appears_real`, `website_links_to_instagram`, …) plus `verification_score` and `verification_tier`.

## Environment

- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` — optional name fallback via PostgREST **GET** only.
- Optional: `VERIFIER_PHOTOGRAPHERS_TABLE`, `VERIFIER_PHOTOGRAPHERS_ID_COLUMN`.

Loads `env.local` from the repo root when present (`load_dotenv(..., override=True)`).

## Local run

```bash
cd /path/to/data-builder
python -m scrapers.llm_data_verifier.run_verifier \
  --vendor-type photographers \
  --max-vendors 10 \
  --no-base64-emit
```

- Polite delays: **1.5–3.0 s** before every HTTP GET, plus the same distribution between website / Instagram / Facebook steps.
- **No** cookies, sessions, or authenticated scrapes on Meta properties.
- **403** hosts: recorded as “could not verify”, host added to the skip set when using `--skip-rejected-host` (automatically augmented after HTTP 403).

## CLI flags

| Flag | Meaning |
|------|--------|
| `--input-dir` | Default: `scrapers/llm_data_verifier/inputs` |
| `--output-dir` | Default: `scrapers/llm_data_verifier/outputs` |
| `--vendor-type` | `photographers`, `celebrants`, or `all` |
| `--max-vendors` | Per-vertical limit (`0` = unlimited). With `all`, each vertical may process up to this many rows. |
| `--start-from` | Skip rows until this `vendor_id` is seen, then skip that row and continue (resume after an interruption). |
| `--skip-rejected-host` | Comma-separated hostnames to skip. |
| `--no-base64-emit` | Suppress log-sized base64 blocks (recommended locally). |

## Railway

See `RAILWAY_SETUP.md`. Image build uses `scrapers/llm_data_verifier/Dockerfile`; config is `scrapers/llm_data_verifier/railway.json`.

Successful runs print **base64-wrapped** output files to stdout for copy/paste recovery (decode with `base64 -d` on Unix or equivalent on Windows).

## Guardrails

- Do not bypass polite delays.
- Do not authenticate to Instagram or Facebook.
- Do **not** treat **403** as proof a handle is fake — only as **unverified**.
