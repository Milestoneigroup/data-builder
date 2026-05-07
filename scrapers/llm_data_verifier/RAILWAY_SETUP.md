# Railway: `llm-data-verifier` service

## Prerequisites

- Branch `feature/llm-data-verifier` pushed to `Milestoneigroup/data-builder`.
- `master_*_enriched.csv` files available at runtime (see below).

## Service setup

1. In the `distinguished-gentleness` Railway project, create a new service **llm-data-verifier**.
2. Connect GitHub repository **Milestoneigroup/data-builder**.
3. Set the deployment branch to **`feature/llm-data-verifier`**.
4. Point Railway at config-as-code: **`scrapers/llm_data_verifier/railway.json`** (or set an equivalent build/deploy override to use that Dockerfile).
5. **Environment variables** (mirror `env.local` on your machine):
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`  
   The verifier uses these **only** for optional read-only name lookup — **no writes**.
6. **Inputs**: choose one approach before first production run:
   - Temporarily allow the enriched CSVs under `scrapers/llm_data_verifier/inputs/` on this branch (override `.gitignore` locally and push — **sensitive**), **or**
   - Attach a Railway volume and copy files in, **or**
   - Bake files into a private image (not recommended for PII).
7. Deploy. The service is configured with **`restartPolicyType`: `NEVER`** — it runs once and exits (`0` on success).

## Retrieving outputs

After the run, open the deployment logs. Each artefact is emitted between:

```text
===BEGIN OUTPUT FILE <filename>===
<base64>
===END OUTPUT FILE <filename>===
```

Decode locally, for example on macOS or Linux:

```bash
cat block.txt | base64 -d > master_photographers_VERIFIED.csv
```

On Windows (PowerShell), pipe the base64 string through `[Convert]::FromBase64String` or use WSL.

## Docker note

`env.local` is **not** copied into the image (it is gitignored). Configure secrets only via Railway environment variables.
