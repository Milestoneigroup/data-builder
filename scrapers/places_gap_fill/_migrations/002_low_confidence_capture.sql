-- Tier 1 migration 002 — capture low-confidence website matches
-- Apply via Supabase MCP (apply_migration) or SQL editor.

ALTER TABLE public.venues
  ADD COLUMN IF NOT EXISTS website_from_google_low_confidence boolean DEFAULT false;

ALTER TABLE public.celebrants
  ADD COLUMN IF NOT EXISTS website_from_google_low_confidence boolean DEFAULT false;

ALTER TABLE public.photographers
  ADD COLUMN IF NOT EXISTS website_from_google_low_confidence boolean DEFAULT false;
