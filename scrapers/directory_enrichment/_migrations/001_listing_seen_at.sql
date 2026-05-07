-- Directory enrichment Service A — listing timestamps + synthetic ID anchor.
-- Apply in Supabase SQL Editor before running directory enrichment scrapers.
-- Idempotent: safe to re-run.

ALTER TABLE public.venues
  ADD COLUMN IF NOT EXISTS easy_weddings_listing_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS hello_may_listing_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS source_directory_synthetic_id text;

ALTER TABLE public.celebrants
  ADD COLUMN IF NOT EXISTS easy_weddings_listing_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS hello_may_listing_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS source_directory_synthetic_id text;

ALTER TABLE public.photographers
  ADD COLUMN IF NOT EXISTS easy_weddings_listing_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS hello_may_listing_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS source_directory_synthetic_id text;
