-- Tier 1 migration — richer Google Places capture
-- Universal additions across all three vendor tables
-- Apply via Supabase SQL Editor before running the gap-fill worker.

ALTER TABLE public.venues
  ADD COLUMN IF NOT EXISTS lat numeric,
  ADD COLUMN IF NOT EXISTS lng numeric,
  ADD COLUMN IF NOT EXISTS business_status text,
  ADD COLUMN IF NOT EXISTS google_address text,
  ADD COLUMN IF NOT EXISTS google_address_components_json jsonb,
  ADD COLUMN IF NOT EXISTS google_types_json jsonb,
  ADD COLUMN IF NOT EXISTS google_editorial_summary text,
  ADD COLUMN IF NOT EXISTS google_phone text,
  ADD COLUMN IF NOT EXISTS opening_hours_json jsonb;

ALTER TABLE public.celebrants
  ADD COLUMN IF NOT EXISTS lat numeric,
  ADD COLUMN IF NOT EXISTS lng numeric,
  ADD COLUMN IF NOT EXISTS business_status text,
  ADD COLUMN IF NOT EXISTS google_address_components_json jsonb,
  ADD COLUMN IF NOT EXISTS google_editorial_summary text;

ALTER TABLE public.photographers
  ADD COLUMN IF NOT EXISTS lat numeric,
  ADD COLUMN IF NOT EXISTS lng numeric,
  ADD COLUMN IF NOT EXISTS business_status text,
  ADD COLUMN IF NOT EXISTS google_address_components_json jsonb,
  ADD COLUMN IF NOT EXISTS google_editorial_summary text;
