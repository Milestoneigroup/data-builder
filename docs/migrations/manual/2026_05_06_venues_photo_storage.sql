-- Manual schema change (record only — apply via Supabase Studio SQL editor).
-- Venue photo persistence: augment public.venues with storage URLs after Places photo download.
-- Do not overwrite existing enrichment columns.

ALTER TABLE public.venues
  ADD COLUMN IF NOT EXISTS photo_storage_urls jsonb,
  ADD COLUMN IF NOT EXISTS photos_downloaded_at timestamptz,
  ADD COLUMN IF NOT EXISTS photos_download_error text;

COMMENT ON COLUMN public.venues.photo_storage_urls IS
  'Array of Supabase Storage public URLs after photo download. NULL = not yet downloaded.';

COMMENT ON COLUMN public.venues.photos_downloaded_at IS
  'Timestamp of last successful photo download for this venue.';

COMMENT ON COLUMN public.venues.photos_download_error IS
  'Last error encountered downloading photos for this venue, if any.';

-- AUGMENTATION-ONLY: never overwrite existing fields.
-- Existing photo_ref_* and total_photo_count columns are UNTOUCHED by this DDL.
