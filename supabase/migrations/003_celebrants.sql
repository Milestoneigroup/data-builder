-- public.celebrants — Australian marriage celebrants (AG register + merged enrichment)
-- 53 string columns; unset fields use the sentinel 'VERIFY_REQUIRED' (not NULL).

CREATE TABLE IF NOT EXISTS public.celebrants (
  celebrant_id text PRIMARY KEY,
  brand_id text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  full_name text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  ag_display_name text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  title text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  email text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  phone text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  state text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  address_text text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  suburb text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  postcode text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  website text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  registration_date text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  register_class text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  status text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  unavailability_text text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  ceremony_type text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  data_source text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  abia_winner text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  abia_awards_text text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  vibe text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  style_description text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  service_area_notes text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  min_price_aud text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  max_price_aud text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  years_experience text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  estimated_ceremonies text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  languages_non_english text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  instagram_handle_or_url text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  facebook_url text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  phone_from_website text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  celebrant_institute_member text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  joshua_withers_mentioned text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  data_quality_score text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  merge_fuzzy_score text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  is_standalone_award_entry text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  google_place_id text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  google_rating text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  google_review_count text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  website_from_places text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  phone_from_places text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  last_website_enrich_at text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  last_places_enrich_at text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  ag_scrape_page text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  ag_scrape_index text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  import_notes text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  pds_ack text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  insurance_notes text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  public_profile_url text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  linkedin_url text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  raw_address_cell text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  last_updated_source text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  content_tier text NOT NULL DEFAULT 'VERIFY_REQUIRED',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.celebrants IS
  'AU marriage celebrant directory: AG register + ABIA/award merge and enrichments.';

CREATE INDEX IF NOT EXISTS idx_celebrants_state ON public.celebrants (state);
CREATE INDEX IF NOT EXISTS idx_celebrants_data_source ON public.celebrants (data_source);

ALTER TABLE public.celebrants ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "celebrants_read_anon" ON public.celebrants;
DROP POLICY IF EXISTS "celebrants_read_auth" ON public.celebrants;
DROP POLICY IF EXISTS "celebrants_write_service" ON public.celebrants;
CREATE POLICY "celebrants_read_anon" ON public.celebrants
  FOR SELECT TO anon USING (true);
CREATE POLICY "celebrants_read_auth" ON public.celebrants
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "celebrants_write_service" ON public.celebrants
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON public.celebrants TO anon;
GRANT SELECT ON public.celebrants TO authenticated;
GRANT ALL ON public.celebrants TO service_role;
