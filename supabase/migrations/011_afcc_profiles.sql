-- AFCC public directory scrape (quarterly job). RLS: anon read, service_role write.

CREATE TABLE IF NOT EXISTS public.afcc_profiles (
  afcc_slug text PRIMARY KEY,
  full_name text,
  mobile text,
  email text,
  suburb text,
  state text,
  afcc_profile_url text,
  services text,
  summary text,
  testimonial_1 text,
  testimonial_2 text,
  testimonial_3 text,
  website text,
  scraped_date date NOT NULL DEFAULT (timezone('UTC', now())::date),
  matched_celebrant_id text
);

CREATE INDEX IF NOT EXISTS idx_afcc_profiles_scraped_date ON public.afcc_profiles (scraped_date);
CREATE INDEX IF NOT EXISTS idx_afcc_profiles_matched ON public.afcc_profiles (matched_celebrant_id)
  WHERE matched_celebrant_id IS NOT NULL;

COMMENT ON TABLE public.afcc_profiles IS
  'Scraped celebrant profile rows from afcc.com.au/celebrant/{slug}/ (data-builder job).';
COMMENT ON COLUMN public.afcc_profiles.matched_celebrant_id IS
  'Optional link to public.celebrants.celebrant_id after directory merge.';

ALTER TABLE public.afcc_profiles ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "afcc_profiles_read_anon" ON public.afcc_profiles;
DROP POLICY IF EXISTS "afcc_profiles_read_auth" ON public.afcc_profiles;
DROP POLICY IF EXISTS "afcc_profiles_write_service" ON public.afcc_profiles;

CREATE POLICY "afcc_profiles_read_anon" ON public.afcc_profiles
  FOR SELECT TO anon USING (true);
CREATE POLICY "afcc_profiles_read_auth" ON public.afcc_profiles
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "afcc_profiles_write_service" ON public.afcc_profiles
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON public.afcc_profiles TO anon;
GRANT SELECT ON public.afcc_profiles TO authenticated;
GRANT ALL ON public.afcc_profiles TO service_role;
