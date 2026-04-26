-- shared.ref_tourism_listings — categorised extract listings from tourism org sites (Section C)
-- Requires shared.ref_tourism_organisations.

CREATE SEQUENCE IF NOT EXISTS shared.ref_tourism_listings_seq;

CREATE OR REPLACE FUNCTION shared.default_tourism_listing_id()
RETURNS text
LANGUAGE sql
VOLATILE
AS $$
  SELECT 'TLIST-' || lpad(nextval('shared.ref_tourism_listings_seq')::text, 6, '0');
$$;

CREATE TABLE IF NOT EXISTS shared.ref_tourism_listings (
  listing_id text PRIMARY KEY DEFAULT shared.default_tourism_listing_id(),
  org_id text NOT NULL
    REFERENCES shared.ref_tourism_organisations (org_id) ON DELETE CASCADE,
  listing_name text NOT NULL,
  listing_type text NOT NULL
    CHECK (listing_type IN (
      'wedding_venue', 'accommodation', 'restaurant', 'winery_brewery',
      'activity', 'attraction', 'transport', 'supplier', 'other'
    )),
  address text NOT NULL DEFAULT '',
  suburb text NOT NULL DEFAULT '',
  postcode text NOT NULL DEFAULT '',
  state text NOT NULL DEFAULT '',
  website text NOT NULL DEFAULT '',
  phone text NOT NULL DEFAULT '',
  description text NOT NULL DEFAULT '',
  is_wedding_relevant boolean NOT NULL DEFAULT false,
  tags text[] NOT NULL DEFAULT '{}',
  source_page_url text NOT NULL DEFAULT '',
  scraped_date date NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE shared.ref_tourism_listings IS
  'Categorised listings scraped from STOT sites (Section C; manual QA expected).';

CREATE INDEX IF NOT EXISTS idx_tourism_listings_org
  ON shared.ref_tourism_listings (org_id);
CREATE INDEX IF NOT EXISTS idx_tourism_listings_type
  ON shared.ref_tourism_listings (listing_type);
CREATE INDEX IF NOT EXISTS idx_tourism_listings_wedding
  ON shared.ref_tourism_listings (is_wedding_relevant);

ALTER TABLE shared.ref_tourism_listings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "tourism_listings_read_anon" ON shared.ref_tourism_listings;
DROP POLICY IF EXISTS "tourism_listings_read_auth" ON shared.ref_tourism_listings;
DROP POLICY IF EXISTS "tourism_listings_write_service" ON shared.ref_tourism_listings;
CREATE POLICY "tourism_listings_read_anon" ON shared.ref_tourism_listings
  FOR SELECT TO anon USING (true);
CREATE POLICY "tourism_listings_read_auth" ON shared.ref_tourism_listings
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "tourism_listings_write_service" ON shared.ref_tourism_listings
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT, USAGE ON SEQUENCE shared.ref_tourism_listings_seq TO service_role;
GRANT USAGE ON SEQUENCE shared.ref_tourism_listings_seq TO authenticated;
GRANT SELECT ON shared.ref_tourism_listings TO anon;
GRANT SELECT ON shared.ref_tourism_listings TO authenticated;
GRANT ALL ON shared.ref_tourism_listings TO service_role;
