-- shared.ref_tourism_weekend_content — wedding weekend guest intelligence (one row per org)
-- Apply after 001/002; requires shared.ref_tourism_organisations.
-- RLS: anon/auth SELECT; service_role ALL.

CREATE TABLE IF NOT EXISTS shared.ref_tourism_weekend_content (
  org_id text PRIMARY KEY
    REFERENCES shared.ref_tourism_organisations (org_id) ON DELETE CASCADE,
  state_code text NOT NULL,
  region_name text NOT NULL,

  things_to_do_rainy_day text[] NOT NULL DEFAULT '{}',
  things_to_do_outdoor text[] NOT NULL DEFAULT '{}',
  things_to_do_couples text[] NOT NULL DEFAULT '{}',
  things_to_do_groups text[] NOT NULL DEFAULT '{}',

  accommodation_types text[] NOT NULL DEFAULT '{}',
  accommodation_price_range text NOT NULL DEFAULT '',
  accommodation_booking_url text NOT NULL DEFAULT '',

  nearest_airport text NOT NULL DEFAULT '',
  airport_distance_note text NOT NULL DEFAULT '',
  transport_options text[] NOT NULL DEFAULT '{}',

  notable_restaurants text[] NOT NULL DEFAULT '{}',
  wineries_breweries text[] NOT NULL DEFAULT '{}',
  local_produce text NOT NULL DEFAULT '',

  top_attractions text[] NOT NULL DEFAULT '{}',

  things_to_do_url text NOT NULL DEFAULT '',
  accommodation_url text NOT NULL DEFAULT '',
  transport_url text NOT NULL DEFAULT '',

  scraped_date date NOT NULL,
  data_confidence text NOT NULL DEFAULT 'unknown',
  source_urls text[] NOT NULL DEFAULT '{}',
  updated_at date NOT NULL DEFAULT (CURRENT_DATE)
);

COMMENT ON TABLE shared.ref_tourism_weekend_content IS
  'Structured wedding-weekend context for guest planning (Section B of tourism intelligence).';

CREATE INDEX IF NOT EXISTS idx_tourism_weekend_state
  ON shared.ref_tourism_weekend_content (state_code);

ALTER TABLE shared.ref_tourism_weekend_content ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "tourism_weekend_read_anon" ON shared.ref_tourism_weekend_content;
DROP POLICY IF EXISTS "tourism_weekend_read_auth" ON shared.ref_tourism_weekend_content;
DROP POLICY IF EXISTS "tourism_weekend_write_service" ON shared.ref_tourism_weekend_content;
CREATE POLICY "tourism_weekend_read_anon" ON shared.ref_tourism_weekend_content
  FOR SELECT TO anon USING (true);
CREATE POLICY "tourism_weekend_read_auth" ON shared.ref_tourism_weekend_content
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "tourism_weekend_write_service" ON shared.ref_tourism_weekend_content
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_tourism_weekend_content TO anon;
GRANT SELECT ON shared.ref_tourism_weekend_content TO authenticated;
GRANT ALL ON shared.ref_tourism_weekend_content TO service_role;
