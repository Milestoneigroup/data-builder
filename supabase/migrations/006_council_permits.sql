-- shared.ref_council_permits
-- Structured council wedding/event permit metadata by destination.

CREATE TABLE IF NOT EXISTS shared.ref_council_permits (
  destination_id text PRIMARY KEY
    REFERENCES shared.ref_destinations (destination_id) ON DELETE CASCADE,
  council_name text,
  state_code text,
  permit_page_url text,
  permit_required boolean,
  permit_fee_aud text,
  permit_lead_time_days integer,
  max_guests_outdoor integer,
  approved_locations text[] NOT NULL DEFAULT '{}',
  restricted_times text,
  insurance_required boolean,
  insurance_min_cover_aud text,
  alcohol_permitted boolean,
  caterers_approved_list boolean,
  contact_name text,
  contact_email text,
  contact_phone text,
  application_url text,
  application_form_url text,
  notes text,
  scraped_date date,
  data_confidence text
    CHECK (data_confidence IN ('high', 'medium', 'low') OR data_confidence IS NULL),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE shared.ref_council_permits IS
  'Council permit metadata for outdoor wedding/event approvals by destination.';

CREATE INDEX IF NOT EXISTS idx_ref_council_permits_state
  ON shared.ref_council_permits (state_code);
CREATE INDEX IF NOT EXISTS idx_ref_council_permits_scraped_date
  ON shared.ref_council_permits (scraped_date);
CREATE INDEX IF NOT EXISTS idx_ref_council_permits_confidence
  ON shared.ref_council_permits (data_confidence);

ALTER TABLE shared.ref_council_permits ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "council_permits_read_anon" ON shared.ref_council_permits;
DROP POLICY IF EXISTS "council_permits_read_auth" ON shared.ref_council_permits;
DROP POLICY IF EXISTS "council_permits_write_service" ON shared.ref_council_permits;

CREATE POLICY "council_permits_read_anon" ON shared.ref_council_permits
  FOR SELECT TO anon USING (true);
CREATE POLICY "council_permits_read_auth" ON shared.ref_council_permits
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "council_permits_write_service" ON shared.ref_council_permits
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_council_permits TO anon;
GRANT SELECT ON shared.ref_council_permits TO authenticated;
GRANT ALL ON shared.ref_council_permits TO service_role;
