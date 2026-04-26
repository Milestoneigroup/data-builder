-- shared.ref_councils

CREATE TABLE IF NOT EXISTS shared.ref_councils (
  council_id text PRIMARY KEY,
  council_name text NOT NULL,
  state_code text,
  website text,
  url_pattern text,
  source_directory text,
  scraped_date date,
  is_active boolean NOT NULL DEFAULT true,
  aligned_destination_ids text[] NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ref_councils_state ON shared.ref_councils (state_code);
CREATE INDEX IF NOT EXISTS idx_ref_councils_active ON shared.ref_councils (is_active);

ALTER TABLE shared.ref_councils ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "councils_read_anon" ON shared.ref_councils;
DROP POLICY IF EXISTS "councils_read_auth" ON shared.ref_councils;
DROP POLICY IF EXISTS "councils_write_service" ON shared.ref_councils;

CREATE POLICY "councils_read_anon" ON shared.ref_councils
  FOR SELECT TO anon USING (true);
CREATE POLICY "councils_read_auth" ON shared.ref_councils
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "councils_write_service" ON shared.ref_councils
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_councils TO anon;
GRANT SELECT ON shared.ref_councils TO authenticated;
GRANT ALL ON shared.ref_councils TO service_role;

CREATE OR REPLACE FUNCTION shared.refresh_ref_councils_alignment()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'shared' AND table_name = 'ref_destinations' AND column_name = 'lga_name'
  ) THEN
    UPDATE shared.ref_councils c
    SET aligned_destination_ids = COALESCE((
      SELECT array_agg(d.destination_id ORDER BY d.destination_id)
      FROM shared.ref_destinations d
      WHERE COALESCE(d.state_code, '') = COALESCE(c.state_code, '')
        AND COALESCE(d.lga_name, '') <> ''
        AND (
          lower(d.lga_name) LIKE ('%' || lower(c.council_name) || '%')
          OR lower(c.council_name) LIKE ('%' || lower(d.lga_name) || '%')
        )
    ), '{}');
  ELSE
    UPDATE shared.ref_councils c
    SET aligned_destination_ids = COALESCE((
      SELECT array_agg(d.destination_id ORDER BY d.destination_id)
      FROM shared.ref_destinations d
      WHERE COALESCE(d.state_code, '') = COALESCE(c.state_code, '')
        AND lower(d.destination_name) LIKE ('%' || split_part(lower(c.council_name), ' council', 1) || '%')
    ), '{}');
  END IF;
END;
$$;
