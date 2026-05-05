-- Bushfire historical activity: NASA FIRMS VIIRS S-NPP (SP) per destination × month-of-year.
-- Migration 016.

CREATE OR REPLACE FUNCTION shared.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := timezone('utc', now());
  RETURN NEW;
END;
$$;

CREATE TABLE shared.ref_destination_fire_activity_monthly (
  fire_activity_id bigserial PRIMARY KEY,
  destination_id text NOT NULL
    REFERENCES shared.ref_destinations (destination_id) ON DELETE CASCADE,
  month_of_year smallint NOT NULL,
  years_in_sample smallint NOT NULL,
  avg_hotspots_per_year_10km numeric(6, 1),
  avg_hotspots_per_year_25km numeric(6, 1),
  avg_hotspots_per_year_50km numeric(6, 1),
  max_month_hotspots_25km integer,
  years_with_significant_activity_25km smallint,
  relative_risk_label text,
  absolute_risk_label text,
  data_source text NOT NULL DEFAULT 'NASA FIRMS VIIRS_SNPP',
  data_period_start date NOT NULL,
  data_period_end date NOT NULL,
  last_refreshed_at timestamptz DEFAULT timezone('utc', now()),
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  updated_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_destination_fire_activity_monthly_month_chk
    CHECK (month_of_year BETWEEN 1 AND 12),
  CONSTRAINT ref_destination_fire_activity_monthly_relative_chk
    CHECK (
      relative_risk_label IS NULL
      OR relative_risk_label IN ('lowest', 'low', 'medium', 'high', 'peak')
    ),
  CONSTRAINT ref_destination_fire_activity_monthly_absolute_chk
    CHECK (
      absolute_risk_label IS NULL
      OR absolute_risk_label IN ('low', 'medium', 'high', 'extreme')
    ),
  CONSTRAINT ref_destination_fire_activity_monthly_dest_month_unique UNIQUE (destination_id, month_of_year)
);

CREATE INDEX idx_ref_destination_fire_activity_monthly_destination_id
  ON shared.ref_destination_fire_activity_monthly (destination_id);
CREATE INDEX idx_ref_destination_fire_activity_monthly_relative_risk
  ON shared.ref_destination_fire_activity_monthly (relative_risk_label);
CREATE INDEX idx_ref_destination_fire_activity_monthly_absolute_risk
  ON shared.ref_destination_fire_activity_monthly (absolute_risk_label);

DROP TRIGGER IF EXISTS trg_ref_destination_fire_activity_monthly_updated_at
  ON shared.ref_destination_fire_activity_monthly;
CREATE TRIGGER trg_ref_destination_fire_activity_monthly_updated_at
  BEFORE UPDATE ON shared.ref_destination_fire_activity_monthly
  FOR EACH ROW
  EXECUTE PROCEDURE shared.set_updated_at();

COMMENT ON TABLE shared.ref_destination_fire_activity_monthly IS
  'Historical VIIRS hotspot density by calendar month within 10/25/50 km of each destination (NASA FIRMS archive).';

ALTER TABLE shared.ref_destinations
  ADD COLUMN IF NOT EXISTS peak_fire_months text;
ALTER TABLE shared.ref_destinations
  ADD COLUMN IF NOT EXISTS lowest_fire_months text;
ALTER TABLE shared.ref_destinations
  ADD COLUMN IF NOT EXISTS fire_activity_data_period_end date;

COMMENT ON COLUMN shared.ref_destinations.peak_fire_months IS
  'Denormalised: two highest relative-risk calendar months (e.g. Jan, Feb).';
COMMENT ON COLUMN shared.ref_destinations.lowest_fire_months IS
  'Denormalised: two lowest relative-risk calendar months.';
COMMENT ON COLUMN shared.ref_destinations.fire_activity_data_period_end IS
  'End date of NASA FIRMS window used for fire_activity aggregates.';

ALTER TABLE shared.ref_destination_fire_activity_monthly ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS fire_activity_read ON shared.ref_destination_fire_activity_monthly;
DROP POLICY IF EXISTS fire_activity_write ON shared.ref_destination_fire_activity_monthly;
DROP POLICY IF EXISTS "fire_activity_read" ON shared.ref_destination_fire_activity_monthly;
DROP POLICY IF EXISTS "fire_activity_write" ON shared.ref_destination_fire_activity_monthly;

CREATE POLICY fire_activity_read ON shared.ref_destination_fire_activity_monthly
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY fire_activity_write ON shared.ref_destination_fire_activity_monthly
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_destination_fire_activity_monthly TO anon, authenticated;
GRANT ALL ON shared.ref_destination_fire_activity_monthly TO service_role;

GRANT SELECT, USAGE ON SEQUENCE shared.ref_destination_fire_activity_monthly_fire_activity_id_seq TO service_role;

CREATE OR REPLACE VIEW shared.v_destination_fire_risk_by_month AS
SELECT
  d.destination_id,
  d.destination_slug,
  d.destination_name,
  d.state_code,
  d.lat,
  d.lng,
  d.peak_fire_months,
  d.lowest_fire_months,
  f.month_of_year,
  f.avg_hotspots_per_year_25km AS hotspots_25km_avg,
  f.avg_hotspots_per_year_50km AS hotspots_50km_avg,
  f.max_month_hotspots_25km,
  f.years_with_significant_activity_25km,
  f.relative_risk_label,
  f.absolute_risk_label,
  f.years_in_sample,
  f.data_period_start,
  f.data_period_end
FROM shared.ref_destinations d
LEFT JOIN shared.ref_destination_fire_activity_monthly f ON f.destination_id = d.destination_id
WHERE d.is_active = true
ORDER BY d.destination_id, f.month_of_year;

GRANT SELECT ON shared.v_destination_fire_risk_by_month TO anon, authenticated, service_role;
