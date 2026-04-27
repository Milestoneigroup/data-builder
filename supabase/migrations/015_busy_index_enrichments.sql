-- Busy Index enrichments (Migration 015): TRA-aligned accommodation pressure, destination→TRA mapping,
-- event proximity, and derived calendar views (overlap, long weekends, combined monthly signal).
-- Depends on shared.ref_destinations, ref_school_holidays, ref_public_holidays, ref_major_events (014).

-- ---------------------------------------------------------------------------
-- Table A: monthly accommodation pressure (aggregated month-of-year per TR)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS shared.ref_accommodation_pressure_monthly (
  accommodation_pressure_id bigserial PRIMARY KEY,
  tra_region_code text NOT NULL,
  tra_region_name text NOT NULL,
  state_code text NOT NULL,
  month_of_year smallint NOT NULL,
  years_in_sample smallint NOT NULL,
  avg_occupancy_pct numeric(5, 2),
  avg_adr_aud numeric(7, 2),
  avg_revpar_aud numeric(7, 2),
  peak_month_for_region boolean,
  relative_pressure_label text,
  data_source text NOT NULL DEFAULT 'Tourism Research Australia STAR',
  data_period_start date NOT NULL,
  data_period_end date NOT NULL,
  last_refreshed_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  updated_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_accommodation_pressure_monthly_month_chk
    CHECK (month_of_year >= 1 AND month_of_year <= 12),
  CONSTRAINT ref_accommodation_pressure_monthly_relative_chk
    CHECK (
      relative_pressure_label IS NULL
      OR relative_pressure_label IN ('lowest', 'low', 'medium', 'high', 'peak')
    ),
  CONSTRAINT ref_accommodation_pressure_monthly_tra_month_unique UNIQUE (tra_region_code, month_of_year)
);

CREATE INDEX IF NOT EXISTS idx_ref_accommodation_pressure_monthly_state_code
  ON shared.ref_accommodation_pressure_monthly (state_code);
CREATE INDEX IF NOT EXISTS idx_ref_accommodation_pressure_monthly_relative_label
  ON shared.ref_accommodation_pressure_monthly (relative_pressure_label);

DROP TRIGGER IF EXISTS trg_ref_accommodation_pressure_monthly_updated_at
  ON shared.ref_accommodation_pressure_monthly;
CREATE TRIGGER trg_ref_accommodation_pressure_monthly_updated_at
  BEFORE UPDATE ON shared.ref_accommodation_pressure_monthly
  FOR EACH ROW
  EXECUTE PROCEDURE shared.set_updated_at();

COMMENT ON TABLE shared.ref_accommodation_pressure_monthly IS
  'Month-of-year accommodation demand signals by ASGS Tourism Region (TRA STAR / ABS STA aligned).';

-- ---------------------------------------------------------------------------
-- Table B: destination → TRA / ASGS tourism region (many-to-one)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS shared.ref_destination_to_tra_region (
  destination_id text NOT NULL
    REFERENCES shared.ref_destinations (destination_id) ON DELETE CASCADE,
  tra_region_code text NOT NULL,
  mapping_confidence text NOT NULL
    CHECK (mapping_confidence IN ('exact', 'strong', 'approximate')),
  mapping_notes text,
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  PRIMARY KEY (destination_id)
);

COMMENT ON TABLE shared.ref_destination_to_tra_region IS
  'Maps each wedding destination cluster to a single ASGS Tourism Region (coarser than destination).';

-- ---------------------------------------------------------------------------
-- Table C: destination ↔ major event proximity (haversine bands)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS shared.ref_destination_event_proximity (
  proximity_id bigserial PRIMARY KEY,
  destination_id text NOT NULL
    REFERENCES shared.ref_destinations (destination_id) ON DELETE CASCADE,
  major_event_id bigint NOT NULL
    REFERENCES shared.ref_major_events (major_event_id) ON DELETE CASCADE,
  distance_km numeric(6, 2) NOT NULL,
  proximity_band text NOT NULL
    CHECK (proximity_band IN ('within_50km', 'within_100km', 'within_200km')),
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_destination_event_proximity_dest_event_unique UNIQUE (destination_id, major_event_id)
);

CREATE INDEX IF NOT EXISTS idx_ref_destination_event_proximity_destination_id
  ON shared.ref_destination_event_proximity (destination_id);
CREATE INDEX IF NOT EXISTS idx_ref_destination_event_proximity_major_event_id
  ON shared.ref_destination_event_proximity (major_event_id);

-- ---------------------------------------------------------------------------
-- Views
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW shared.v_school_holiday_overlap_daily AS
SELECT
  d.observation_date,
  COUNT(DISTINCT sh.state_code) AS states_on_holiday,
  (
    SELECT COALESCE(array_agg(DISTINCT sh2.state_code ORDER BY sh2.state_code), ARRAY[]::text[])
    FROM shared.ref_school_holidays sh2
    WHERE d.observation_date BETWEEN sh2.start_date AND sh2.end_date
  ) AS states_list,
  CASE
    WHEN COUNT(DISTINCT sh.state_code) = 0 THEN 'none'
    WHEN COUNT(DISTINCT sh.state_code) <= 2 THEN 'low'
    WHEN COUNT(DISTINCT sh.state_code) <= 4 THEN 'medium'
    WHEN COUNT(DISTINCT sh.state_code) <= 6 THEN 'high'
    ELSE 'peak'
  END AS overlap_intensity
FROM (
  SELECT generated_date::date AS observation_date
  FROM generate_series(
    CURRENT_DATE,
    CURRENT_DATE + INTERVAL '24 months',
    INTERVAL '1 day'
  ) AS generated_date
) d
LEFT JOIN shared.ref_school_holidays sh
  ON d.observation_date BETWEEN sh.start_date AND sh.end_date
GROUP BY d.observation_date;

CREATE OR REPLACE VIEW shared.v_long_weekend_windows AS
WITH non_working_days AS (
  SELECT generated_date::date AS d
  FROM generate_series(
    CURRENT_DATE,
    CURRENT_DATE + INTERVAL '24 months',
    INTERVAL '1 day'
  ) AS generated_date
  WHERE
    EXTRACT(DOW FROM generated_date) IN (0, 6)
    OR generated_date::date IN (
      SELECT observed_date
      FROM shared.ref_public_holidays
      WHERE is_national = true
        OR state_code IN ('NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT')
    )
),
runs AS (
  SELECT
    d,
    d - (ROW_NUMBER() OVER (ORDER BY d))::integer AS grp
  FROM non_working_days
)
SELECT
  MIN(d)::date AS window_start,
  MAX(d)::date AS window_end,
  (MAX(d) - MIN(d) + 1)::int AS days_in_window
FROM runs
GROUP BY grp
HAVING (MAX(d) - MIN(d) + 1) >= 3
ORDER BY window_start;

CREATE OR REPLACE VIEW shared.v_destination_busy_signal_monthly AS
SELECT
  d.destination_id,
  d.destination_slug,
  d.destination_name,
  d.state_code,
  m.month_of_year,
  ap.avg_occupancy_pct,
  ap.avg_adr_aud,
  ap.relative_pressure_label AS accommodation_pressure,
  ap.peak_month_for_region,
  (
    SELECT COUNT(*)::bigint
    FROM shared.ref_destination_event_proximity dep
    JOIN shared.ref_major_events e ON e.major_event_id = dep.major_event_id
    WHERE dep.destination_id = d.destination_id
      AND dep.proximity_band = 'within_100km'
      AND EXTRACT(MONTH FROM e.start_date) = m.month_of_year
      AND e.is_active = true
  ) AS nearby_events_in_month
FROM shared.ref_destinations d
CROSS JOIN generate_series(1, 12) AS m(month_of_year)
LEFT JOIN shared.ref_destination_to_tra_region dt ON dt.destination_id = d.destination_id
LEFT JOIN shared.ref_accommodation_pressure_monthly ap
  ON ap.tra_region_code = dt.tra_region_code
  AND ap.month_of_year = m.month_of_year
WHERE d.is_active = true
ORDER BY d.destination_id, m.month_of_year;

-- ---------------------------------------------------------------------------
-- RLS + grants
-- ---------------------------------------------------------------------------

ALTER TABLE shared.ref_accommodation_pressure_monthly ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ref_accommodation_pressure_monthly_read" ON shared.ref_accommodation_pressure_monthly;
DROP POLICY IF EXISTS "ref_accommodation_pressure_monthly_write" ON shared.ref_accommodation_pressure_monthly;
CREATE POLICY "ref_accommodation_pressure_monthly_read" ON shared.ref_accommodation_pressure_monthly
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "ref_accommodation_pressure_monthly_write" ON shared.ref_accommodation_pressure_monthly
  FOR ALL TO service_role USING (true) WITH CHECK (true);

ALTER TABLE shared.ref_destination_to_tra_region ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ref_destination_to_tra_region_read" ON shared.ref_destination_to_tra_region;
DROP POLICY IF EXISTS "ref_destination_to_tra_region_write" ON shared.ref_destination_to_tra_region;
CREATE POLICY "ref_destination_to_tra_region_read" ON shared.ref_destination_to_tra_region
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "ref_destination_to_tra_region_write" ON shared.ref_destination_to_tra_region
  FOR ALL TO service_role USING (true) WITH CHECK (true);

ALTER TABLE shared.ref_destination_event_proximity ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ref_destination_event_proximity_read" ON shared.ref_destination_event_proximity;
DROP POLICY IF EXISTS "ref_destination_event_proximity_write" ON shared.ref_destination_event_proximity;
CREATE POLICY "ref_destination_event_proximity_read" ON shared.ref_destination_event_proximity
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "ref_destination_event_proximity_write" ON shared.ref_destination_event_proximity
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_accommodation_pressure_monthly, shared.ref_destination_to_tra_region, shared.ref_destination_event_proximity TO anon, authenticated;
GRANT ALL ON shared.ref_accommodation_pressure_monthly, shared.ref_destination_to_tra_region, shared.ref_destination_event_proximity TO service_role;
GRANT SELECT, USAGE ON SEQUENCE shared.ref_accommodation_pressure_monthly_accommodation_pressure_id_seq TO service_role;
GRANT SELECT, USAGE ON SEQUENCE shared.ref_destination_event_proximity_proximity_id_seq TO service_role;
GRANT SELECT ON shared.v_school_holiday_overlap_daily, shared.v_long_weekend_windows, shared.v_destination_busy_signal_monthly TO anon, authenticated, service_role;
