-- SILO gridded weather: grid cells, daily observations, monthly climatology (shared schema).
-- DataDrill CSV (SILO) — CC BY 4.0; attribute in product surfaces.

CREATE OR REPLACE FUNCTION shared.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := timezone('utc', now());
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS shared.ref_weather_grid_cells (
  grid_cell_id text PRIMARY KEY,
  silo_lat numeric(6, 4) NOT NULL,
  silo_lng numeric(7, 4) NOT NULL,
  requested_lat numeric(8, 6) NOT NULL,
  requested_lng numeric(9, 6) NOT NULL,
  coverage_label text,
  is_active boolean NOT NULL DEFAULT true,
  first_observed_date date,
  last_observed_date date,
  total_observations integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  updated_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_weather_grid_cells_silo_unique UNIQUE (silo_lat, silo_lng)
);

DROP TRIGGER IF EXISTS trg_ref_weather_grid_cells_updated_at ON shared.ref_weather_grid_cells;
CREATE TRIGGER trg_ref_weather_grid_cells_updated_at
  BEFORE UPDATE ON shared.ref_weather_grid_cells
  FOR EACH ROW
  EXECUTE PROCEDURE shared.set_updated_at();

COMMENT ON TABLE shared.ref_weather_grid_cells IS
  'SILO DataDrill grid centroid per upserted cell; grid_cell_id derived from SILO lat/lng.';

CREATE TABLE IF NOT EXISTS shared.ref_weather_daily (
  weather_daily_id bigserial PRIMARY KEY,
  grid_cell_id text NOT NULL
    REFERENCES shared.ref_weather_grid_cells (grid_cell_id) ON DELETE CASCADE,
  observation_date date NOT NULL,
  daily_rain_mm numeric(6, 2),
  temp_max_c numeric(4, 1),
  temp_min_c numeric(4, 1),
  humidity_pct numeric(5, 2),
  data_source text NOT NULL DEFAULT 'silo',
  ingested_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_weather_daily_cell_date UNIQUE (grid_cell_id, observation_date)
);

CREATE INDEX IF NOT EXISTS idx_ref_weather_daily_cell_date_desc
  ON shared.ref_weather_daily (grid_cell_id, observation_date DESC);
CREATE INDEX IF NOT EXISTS idx_ref_weather_daily_observation_date
  ON shared.ref_weather_daily (observation_date);

COMMENT ON TABLE shared.ref_weather_daily IS
  'Daily SILO DataDrill variables ingested from CSV (rain, max/min temp, rh at tmax).';

CREATE TABLE IF NOT EXISTS shared.ref_weather_monthly_stats (
  monthly_stats_id bigserial PRIMARY KEY,
  grid_cell_id text NOT NULL
    REFERENCES shared.ref_weather_grid_cells (grid_cell_id) ON DELETE CASCADE,
  month_of_year smallint NOT NULL,
  years_in_sample smallint NOT NULL,
  avg_rainfall_mm numeric(6, 2),
  median_rainfall_mm numeric(6, 2),
  rain_days_avg numeric(4, 1),
  avg_temp_max_c numeric(4, 1),
  avg_temp_min_c numeric(4, 1),
  avg_humidity_pct numeric(5, 2),
  risk_rating text,
  last_refreshed_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_weather_monthly_stats_month_chk
    CHECK (month_of_year >= 1 AND month_of_year <= 12),
  CONSTRAINT ref_weather_monthly_stats_risk_chk
    CHECK (risk_rating IS NULL OR risk_rating IN ('low', 'medium', 'high', 'very_high')),
  CONSTRAINT ref_weather_monthly_stats_cell_month UNIQUE (grid_cell_id, month_of_year)
);

COMMENT ON TABLE shared.ref_weather_monthly_stats IS
  'Per calendar month climatology from ref_weather_daily (rain-day counts > 1 mm).';

ALTER TABLE shared.ref_destinations
  ADD COLUMN IF NOT EXISTS grid_cell_id text
    REFERENCES shared.ref_weather_grid_cells (grid_cell_id);

COMMENT ON COLUMN shared.ref_destinations.grid_cell_id IS
  'Optional SILO grid cell; populated after destination dedup (separate script).';

-- RLS
ALTER TABLE shared.ref_weather_grid_cells ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "weather_grid_cells_read" ON shared.ref_weather_grid_cells;
DROP POLICY IF EXISTS "weather_grid_cells_write" ON shared.ref_weather_grid_cells;
CREATE POLICY "weather_grid_cells_read" ON shared.ref_weather_grid_cells
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "weather_grid_cells_write" ON shared.ref_weather_grid_cells
  FOR ALL TO service_role USING (true) WITH CHECK (true);

ALTER TABLE shared.ref_weather_daily ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "weather_daily_read" ON shared.ref_weather_daily;
DROP POLICY IF EXISTS "weather_daily_write" ON shared.ref_weather_daily;
CREATE POLICY "weather_daily_read" ON shared.ref_weather_daily
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "weather_daily_write" ON shared.ref_weather_daily
  FOR ALL TO service_role USING (true) WITH CHECK (true);

ALTER TABLE shared.ref_weather_monthly_stats ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "weather_monthly_stats_read" ON shared.ref_weather_monthly_stats;
DROP POLICY IF EXISTS "weather_monthly_stats_write" ON shared.ref_weather_monthly_stats;
CREATE POLICY "weather_monthly_stats_read" ON shared.ref_weather_monthly_stats
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "weather_monthly_stats_write" ON shared.ref_weather_monthly_stats
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_weather_grid_cells TO anon;
GRANT SELECT ON shared.ref_weather_grid_cells TO authenticated;
GRANT ALL ON shared.ref_weather_grid_cells TO service_role;

GRANT SELECT ON shared.ref_weather_daily TO anon;
GRANT SELECT ON shared.ref_weather_daily TO authenticated;
GRANT ALL ON shared.ref_weather_daily TO service_role;

GRANT SELECT ON shared.ref_weather_monthly_stats TO anon;
GRANT SELECT ON shared.ref_weather_monthly_stats TO authenticated;
GRANT ALL ON shared.ref_weather_monthly_stats TO service_role;

CREATE OR REPLACE VIEW shared.v_rain_predictor_monthly AS
SELECT
  d.destination_id,
  d.destination_slug,
  d.destination_name,
  d.state_code,
  d.lat,
  d.lng,
  m.month_of_year,
  m.avg_rainfall_mm,
  m.median_rainfall_mm,
  m.rain_days_avg,
  m.avg_temp_max_c,
  m.avg_temp_min_c,
  m.avg_humidity_pct,
  m.risk_rating,
  m.years_in_sample
FROM shared.ref_destinations d
JOIN shared.ref_weather_grid_cells g ON g.grid_cell_id = d.grid_cell_id
JOIN shared.ref_weather_monthly_stats m ON m.grid_cell_id = g.grid_cell_id
WHERE g.is_active = true;

GRANT SELECT ON shared.v_rain_predictor_monthly TO anon;
GRANT SELECT ON shared.v_rain_predictor_monthly TO authenticated;
GRANT SELECT ON shared.v_rain_predictor_monthly TO service_role;
