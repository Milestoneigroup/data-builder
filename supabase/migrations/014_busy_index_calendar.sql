-- Busy Index v1: school/public holidays + major events (shared schema) + daily signal view.
-- RLS: anon/authenticated SELECT; service_role ALL.

CREATE OR REPLACE FUNCTION shared.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := timezone('utc', now());
  RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS shared.ref_school_holidays (
  school_holiday_id bigserial PRIMARY KEY,
  state_code text NOT NULL CHECK (state_code IN ('NSW','VIC','QLD','SA','WA','TAS','NT','ACT')),
  year smallint NOT NULL,
  term_or_break_label text NOT NULL,
  start_date date NOT NULL,
  end_date date NOT NULL,
  data_source text NOT NULL,
  source_url text,
  verified_at date NOT NULL DEFAULT CURRENT_DATE,
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  updated_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_school_holidays_dates_chk CHECK (end_date >= start_date),
  CONSTRAINT ref_school_holidays_state_year_label UNIQUE (state_code, year, term_or_break_label)
);

CREATE INDEX IF NOT EXISTS idx_ref_school_holidays_dates
  ON shared.ref_school_holidays (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_ref_school_holidays_state_year
  ON shared.ref_school_holidays (state_code, year);

DROP TRIGGER IF EXISTS trg_ref_school_holidays_updated_at ON shared.ref_school_holidays;
CREATE TRIGGER trg_ref_school_holidays_updated_at
  BEFORE UPDATE ON shared.ref_school_holidays
  FOR EACH ROW
  EXECUTE PROCEDURE shared.set_updated_at();

CREATE TABLE IF NOT EXISTS shared.ref_public_holidays (
  public_holiday_id bigserial PRIMARY KEY,
  state_code text NOT NULL,
  year smallint NOT NULL,
  holiday_name text NOT NULL,
  observed_date date NOT NULL,
  is_national boolean NOT NULL DEFAULT false,
  creates_long_weekend boolean,
  data_source text NOT NULL,
  source_url text,
  verified_at date NOT NULL DEFAULT CURRENT_DATE,
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  updated_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_public_holidays_state_year_name UNIQUE (state_code, year, holiday_name)
);

CREATE INDEX IF NOT EXISTS idx_ref_public_holidays_observed_date
  ON shared.ref_public_holidays (observed_date);

DROP TRIGGER IF EXISTS trg_ref_public_holidays_updated_at ON shared.ref_public_holidays;
CREATE TRIGGER trg_ref_public_holidays_updated_at
  BEFORE UPDATE ON shared.ref_public_holidays
  FOR EACH ROW
  EXECUTE PROCEDURE shared.set_updated_at();

CREATE TABLE IF NOT EXISTS shared.ref_major_events (
  major_event_id bigserial PRIMARY KEY,
  event_slug text UNIQUE NOT NULL,
  event_name text NOT NULL,
  event_type text NOT NULL CHECK (event_type IN ('festival','sport','arts_culture','food_wine','music','seasonal','other')),
  state_code text NOT NULL,
  lga_or_area text,
  event_lat numeric(8,6),
  event_lng numeric(9,6),
  start_date date NOT NULL,
  end_date date NOT NULL,
  is_recurring_annual boolean NOT NULL DEFAULT true,
  expected_visitors_label text,
  notes text,
  data_source text NOT NULL,
  source_url text,
  verified_at date NOT NULL DEFAULT CURRENT_DATE,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  updated_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
  CONSTRAINT ref_major_events_dates_chk CHECK (end_date >= start_date)
);

CREATE INDEX IF NOT EXISTS idx_ref_major_events_dates ON shared.ref_major_events (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_ref_major_events_state_type ON shared.ref_major_events (state_code, event_type);
CREATE INDEX IF NOT EXISTS idx_ref_major_events_lat_lng ON shared.ref_major_events (event_lat, event_lng);

DROP TRIGGER IF EXISTS trg_ref_major_events_updated_at ON shared.ref_major_events;
CREATE TRIGGER trg_ref_major_events_updated_at
  BEFORE UPDATE ON shared.ref_major_events
  FOR EACH ROW
  EXECUTE PROCEDURE shared.set_updated_at();

-- RLS (match other shared.ref_* tables)
ALTER TABLE shared.ref_school_holidays ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ref_school_holidays_read" ON shared.ref_school_holidays;
DROP POLICY IF EXISTS "ref_school_holidays_write" ON shared.ref_school_holidays;
CREATE POLICY "ref_school_holidays_read" ON shared.ref_school_holidays
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "ref_school_holidays_write" ON shared.ref_school_holidays
  FOR ALL TO service_role USING (true) WITH CHECK (true);

ALTER TABLE shared.ref_public_holidays ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ref_public_holidays_read" ON shared.ref_public_holidays;
DROP POLICY IF EXISTS "ref_public_holidays_write" ON shared.ref_public_holidays;
CREATE POLICY "ref_public_holidays_read" ON shared.ref_public_holidays
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "ref_public_holidays_write" ON shared.ref_public_holidays
  FOR ALL TO service_role USING (true) WITH CHECK (true);

ALTER TABLE shared.ref_major_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "ref_major_events_read" ON shared.ref_major_events;
DROP POLICY IF EXISTS "ref_major_events_write" ON shared.ref_major_events;
CREATE POLICY "ref_major_events_read" ON shared.ref_major_events
  FOR SELECT TO anon, authenticated USING (true);
CREATE POLICY "ref_major_events_write" ON shared.ref_major_events
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_school_holidays TO anon, authenticated;
GRANT ALL ON shared.ref_school_holidays TO service_role;
GRANT SELECT, USAGE ON SEQUENCE shared.ref_school_holidays_school_holiday_id_seq TO service_role;

GRANT SELECT ON shared.ref_public_holidays TO anon, authenticated;
GRANT ALL ON shared.ref_public_holidays TO service_role;
GRANT SELECT, USAGE ON SEQUENCE shared.ref_public_holidays_public_holiday_id_seq TO service_role;

GRANT SELECT ON shared.ref_major_events TO anon, authenticated;
GRANT ALL ON shared.ref_major_events TO service_role;
GRANT SELECT, USAGE ON SEQUENCE shared.ref_major_events_major_event_id_seq TO service_role;

CREATE OR REPLACE VIEW shared.v_busy_signal_daily AS
SELECT
  s.state_code,
  d.observation_date,
  CASE WHEN sh.school_holiday_id IS NOT NULL THEN true ELSE false END AS school_holiday_active,
  sh.term_or_break_label AS school_holiday_label,
  CASE WHEN ph.public_holiday_id IS NOT NULL THEN true ELSE false END AS public_holiday_active,
  ph.holiday_name AS public_holiday_name,
  ph.creates_long_weekend,
  (SELECT COUNT(*) FROM shared.ref_major_events e
   WHERE e.state_code = s.state_code
     AND d.observation_date BETWEEN e.start_date AND e.end_date
     AND e.is_active = true) AS major_events_count
FROM (SELECT DISTINCT state_code FROM shared.ref_destinations WHERE is_active = true) s
CROSS JOIN generate_series(CURRENT_DATE, CURRENT_DATE + INTERVAL '24 months', '1 day')::date AS d(observation_date)
LEFT JOIN shared.ref_school_holidays sh
  ON sh.state_code = s.state_code AND d.observation_date BETWEEN sh.start_date AND sh.end_date
LEFT JOIN shared.ref_public_holidays ph
  ON (ph.state_code = s.state_code OR ph.is_national = true) AND ph.observed_date = d.observation_date;

GRANT SELECT ON shared.v_busy_signal_daily TO anon, authenticated, service_role;
