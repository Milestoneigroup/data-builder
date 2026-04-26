-- shared.ref_destination_services — wedding weekend supplier & services layer (Section C)
-- Adds nearest-airport columns on shared.ref_destinations for reuse across products.

CREATE TABLE IF NOT EXISTS shared.ref_destination_services (
  destination_id text PRIMARY KEY
    REFERENCES shared.ref_destinations (destination_id) ON DELETE CASCADE,
  state_code text NOT NULL,

  nearest_airport_name text NOT NULL DEFAULT '',
  nearest_airport_iata text NOT NULL DEFAULT '',
  nearest_airport_distance_km integer,
  nearest_airport_drive_mins integer,
  airport_transfer_name text NOT NULL DEFAULT '',
  airport_transfer_url text NOT NULL DEFAULT '',
  airport_transfer_phone text NOT NULL DEFAULT '',
  local_taxi_name text NOT NULL DEFAULT '',
  local_taxi_phone text NOT NULL DEFAULT '',
  rideshare_available boolean NOT NULL DEFAULT false,
  bus_coach_services text NOT NULL DEFAULT '',
  train_station_name text NOT NULL DEFAULT '',
  train_station_distance_km integer,

  accommodation_budget_name text NOT NULL DEFAULT '',
  accommodation_budget_url text NOT NULL DEFAULT '',
  accommodation_mid_name text NOT NULL DEFAULT '',
  accommodation_mid_url text NOT NULL DEFAULT '',
  accommodation_luxury_name text NOT NULL DEFAULT '',
  accommodation_luxury_url text NOT NULL DEFAULT '',
  accommodation_capacity_note text NOT NULL DEFAULT '',
  primary_booking_platform text NOT NULL DEFAULT '',

  babysitter_service_name text NOT NULL DEFAULT '',
  babysitter_service_url text NOT NULL DEFAULT '',
  child_activity_1 text NOT NULL DEFAULT '',
  child_activity_2 text NOT NULL DEFAULT '',
  nearest_hospital_name text NOT NULL DEFAULT '',
  nearest_hospital_distance_km integer,
  nearest_pharmacy_name text NOT NULL DEFAULT '',
  nearest_pharmacy_address text NOT NULL DEFAULT '',

  florist_name text NOT NULL DEFAULT '',
  florist_url text NOT NULL DEFAULT '',
  florist_instagram text NOT NULL DEFAULT '',
  florist_google_rating numeric(3, 1),
  hairmakeup_name text NOT NULL DEFAULT '',
  hairmakeup_url text NOT NULL DEFAULT '',
  hairmakeup_instagram text NOT NULL DEFAULT '',
  dj_band_name text NOT NULL DEFAULT '',
  dj_band_url text NOT NULL DEFAULT '',
  photobooth_name text NOT NULL DEFAULT '',
  photobooth_url text NOT NULL DEFAULT '',
  cake_maker_name text NOT NULL DEFAULT '',
  cake_maker_url text NOT NULL DEFAULT '',
  caterer_name text NOT NULL DEFAULT '',
  caterer_url text NOT NULL DEFAULT '',
  marquee_hire_name text NOT NULL DEFAULT '',
  marquee_hire_url text NOT NULL DEFAULT '',
  celebrant_crossref text NOT NULL DEFAULT '',
  photographer_crossref text NOT NULL DEFAULT '',

  rehearsal_dinner_venue text NOT NULL DEFAULT '',
  rehearsal_dinner_url text NOT NULL DEFAULT '',
  morning_after_cafe text NOT NULL DEFAULT '',
  morning_after_cafe_url text NOT NULL DEFAULT '',
  hens_bucks_bar text NOT NULL DEFAULT '',
  hens_bucks_bar_url text NOT NULL DEFAULT '',
  local_food_speciality text NOT NULL DEFAULT '',

  rainy_day_activity_1_name text NOT NULL DEFAULT '',
  rainy_day_activity_1_type text NOT NULL DEFAULT '',
  rainy_day_activity_1_url text NOT NULL DEFAULT '',
  rainy_day_activity_2_name text NOT NULL DEFAULT '',
  rainy_day_activity_2_type text NOT NULL DEFAULT '',
  rainy_day_activity_2_url text NOT NULL DEFAULT '',
  rainy_day_venue_hire_name text NOT NULL DEFAULT '',
  rainy_day_venue_hire_url text NOT NULL DEFAULT '',

  data_confidence text NOT NULL DEFAULT 'medium',
  scraped_date date NOT NULL,
  notes text NOT NULL DEFAULT '',
  updated_at date NOT NULL DEFAULT (CURRENT_DATE)
);

COMMENT ON TABLE shared.ref_destination_services IS
  'Practical guest and couple services for wedding weekends by destination cluster.';

CREATE INDEX IF NOT EXISTS idx_destination_services_state
  ON shared.ref_destination_services (state_code);

ALTER TABLE shared.ref_destination_services ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "destination_services_read_anon" ON shared.ref_destination_services;
DROP POLICY IF EXISTS "destination_services_read_auth" ON shared.ref_destination_services;
DROP POLICY IF EXISTS "destination_services_write_service" ON shared.ref_destination_services;
CREATE POLICY "destination_services_read_anon" ON shared.ref_destination_services
  FOR SELECT TO anon USING (true);
CREATE POLICY "destination_services_read_auth" ON shared.ref_destination_services
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "destination_services_write_service" ON shared.ref_destination_services
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_destination_services TO anon;
GRANT SELECT ON shared.ref_destination_services TO authenticated;
GRANT ALL ON shared.ref_destination_services TO service_role;

-- Optional columns on ref_destinations for nearest commercial airport (computed).
ALTER TABLE shared.ref_destinations
  ADD COLUMN IF NOT EXISTS nearest_airport_iata text;
ALTER TABLE shared.ref_destinations
  ADD COLUMN IF NOT EXISTS nearest_airport_drive_mins integer;

COMMENT ON COLUMN shared.ref_destinations.nearest_airport_iata IS
  'Nearest major commercial airport IATA code (from reference + geo).';
COMMENT ON COLUMN shared.ref_destinations.nearest_airport_drive_mins IS
  'Driving time in minutes to nearest_airport_iata (Distance Matrix or estimate).';
