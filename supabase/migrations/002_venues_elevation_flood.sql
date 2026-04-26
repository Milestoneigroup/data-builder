-- Elevation (Google Elevation API) + flood hazard enrichment columns on venues.
-- Targets public.venues; if you use shared.venues, run equivalent ALTER there.

ALTER TABLE public.venues
  ADD COLUMN IF NOT EXISTS elevation_metres integer,
  ADD COLUMN IF NOT EXISTS flood_risk_category text,
  ADD COLUMN IF NOT EXISTS flood_data_source text;

COMMENT ON COLUMN public.venues.elevation_metres IS
  'Ground elevation in metres above mean sea level (Google Elevation API, rounded).';
COMMENT ON COLUMN public.venues.flood_risk_category IS
  'Rarely | Occasionally | Frequently | Highly Frequently | Unknown | CHECK_MANUALLY';
COMMENT ON COLUMN public.venues.flood_data_source IS
  'Provenance for flood classification, e.g. Geoscience Australia National Flood Hazard.';
