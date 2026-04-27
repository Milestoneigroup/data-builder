-- PostgREST / service_role inserts into bigserial tables need sequence USAGE.

GRANT USAGE, SELECT ON SEQUENCE shared.ref_weather_daily_weather_daily_id_seq TO service_role;
GRANT USAGE, SELECT ON SEQUENCE shared.ref_weather_monthly_stats_monthly_stats_id_seq TO service_role;
