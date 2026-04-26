-- venue_ratings: monthly snapshot rows (INSERT only from application; no updates).
-- Adjust venue_id FK target if your venues table lives outside public (e.g. shared.venues).

CREATE TABLE IF NOT EXISTS public.venue_ratings (
  venue_id uuid NOT NULL,
  captured_date date NOT NULL,
  google_rating double precision,
  review_count integer,
  review_delta integer,
  rating_delta double precision,
  review_text_1 text,
  review_text_2 text,
  review_text_3 text,
  review_text_4 text,
  review_text_5 text,
  review_author_1 text,
  review_author_2 text,
  review_author_3 text,
  review_author_4 text,
  review_author_5 text,
  review_rating_1 smallint,
  review_rating_2 smallint,
  review_rating_3 smallint,
  review_rating_4 smallint,
  review_rating_5 smallint,
  review_date_1 timestamptz,
  review_date_2 timestamptz,
  review_date_3 timestamptz,
  review_date_4 timestamptz,
  review_date_5 timestamptz,
  pollen_grass_index smallint,
  pollen_tree_index smallint,
  pollen_weed_index smallint,
  dominant_pollen_type text,
  air_quality_index integer,
  air_quality_category text,
  dominant_pollutant text,
  sentiment_score double precision,
  sentiment_themes jsonb,
  red_flags jsonb,
  one_line_summary text,
  captured_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT venue_ratings_pkey PRIMARY KEY (venue_id, captured_date)
);

CREATE INDEX IF NOT EXISTS idx_venue_ratings_captured_date
  ON public.venue_ratings (captured_date DESC);

COMMENT ON TABLE public.venue_ratings IS
  'Monthly Google Places / Pollen / Air Quality / Claude sentiment snapshot per venue. '
  'Application inserts only; (venue_id, captured_date) is unique.';

ALTER TABLE public.venue_ratings ENABLE ROW LEVEL SECURITY;

-- Service role bypasses RLS in Supabase; policy documents intended access.
DROP POLICY IF EXISTS venue_ratings_service_all ON public.venue_ratings;
CREATE POLICY venue_ratings_service_all ON public.venue_ratings
  FOR ALL TO service_role
  USING (true)
  WITH CHECK (true);

DROP POLICY IF EXISTS venue_ratings_authenticated_select ON public.venue_ratings;
CREATE POLICY venue_ratings_authenticated_select ON public.venue_ratings
  FOR SELECT TO authenticated
  USING (true);
