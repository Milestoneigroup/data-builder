-- Celebrant Google Places (full) + review + sentiment columns (additive).

ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS business_status text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS google_name text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS google_primary_type text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS google_types_json text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS editorial_summary text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS price_level integer;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS opening_hours text;

ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_text_1 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_text_2 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_text_3 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_text_4 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_text_5 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_author_1 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_author_2 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_author_3 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_author_4 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_author_5 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_rating_1 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_rating_2 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_rating_3 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_rating_4 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_rating_5 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_date_1 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_date_2 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_date_3 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_date_4 text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS review_date_5 text;

ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS good_for_groups boolean;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS serves_wine boolean;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS serves_beer boolean;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS outdoor_seating boolean;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS wheelchair_accessible boolean;

ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS sentiment_score numeric(4, 3);
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS sentiment_themes text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS sentiment_red_flags text;
ALTER TABLE public.celebrants ADD COLUMN IF NOT EXISTS sentiment_summary text;
