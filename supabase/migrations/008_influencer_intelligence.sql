-- Influencer intelligence: directory + content + commercial signals.
-- RLS: anon (and authenticated) read; service_role write. Drop order: children first.

-- ---------------------------------------------------------------------------
-- Sequences for stable prefixed IDs
-- ---------------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS shared.ref_influencers_source_seq;

-- content_id (e.g. CONT-INF-001-001) is set by the loader / scraper.
DROP FUNCTION IF EXISTS shared.default_influencer_content_id() CASCADE;

-- ---------------------------------------------------------------------------
-- 1) shared.ref_influencers
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS shared.ref_influencer_content CASCADE;
DROP TABLE IF EXISTS shared.ref_influencer_signals CASCADE;
DROP TABLE IF EXISTS shared.ref_influencers CASCADE;

CREATE OR REPLACE FUNCTION shared.default_influencer_source_id()
RETURNS text
LANGUAGE sql
VOLATILE
AS $$
  SELECT 'INF-WED-' || lpad(nextval('shared.ref_influencers_source_seq')::text, 3, '0');
$$;

CREATE TABLE shared.ref_influencers (
  source_id text PRIMARY KEY
    DEFAULT shared.default_influencer_source_id(),

  name text NOT NULL,
  founder_name text,
  founder_gender text,
  url text NOT NULL,
  root_domain text,
  source_type text,

  states text,
  primary_state text,
  key_locations text,
  is_international boolean NOT NULL DEFAULT false,
  country text NOT NULL DEFAULT 'Australia',

  specialism_primary text
    CHECK (
      specialism_primary IS NULL
      OR specialism_primary = ANY (ARRAY[
        'planning',
        'photography',
        'styling',
        'venue_discovery',
        'fashion_dress',
        'honeymoon_travel',
        'cultural',
        'elopement',
        'lgbtq',
        'budget',
        'luxury',
        'real_weddings',
        'suppliers',
        'entertainment',
        'food_cake',
        'flowers'
      ]::text[])
    ),
  specialism_tags text,
  specialism_description text,

  audience_persona_tags text,
  audience_size_estimate text
    CHECK (
      audience_size_estimate IS NULL
      OR audience_size_estimate = ANY (ARRAY['small', 'medium', 'large', 'mega']::text[])
    ),
  audience_type text
    CHECK (audience_type IS NULL OR audience_type = ANY (ARRAY['couples', 'industry', 'both']::text[])),

  contact_email text,
  contact_name text,
  contact_role text,

  instagram_handle text,
  instagram_followers integer
    CHECK (instagram_followers IS NULL OR instagram_followers >= 0),
  instagram_engagement_rate numeric(4, 2)
    CHECK (instagram_engagement_rate IS NULL OR (instagram_engagement_rate >= 0 AND instagram_engagement_rate <= 100)),
  instagram_last_post_date date,
  tiktok_handle text,
  tiktok_followers integer
    CHECK (tiktok_followers IS NULL OR tiktok_followers >= 0),
  pinterest_handle text,
  facebook_url text,
  youtube_channel text,
  youtube_subscribers integer
    CHECK (youtube_subscribers IS NULL OR youtube_subscribers >= 0),

  publishes_blog boolean NOT NULL DEFAULT true,
  blog_index_url text,
  about_url text,
  avg_posts_per_month integer
    CHECK (avg_posts_per_month IS NULL OR avg_posts_per_month >= 0),
  last_post_date date,
  content_quality_score integer
    CHECK (content_quality_score IS NULL OR (content_quality_score BETWEEN 1 AND 10)),

  has_advertising boolean,
  has_affiliate boolean,
  has_brand_collab boolean,
  estimated_rate_aud text,
  partnership_potential text
    CHECK (partnership_potential IS NULL OR partnership_potential = ANY (ARRAY['high', 'medium', 'low']::text[])),
  mig_relevance_score integer
    CHECK (mig_relevance_score IS NULL OR (mig_relevance_score BETWEEN 1 AND 10)),
  insurance_hook boolean NOT NULL DEFAULT false,

  relationship_status text
    CHECK (relationship_status IS NULL OR relationship_status = ANY (ARRAY['none', 'aware', 'contacted', 'partner']::text[])),
  relationship_notes text,
  last_contacted_date date,
  mig_mentioned boolean,
  linked_to_us boolean,

  trust_level text
    CHECK (trust_level IS NULL OR trust_level = ANY (ARRAY['high', 'medium', 'low']::text[])),
  is_active boolean NOT NULL DEFAULT true,
  data_confidence text,
  last_verified date,
  discovery_source text,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_ref_influencers_specialism_primary
  ON shared.ref_influencers (specialism_primary);
CREATE INDEX idx_ref_influencers_is_active
  ON shared.ref_influencers (is_active)
  WHERE is_active;
CREATE INDEX idx_ref_influencers_root_domain
  ON shared.ref_influencers (root_domain);

ALTER TABLE shared.ref_influencers ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "ref_influencers_read_anon" ON shared.ref_influencers;
DROP POLICY IF EXISTS "ref_influencers_read_auth" ON shared.ref_influencers;
DROP POLICY IF EXISTS "ref_influencers_write_service" ON shared.ref_influencers;
CREATE POLICY "ref_influencers_read_anon" ON shared.ref_influencers
  FOR SELECT TO anon USING (true);
CREATE POLICY "ref_influencers_read_auth" ON shared.ref_influencers
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "ref_influencers_write_service" ON shared.ref_influencers
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT, USAGE ON SEQUENCE shared.ref_influencers_source_seq TO service_role;
GRANT USAGE ON SEQUENCE shared.ref_influencers_source_seq TO authenticated;
GRANT SELECT ON shared.ref_influencers TO anon;
GRANT SELECT ON shared.ref_influencers TO authenticated;
GRANT ALL ON shared.ref_influencers TO service_role;

COMMENT ON TABLE shared.ref_influencers IS
  'Wedding influencer / blog directory with enrichment fields (MIG relationship + commercial).';

-- ---------------------------------------------------------------------------
-- 2) shared.ref_influencer_content
-- ---------------------------------------------------------------------------

CREATE TABLE shared.ref_influencer_content (
  content_id text PRIMARY KEY,
  source_id text NOT NULL
    REFERENCES shared.ref_influencers (source_id) ON DELETE CASCADE,

  title text NOT NULL,
  url text NOT NULL UNIQUE,
  published_date date,
  author_name text,

  content_type text
    CHECK (content_type IS NULL OR content_type = ANY (ARRAY[
      'article', 'real_wedding', 'styled_shoot', 'review', 'listicle', 'guide', 'sponsored'
    ]::text[])),
  topic_primary text
    CHECK (
      topic_primary IS NULL
      OR topic_primary = ANY (ARRAY[
        'planning',
        'photography',
        'styling',
        'venue_discovery',
        'fashion_dress',
        'honeymoon_travel',
        'cultural',
        'elopement',
        'lgbtq',
        'budget',
        'luxury',
        'real_weddings',
        'suppliers',
        'entertainment',
        'food_cake',
        'flowers'
      ]::text[])
    ),
  topic_tags text,

  insurance_relevant boolean NOT NULL DEFAULT false,
  insurance_hook_text text,

  estimated_word_count integer
    CHECK (estimated_word_count IS NULL OR estimated_word_count >= 0),
  has_images boolean,
  has_video boolean,

  is_ranking boolean,
  ranking_keyword text,
  estimated_traffic text
    CHECK (estimated_traffic IS NULL OR estimated_traffic = ANY (ARRAY['low', 'medium', 'high']::text[])),

  comment_count integer
    CHECK (comment_count IS NULL OR comment_count >= 0),
  social_shares integer
    CHECK (social_shares IS NULL OR social_shares >= 0),

  citation_opportunity boolean,
  outreach_angle text,

  scraped_date date,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_ref_influencer_content_source
  ON shared.ref_influencer_content (source_id);
CREATE INDEX idx_ref_influencer_content_published
  ON shared.ref_influencer_content (published_date DESC);
CREATE INDEX idx_ref_influencer_content_insurance
  ON shared.ref_influencer_content (insurance_relevant)
  WHERE insurance_relevant;

ALTER TABLE shared.ref_influencer_content ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "ref_influencer_content_read_anon" ON shared.ref_influencer_content;
DROP POLICY IF EXISTS "ref_influencer_content_read_auth" ON shared.ref_influencer_content;
DROP POLICY IF EXISTS "ref_influencer_content_write_service" ON shared.ref_influencer_content;
CREATE POLICY "ref_influencer_content_read_anon" ON shared.ref_influencer_content
  FOR SELECT TO anon USING (true);
CREATE POLICY "ref_influencer_content_read_auth" ON shared.ref_influencer_content
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "ref_influencer_content_write_service" ON shared.ref_influencer_content
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_influencer_content TO anon;
GRANT SELECT ON shared.ref_influencer_content TO authenticated;
GRANT ALL ON shared.ref_influencer_content TO service_role;

COMMENT ON TABLE shared.ref_influencer_content IS
  'Per-article / post row for influencer content intelligence (Wedsure hooks, SEO, outreach).';

-- ---------------------------------------------------------------------------
-- 3) shared.ref_influencer_signals
-- ---------------------------------------------------------------------------

CREATE TABLE shared.ref_influencer_signals (
  source_id text PRIMARY KEY
    REFERENCES shared.ref_influencers (source_id) ON DELETE CASCADE,

  persona_fit_score integer
    CHECK (persona_fit_score IS NULL OR (persona_fit_score BETWEEN 1 AND 10)),
  best_persona_match text,

  content_gap_topics text,
  best_collab_angle text,

  accepts_guest_posts boolean,
  accepts_product_reviews boolean,
  has_sponsored_content boolean,

  outreach_priority text
    CHECK (outreach_priority IS NULL OR outreach_priority = ANY (ARRAY[
      'p1_this_month', 'p2_this_quarter', 'p3_backlog'
    ]::text[])),
  outreach_reason text,
  ideal_first_ask text,

  we_can_offer text,

  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_ref_influencer_signals_outreach
  ON shared.ref_influencer_signals (outreach_priority);
CREATE INDEX idx_ref_influencer_signals_persona
  ON shared.ref_influencer_signals (persona_fit_score);

ALTER TABLE shared.ref_influencer_signals ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "ref_influencer_signals_read_anon" ON shared.ref_influencer_signals;
DROP POLICY IF EXISTS "ref_influencer_signals_read_auth" ON shared.ref_influencer_signals;
DROP POLICY IF EXISTS "ref_influencer_signals_write_service" ON shared.ref_influencer_signals;
CREATE POLICY "ref_influencer_signals_read_anon" ON shared.ref_influencer_signals
  FOR SELECT TO anon USING (true);
CREATE POLICY "ref_influencer_signals_read_auth" ON shared.ref_influencer_signals
  FOR SELECT TO authenticated USING (true);
CREATE POLICY "ref_influencer_signals_write_service" ON shared.ref_influencer_signals
  FOR ALL TO service_role USING (true) WITH CHECK (true);

GRANT SELECT ON shared.ref_influencer_signals TO anon;
GRANT SELECT ON shared.ref_influencer_signals TO authenticated;
GRANT ALL ON shared.ref_influencer_signals TO service_role;

COMMENT ON TABLE shared.ref_influencer_signals IS
  'Commercial and partnership intelligence (one row per influencer; periodic refresh).';

-- ---------------------------------------------------------------------------
-- updated_at triggers
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION shared.ref_influencers_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_ref_influencers_updated_at ON shared.ref_influencers;
CREATE TRIGGER trg_ref_influencers_updated_at
  BEFORE UPDATE ON shared.ref_influencers
  FOR EACH ROW
  EXECUTE PROCEDURE shared.ref_influencers_set_updated_at();

CREATE OR REPLACE FUNCTION shared.ref_influencer_signals_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_ref_influencer_signals_updated_at ON shared.ref_influencer_signals;
CREATE TRIGGER trg_ref_influencer_signals_updated_at
  BEFORE UPDATE ON shared.ref_influencer_signals
  FOR EACH ROW
  EXECUTE PROCEDURE shared.ref_influencer_signals_set_updated_at();
