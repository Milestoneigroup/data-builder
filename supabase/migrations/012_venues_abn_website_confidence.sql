-- Website ABN scrape (Session 1): confidence must not reuse abn_lookup_confidence
-- (CHECK allows only API verification values: exact_abn, strong_match, …).

alter table public.venues
  add column if not exists abn_website_confidence text;

comment on column public.venues.abn_website_confidence is
  'Session 1 website scrape only: HIGH or MEDIUM (label/context heuristics). ABR API fill in Session 2.';

alter table public.venues drop constraint if exists venues_abn_website_confidence_check;

alter table public.venues
  add constraint venues_abn_website_confidence_check
  check (
    abn_website_confidence is null
    or abn_website_confidence in ('HIGH', 'MEDIUM')
  );
