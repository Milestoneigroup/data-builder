-- Extend allowed values for venues.abn_name_search_confidence so Method B can record transport/API failures.
alter table public.venues drop constraint if exists venues_abn_name_search_confidence_check;

alter table public.venues
  add constraint venues_abn_name_search_confidence_check
  check (
    abn_name_search_confidence is null
    or abn_name_search_confidence in (
      'exact_name_match',
      'strong_state_match',
      'fuzzy',
      'multiple_candidates',
      'no_match',
      'api_error'
    )
  );
