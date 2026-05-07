-- Sub-A migration 002 — capture Hello May sub-category
-- Hello May categorises beyond standard wedding verticals (e.g. luxe-stays,
-- destination-wedding). Capture as metadata for future segmentation.

ALTER TABLE public.venues
  ADD COLUMN IF NOT EXISTS hello_may_category text;

ALTER TABLE public.celebrants
  ADD COLUMN IF NOT EXISTS hello_may_category text;

ALTER TABLE public.photographers
  ADD COLUMN IF NOT EXISTS hello_may_category text;
