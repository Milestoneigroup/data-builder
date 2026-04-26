"""Canonical 53-column celebrant CSV/DB order for Layer 2/3 + enrichment.

``VERIFY_REQUIRED`` is the sentinel for fields not yet filled (not NULL in CSV).

If ``data/celebrants_au_v1.csv`` exists, column order is taken from its header
(so you can add the file later without code changes). Otherwise a default
53-column superset (merge + website + Places + load) is used. When the starter
CSV or DB column list changes, update this module and
``supabase/migrations/003_celebrants.sql`` together.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_V1 = _ROOT / "data" / "celebrants_au_v1.csv"

VERIFY_REQUIRED = "VERIFY_REQUIRED"

# Default 53 fields when starter CSV is absent (must match migration 003).
DEFAULT_53: tuple[str, ...] = (
    "celebrant_id",
    "brand_id",
    "full_name",
    "ag_display_name",
    "title",
    "email",
    "phone",
    "state",
    "address_text",
    "suburb",
    "postcode",
    "website",
    "registration_date",
    "register_class",
    "status",
    "unavailability_text",
    "ceremony_type",
    "data_source",
    "abia_winner",
    "abia_awards_text",
    "vibe",
    "style_description",
    "service_area_notes",
    "min_price_aud",
    "max_price_aud",
    "years_experience",
    "estimated_ceremonies",
    "languages_non_english",
    "instagram_handle_or_url",
    "facebook_url",
    "phone_from_website",
    "celebrant_institute_member",
    "joshua_withers_mentioned",
    "data_quality_score",
    "merge_fuzzy_score",
    "is_standalone_award_entry",
    "google_place_id",
    "google_rating",
    "google_review_count",
    "website_from_places",
    "phone_from_places",
    "last_website_enrich_at",
    "last_places_enrich_at",
    "ag_scrape_page",
    "ag_scrape_index",
    "import_notes",
    "pds_ack",
    "insurance_notes",
    "public_profile_url",
    "linkedin_url",
    "raw_address_cell",
    "last_updated_source",
    "content_tier",
)


def load_53_column_names() -> list[str]:
    p = _DEFAULT_V1
    if p.is_file():
        with p.open(encoding="utf-8-sig", newline="") as f:
            r = csv.reader(f)
            header = next(r)
        cols = [c.strip() for c in header if c.strip()]
        if len(cols) == 53:
            return cols
        print(
            f"Warning: {p} has {len(cols)} columns (expected 53); using default schema.",
            file=sys.stderr,
        )
    return list(DEFAULT_53)


def empty_row() -> dict[str, str]:
    return {c: VERIFY_REQUIRED for c in load_53_column_names()}
