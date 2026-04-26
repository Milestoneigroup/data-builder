"""Upsert all celebrant rows to ``public.celebrants`` (``celebrant_id`` as key).

Reads ``data/celebrants_merged.csv``; if ``data/celebrants_enriched_top300.csv`` exists,
merges Google fields by ``celebrant_id`` into the same frame before load.

Run migration ``003_celebrants.sql`` in Supabase before this script.

Run: ``python -m scrapers.load_celebrants_supabase``
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

MERGED = _ROOT / "data" / "celebrants_merged.csv"
TOP300 = _ROOT / "data" / "celebrants_enriched_top300.csv"
BATCH = 200


def _clean(v: Any) -> Any:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s


def main() -> int:
    from data_builder.config import get_settings

    s = get_settings()
    url = (s.supabase_url or "").strip()
    key = (s.supabase_service_role_key or "").strip()
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required", file=sys.stderr)
        return 1
    if not MERGED.is_file():
        print(f"ERROR: {MERGED} not found", file=sys.stderr)
        return 1
    base = pd.read_csv(MERGED, dtype=str, keep_default_na=False)
    if TOP300.is_file():
        t3 = pd.read_csv(TOP300, dtype=str, keep_default_na=False)
        for _, tr in t3.iterrows():
            cid = str(tr.get("celebrant_id", "")).strip()
            m = base["celebrant_id"].astype(str).str.strip() == cid
            if not m.any():
                continue
            for c in t3.columns:
                if c == "celebrant_id" or c not in base.columns:
                    continue
                v = tr.get(c, "")
                if _clean(v) is not None:
                    base.loc[m, c] = v
    db_cols = [
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
    ]
    for c in db_cols:
        if c not in base.columns:
            base[c] = "VERIFY_REQUIRED"
    records: list[dict[str, Any]] = []
    for _, r in base.iterrows():
        rec = {c: _clean(r.get(c)) for c in db_cols}
        # DB expects not-null text; use VERIFY_REQUIRED
        for c in db_cols:
            if rec.get(c) is None:
                rec[c] = "VERIFY_REQUIRED"
        records.append(rec)
    from supabase import create_client

    client = create_client(url, key)
    # Fail fast with a clear action if the migration is not applied.
    try:
        client.table("celebrants").select("celebrant_id", count="exact").limit(1).execute()
    except Exception as e:  # noqa: BLE001
        print(
            "ERROR: public.celebrants not found. Run supabase/migrations/003_celebrants.sql "
            f"in the Supabase SQL editor (or `supabase db push`), then retry.\n{e!r}",
            file=sys.stderr,
        )
        return 1
    loaded = 0
    err = 0
    n = len(records)
    for st in range(0, n, BATCH):
        batch = records[st : st + BATCH]
        try:
            client.table("celebrants").upsert(batch, on_conflict="celebrant_id").execute()
            loaded += len(batch)
        except Exception as e:  # noqa: BLE001
            err += len(batch)
            print(f"Batch error: {e!r}"[:500])
    print(f"Total loaded: {loaded}, errors: {err}, rows: {n}")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
