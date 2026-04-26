"""Load enriched venues from Excel into Supabase ``venues`` (upsert on ``place_id``).

Reads ``data/venues_enriched_FULL.xlsx``, keeps rows with
``places_match_confidence == HIGH`` and ``business_status == OPERATIONAL``,
maps columns, then upserts in batches.

Requires ``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY``.
The ``venues`` table must have a **UNIQUE** constraint on ``place_id`` for upsert.

Run: ``python -m scrapers.load_venues_supabase``
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

EXCEL_PATH = _ROOT / "data" / "venues_enriched_FULL.xlsx"
PROGRESS_EVERY = 50
UPSERT_BATCH = 50

BOOL_SOURCE_COLS = (
    "wheelchair_accessible_entrance",
    "has_outdoor_seating",
    "has_live_music",
    "good_for_groups",
    "serves_wine",
    "serves_beer",
    "parking_free_lot",
    "parking_street",
)


def _is_blank(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    if isinstance(val, str) and val.strip().lower() == "nan":
        return True
    try:
        if val is pd.NA:
            return True
    except (TypeError, ValueError):
        pass
    try:
        if pd.isna(val):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, np.floating):
        try:
            if np.isnan(val):
                return True
        except TypeError:
            pass
    return False


def clean_scalar(val: Any) -> Any:
    """NaN / 'nan' / '' → None for JSON-safe payloads."""
    if _is_blank(val):
        return None
    return val


def bool_from_excel(val: Any) -> bool | None:
    v = clean_scalar(val)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, np.integer)):
        return bool(v)
    if isinstance(v, (float, np.floating)):
        if math.isnan(float(v)):
            return None
        if float(v) == 1.0:
            return True
        if float(v) == 0.0:
            return False
    if isinstance(v, str):
        t = v.strip().lower()
        if t in ("true", "1", "yes"):
            return True
        if t in ("false", "0", "no"):
            return False
    return None


def int_from_excel(val: Any) -> int | None:
    v = clean_scalar(val)
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
        return int(float(v))
    except (TypeError, ValueError):
        return None


def postcode_str(val: Any) -> str | None:
    v = clean_scalar(val)
    if v is None:
        return None
    try:
        if isinstance(v, (float, np.floating)) and float(v).is_integer():
            return str(int(float(v)))
        if isinstance(v, (int, np.integer)):
            return str(int(v))
    except (TypeError, ValueError, OverflowError):
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    if s.endswith(".0"):
        s = s[:-2]
    return s or None


def opening_hours_json(val: Any) -> Any | None:
    v = clean_scalar(val)
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def enrichment_date_str(val: Any) -> str | None:
    v = clean_scalar(val)
    if v is None:
        return None
    if hasattr(v, "strftime"):
        try:
            return v.strftime("%Y-%m-%d")
        except Exception:  # noqa: BLE001
            pass
    s = str(v).strip()
    return s or None


def row_to_record(row: pd.Series) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "name": clean_scalar(row.get("NAME")),
        "state": clean_scalar(row.get("STATE")),
        "region": clean_scalar(row.get("REGION")),
        "type": clean_scalar(row.get("TYPE")),
        "address": clean_scalar(row.get("formatted_address")),
        "lat": clean_scalar(row.get("lat")),
        "lng": clean_scalar(row.get("lng")),
        "website_from_google": clean_scalar(row.get("website_from_google")),
        "website": clean_scalar(row.get("WEBSITE")),
        "phone": clean_scalar(row.get("phone_local")),
        "google_rating": clean_scalar(row.get("google_rating")),
        "google_review_count": int_from_excel(row.get("google_review_count")),
        "postcode": postcode_str(row.get("postcode")),
        "place_id": clean_scalar(row.get("place_id")),
        "google_maps_url": clean_scalar(row.get("google_maps_url")),
        "business_status": clean_scalar(row.get("business_status")),
        "google_name": clean_scalar(row.get("google_name")),
        "editorial_summary": clean_scalar(row.get("editorial_summary")),
        "google_primary_type": clean_scalar(row.get("google_primary_type")),
        "award_2025": clean_scalar(row.get("AWARD 2025")),
        "award_body": clean_scalar(row.get("AWARD BODY")),
        "instagram_handle": clean_scalar(row.get("INSTAGRAM HANDLE")),
        "photo_ref_1": clean_scalar(row.get("photo_ref_1")),
        "photo_ref_2": clean_scalar(row.get("photo_ref_2")),
        "photo_ref_3": clean_scalar(row.get("photo_ref_3")),
        "photo_ref_4": clean_scalar(row.get("photo_ref_4")),
        "enrichment_date": enrichment_date_str(row.get("enrichment_date")),
        "fuzzy_match_score": int_from_excel(row.get("fuzzy_match_score")),
        "places_match_confidence": clean_scalar(row.get("places_match_confidence")),
        "total_photo_count": int_from_excel(row.get("total_photo_count")),
        "capacity_max": int_from_excel(row.get("CAPACITY MAX")),
        "setting": clean_scalar(row.get("SETTING")),
        "price_tier": clean_scalar(row.get("PRICE TIER")),
        "enrichment_status": clean_scalar(row.get("ENRICHMENT STATUS")),
        "data_source": clean_scalar(row.get("DATA SOURCE")),
        "price_level": int_from_excel(row.get("price_level")),
        "opening_hours": opening_hours_json(row.get("opening_hours")),
    }
    for col in BOOL_SOURCE_COLS:
        key = col
        rec[key] = bool_from_excel(row.get(col))

    # Final pass: empty strings → None (clean_scalar already; float nan edge)
    out: dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, str) and v.strip() == "":
            out[k] = None
        else:
            out[k] = v
    return out


def main() -> None:
    from data_builder.config import get_settings

    settings = get_settings()
    url = (settings.supabase_url or "").strip()
    key = (settings.supabase_service_role_key or "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")

    if not EXCEL_PATH.is_file():
        raise SystemExit(f"Missing Excel file: {EXCEL_PATH}")

    df = pd.read_excel(EXCEL_PATH, sheet_name=0)
    total_rows = len(df)

    mask = (df["places_match_confidence"] == "HIGH") & (df["business_status"] == "OPERATIONAL")
    filtered = df.loc[mask].copy()
    selected_before_dedupe = len(filtered)
    skipped_filter = total_rows - selected_before_dedupe

    filtered = filtered.drop_duplicates(subset=["place_id"], keep="first")
    dup_removed = selected_before_dedupe - len(filtered)
    print(f"Duplicates removed: {dup_removed} rows")

    records: list[dict[str, Any]] = []
    skipped_bad_pid = 0
    for _, row in filtered.iterrows():
        pid = row.get("place_id")
        if _is_blank(pid) or not str(pid).strip():
            skipped_bad_pid += 1
            continue
        records.append(row_to_record(row))

    skipped = skipped_filter + skipped_bad_pid

    from supabase import create_client

    client = create_client(url, key)

    loaded = 0
    errors = 0
    n = len(records)
    first_err: str | None = None

    for start in range(0, n, UPSERT_BATCH):
        batch = records[start : start + UPSERT_BATCH]
        try:
            client.table("venues").upsert(batch, on_conflict="place_id").execute()
            loaded += len(batch)
        except Exception as e:  # noqa: BLE001
            errors += len(batch)
            if first_err is None:
                first_err = repr(e)
                print(f"ERROR upserting batch {start}-{start + len(batch)}: {first_err}")

        done = min(start + len(batch), n)
        if done % PROGRESS_EVERY == 0 or done == n:
            print(f"Progress: {done}/{n} | Loaded: {loaded} | Skipped: {skipped} | Errors: {errors}")

    print("")
    print("--- load_venues_supabase summary ---")
    print(f"Excel rows read:     {total_rows}")
    print(f"After HIGH+OP filter: {selected_before_dedupe} (before dedupe)")
    print(f"Rows to upsert:      {len(records)}")
    print(f"Loaded (upserted):   {loaded}")
    print(f"Skipped:             {skipped}")
    print(f"Errors (rows):       {errors}")
    if first_err and errors:
        print(f"(first error):       {first_err}")
    print("")


if __name__ == "__main__":
    main()
