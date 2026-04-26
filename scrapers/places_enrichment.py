"""Enrich wedding venue rows with Google Places Text Search + Place Details (full run)."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import date, datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pandas as pd
from dotenv import load_dotenv
from thefuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# --- toggles ---
TEST_MODE = False
EXCEL_PATH = _ROOT / "data" / "wedding_venues_AU_master_v25.xlsx"
OUTPUT_PATH = _ROOT / "data" / "venues_enriched_FULL.xlsx"
CHECKPOINT_PATH = _ROOT / "data" / "enrichment_checkpoint.csv"
ENRICHMENT_LOG = _ROOT / "logs" / "enrichment_errors.log"
TARGET_SHEET_SUBSTR = "AU Venues (966)"
REQUEST_DELAY_S = 0.3
CHECKPOINT_EVERY = 50
PROGRESS_EVERY = 50
FUZZY_THRESHOLD = 70

TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TEXT_SEARCH_FIELD_MASK = (
    "places.id,places.name,places.displayName,places.formattedAddress"
)
# Field mask: no spaces (Places API requirement).
DETAILS_FIELD_MASK = (
    "id,name,displayName,formattedAddress,adrFormatAddress,location,rating,userRatingCount,"
    "businessStatus,photos,googleMapsUri,websiteUri,nationalPhoneNumber,internationalPhoneNumber,"
    "regularOpeningHours,priceLevel,editorialSummary,primaryType,types,accessibilityOptions,"
    "outdoorSeating,liveMusic,goodForGroups,servesWine,servesBeer,reservable,parkingOptions,reviews"
)

LOG = logging.getLogger(__name__)

NEW_COLUMNS: tuple[str, ...] = (
    "place_id",
    "google_maps_url",
    "business_status",
    "formatted_address",
    "postcode",
    "adr_address",
    "lat",
    "lng",
    "website_from_google",
    "phone_local",
    "phone_international",
    "google_rating",
    "google_review_count",
    "photo_ref_1",
    "photo_ref_2",
    "photo_ref_3",
    "photo_ref_4",
    "total_photo_count",
    "places_match_confidence",
    "places_search_query",
    "google_name",
    "fuzzy_match_score",
    "opening_hours",
    "price_level",
    "editorial_summary",
    "google_primary_type",
    "google_types_json",
    "wheelchair_accessible_entrance",
    "wheelchair_accessible_parking",
    "wheelchair_accessible_restroom",
    "wheelchair_accessible_seating",
    "has_outdoor_seating",
    "has_live_music",
    "good_for_groups",
    "serves_wine",
    "serves_beer",
    "is_reservable",
    "parking_free_lot",
    "parking_paid",
    "parking_valet",
    "parking_street",
    "review_text_1",
    "review_author_1",
    "review_rating_1",
    "review_date_1",
    "review_text_2",
    "review_author_2",
    "review_rating_2",
    "review_date_2",
    "review_text_3",
    "review_author_3",
    "review_rating_3",
    "review_date_3",
    "review_text_4",
    "review_author_4",
    "review_rating_4",
    "review_date_4",
    "review_text_5",
    "review_author_5",
    "review_rating_5",
    "review_date_5",
    "website_needs_review",
    "enrichment_date",
)


def _norm_name(s: str) -> str:
    return "".join(c.lower() for c in s if not c.isspace() or c == " ").strip()


def _name_match_ratio(venue: str, candidate: str | None) -> float:
    if not candidate or not venue:
        return 0.0
    return SequenceMatcher(None, _norm_name(venue), _norm_name(candidate)).ratio()


def _match_confidence_high_medium(venue_name: str, google_name: str | None) -> str:
    if not google_name:
        return "MEDIUM"
    r = _name_match_ratio(venue_name, google_name)
    if r >= 0.72:
        return "HIGH"
    if r >= 0.35:
        return "MEDIUM"
    return "MEDIUM"


def _clean_region(region: Any) -> str:
    if pd.isna(region):
        return ""
    s = str(region).strip()
    if "/" in s:
        s = s.split("/")[0].strip()
    return s


def _build_queries(name: str, region: str, state: str) -> tuple[str, str]:
    name = (name or "").strip()
    state = (state or "").strip()
    reg = _clean_region(region)
    primary = " ".join(p for p in (name, reg, state, "Australia") if p).strip()
    fallback = " ".join(p for p in (name, "wedding venue", state, "Australia") if p).strip()
    return primary, fallback


def _pick_sheet(xl: pd.ExcelFile) -> str:
    for s in xl.sheet_names:
        if TARGET_SHEET_SUBSTR in s:
            return s
    return xl.sheet_names[0]


def _has_chi_place_id(val: Any) -> bool:
    if pd.isna(val):
        return False
    return str(val).strip().startswith("ChI")


def _row_already_enriched(df: pd.DataFrame, idx: int) -> bool:
    for key in ("place_id", "PLACE_ID"):
        if key in df.columns and _has_chi_place_id(df.loc[idx, key]):
            return True
    return False


def _sleep_between_calls() -> None:
    time.sleep(REQUEST_DELAY_S)


def _place_id_from_resource(name: str | None) -> str:
    if not name:
        return ""
    if name.startswith("places/"):
        return name.split("/", 1)[1]
    return name


def _places_text_search(client: httpx.Client, query: str, api_key: str) -> dict[str, Any]:
    r = client.post(
        TEXT_SEARCH_URL,
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": TEXT_SEARCH_FIELD_MASK,
        },
        json={"textQuery": query},
        timeout=60.0,
    )
    _sleep_between_calls()
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        try:
            body = r.json()
            msg = (body.get("error") or {}).get("message", r.text[:400])
        except Exception:  # noqa: BLE001
            msg = r.text[:400] if r.text else str(r.status_code)
        return {"_error": msg, "places": []}
    data = r.json()
    if isinstance(data, dict) and "error" in data:
        err = data.get("error") or {}
        return {"_error": err.get("message", str(err)), "places": []}
    return data if isinstance(data, dict) else {"places": []}


def _places_details(client: httpx.Client, place_id: str, api_key: str) -> dict[str, Any]:
    pid = quote(place_id, safe="")
    url = f"https://places.googleapis.com/v1/places/{pid}"
    r = client.get(
        url,
        headers={
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": DETAILS_FIELD_MASK,
        },
        timeout=60.0,
    )
    _sleep_between_calls()
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        try:
            body = r.json()
            msg = (body.get("error") or {}).get("message", r.text[:400])
        except Exception:  # noqa: BLE001
            msg = r.text[:400] if r.text else str(r.status_code)
        return {"_error": msg}
    data = r.json()
    if isinstance(data, dict) and "error" in data:
        err = data.get("error") or {}
        return {"_error": err.get("message", str(err))}
    return data if isinstance(data, dict) else {}


def _display_name_text(obj: dict[str, Any] | None) -> str | None:
    if not obj:
        return None
    dn = obj.get("displayName")
    if isinstance(dn, dict):
        return dn.get("text")
    return None


def _photo_refs(photos: list[dict[str, Any]] | None) -> tuple[str, str, str, str, int]:
    refs = ["", "", "", ""]
    n = len(photos or [])
    if photos:
        for i, ph in enumerate(photos[:4]):
            refs[i] = str(ph.get("name") or ph.get("photo_reference") or "")
    return refs[0], refs[1], refs[2], refs[3], n


def _price_level_to_int(raw: Any) -> Any:
    if raw is None or raw == "" or str(raw) == "PRICE_LEVEL_UNSPECIFIED":
        return pd.NA
    if isinstance(raw, int) and 0 <= raw <= 4:
        return raw
    m = {
        "PRICE_LEVEL_FREE": 0,
        "PRICE_LEVEL_INEXPENSIVE": 1,
        "PRICE_LEVEL_MODERATE": 2,
        "PRICE_LEVEL_EXPENSIVE": 3,
        "PRICE_LEVEL_VERY_EXPENSIVE": 4,
    }
    return m.get(str(raw), pd.NA)


def _localized_text(t: Any) -> str:
    if isinstance(t, dict):
        return str(t.get("text") or "")
    return ""


def _extract_review_columns(reviews: list[dict[str, Any]] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    reviews = reviews or []
    for i in range(5):
        n = i + 1
        if i < len(reviews):
            rv = reviews[i]
            text = _localized_text(rv.get("text")) or _localized_text(rv.get("originalText"))
            auth = rv.get("authorAttribution") or {}
            author = str(auth.get("displayName") or "") if isinstance(auth, dict) else ""
            rating_rv = rv.get("rating")
            pub = str(rv.get("publishTime") or "")
        else:
            text, author, rating_rv, pub = "", "", pd.NA, ""
        out[f"review_text_{n}"] = text
        out[f"review_author_{n}"] = author
        out[f"review_rating_{n}"] = rating_rv
        out[f"review_date_{n}"] = pub
    return out


def _extract_postcode(formatted_address: str) -> str:
    if not formatted_address:
        return ""
    addr = str(formatted_address)
    if "Australia" in addr:
        before = addr.split("Australia", 1)[0]
    else:
        before = addr
    nums = re.findall(r"\b\d{4}\b", before)
    return nums[-1] if nums else ""


def _bool_or_na(val: Any) -> Any:
    if val is None:
        return pd.NA
    if isinstance(val, bool):
        return val
    return pd.NA


def _parking_bool(park: dict[str, Any] | None, key: str) -> Any:
    if not park or not isinstance(park, dict):
        return pd.NA
    if key not in park:
        return pd.NA
    return bool(park[key])


def _append_enrichment_log(kind: str, message: str) -> None:
    ENRICHMENT_LOG.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    line = f"{ts}\t{kind}\t{message}\n"
    with ENRICHMENT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line)


def _append_low_confidence_log(venue: str, google_name: str, score: int) -> None:
    _append_enrichment_log(
        "LOW",
        f"venue={venue!r}\tgoogle_name={google_name!r}\tfuzzy_token_sort_score={score}",
    )


def _append_no_match_log(venue: str, query: str) -> None:
    _append_enrichment_log("NO_MATCH", f"venue={venue!r}\tquery={query!r}")


def _append_error_log(venue: str, err: str) -> None:
    _append_enrichment_log("ERROR", f"venue={venue!r}\t{err[:2000]}")


def _clear_address_rating_coords(df: pd.DataFrame, idx: int) -> None:
    for col in ("formatted_address", "postcode", "lat", "lng", "google_rating", "google_review_count"):
        if col in df.columns:
            df.loc[idx, col] = pd.NA


def _nan_out_google_details_for_row(df: pd.DataFrame, idx: int) -> None:
    """Clear Google-derived columns before applying a partial LOW write."""
    for col in NEW_COLUMNS:
        if col in (
            "google_name",
            "fuzzy_match_score",
            "place_id",
            "places_search_query",
            "business_status",
            "website_from_google",
            "phone_local",
            "opening_hours",
            "google_primary_type",
            "website_needs_review",
            "enrichment_date",
        ):
            continue
        if col in df.columns:
            df.loc[idx, col] = pd.NA


def load_workbook() -> pd.DataFrame:
    xl = pd.ExcelFile(EXCEL_PATH, engine="openpyxl")
    sheet = _pick_sheet(xl)
    LOG.info("Reading sheet: %s", sheet.encode("unicode_escape").decode())
    df = pd.read_excel(xl, sheet_name=sheet, engine="openpyxl")
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _load_or_init_dataframe() -> pd.DataFrame:
    if OUTPUT_PATH.exists():
        LOG.info("Resuming from existing output: %s", OUTPUT_PATH)
        df = pd.read_excel(OUTPUT_PATH, engine="openpyxl")
        # Preserve lowercase enrichment column names; keep source headers trimmed.
        df.columns = [str(c).strip() for c in df.columns]
        return df
    return load_workbook()


def _flag_wedshed_website(df: pd.DataFrame) -> None:
    if "website_needs_review" not in df.columns:
        df["website_needs_review"] = False
    if "WEBSITE" not in df.columns:
        return
    w = df["WEBSITE"].astype(str).str.lower()
    df.loc[w.str.contains("wedshed.com.au", na=False), "website_needs_review"] = True


def _load_checkpoint_indices() -> set[int]:
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        cp = pd.read_csv(CHECKPOINT_PATH)
        if "row_index" not in cp.columns:
            return set()
        return set(int(x) for x in cp["row_index"].dropna().tolist())
    except Exception:  # noqa: BLE001
        LOG.warning("Could not read checkpoint; starting fresh")
        return set()


def _write_checkpoint(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows)
    if CHECKPOINT_PATH.exists():
        old = pd.read_csv(CHECKPOINT_PATH)
        merged = pd.concat([old, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["row_index"], keep="last")
    else:
        merged = new_df
    merged.to_csv(CHECKPOINT_PATH, index=False)


def _load_google_api_key() -> str:
    for path in (_ROOT / ".env.local", _ROOT / "env.local", _ROOT / ".env"):
        if path.is_file():
            load_dotenv(path, override=False, encoding="utf-8")
    key = (
        os.getenv("GOOGLE_PLACES_API_KEY")
        or os.getenv("GOOGLE_MAPS_API_KEY")
        or os.getenv("GOOGLE_PLACES_KEY")
        or ""
    ).strip()
    if key:
        return key
    try:
        from data_builder.config import get_settings

        s = get_settings()
        return (s.google_places_api_key or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def _extract_place_row_dict(det: dict[str, Any], candidate_place_id: str) -> dict[str, Any]:
    """Map Place Details JSON to our column names (HIGH/MEDIUM path)."""
    place_id = _place_id_from_resource(det.get("name")) or candidate_place_id
    fmt_addr = str(det.get("formattedAddress") or "")
    adr = str(det.get("adrFormatAddress") or "")
    postcode = _extract_postcode(fmt_addr)
    rating = det.get("rating")
    urt = det.get("userRatingCount")
    biz = str(det.get("businessStatus") or "")
    website = str(det.get("websiteUri") or "")
    phone_nat = str(det.get("nationalPhoneNumber") or "")
    phone_intl = str(det.get("internationalPhoneNumber") or "")
    gurl = str(det.get("googleMapsUri") or "")
    if not gurl and place_id:
        gurl = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

    loc = det.get("location") or {}
    lat = loc.get("latitude")
    lng = loc.get("longitude")

    photos = det.get("photos")
    p1, p2, p3, p4, photo_total = _photo_refs(photos if isinstance(photos, list) else None)

    opening = det.get("regularOpeningHours")
    opening_json = json.dumps(opening, ensure_ascii=False, default=str) if opening else ""

    park = det.get("parkingOptions") if isinstance(det.get("parkingOptions"), dict) else {}
    acc = det.get("accessibilityOptions") if isinstance(det.get("accessibilityOptions"), dict) else {}

    types_list = det.get("types")
    types_json = (
        json.dumps(types_list, ensure_ascii=False)
        if isinstance(types_list, list)
        else "[]"
    )

    review_cols = _extract_review_columns(det.get("reviews") if isinstance(det.get("reviews"), list) else None)

    return {
        "place_id": place_id,
        "google_maps_url": gurl,
        "business_status": biz,
        "formatted_address": fmt_addr,
        "postcode": postcode,
        "adr_address": adr,
        "lat": lat,
        "lng": lng,
        "website_from_google": website,
        "phone_local": phone_nat,
        "phone_international": phone_intl,
        "google_rating": rating,
        "google_review_count": urt,
        "photo_ref_1": p1,
        "photo_ref_2": p2,
        "photo_ref_3": p3,
        "photo_ref_4": p4,
        "total_photo_count": photo_total,
        "opening_hours": opening_json,
        "price_level": _price_level_to_int(det.get("priceLevel")),
        "editorial_summary": _localized_text(det.get("editorialSummary")),
        "google_primary_type": str(det.get("primaryType") or ""),
        "google_types_json": types_json,
        "wheelchair_accessible_entrance": _bool_or_na(acc.get("wheelchairAccessibleEntrance")),
        "wheelchair_accessible_parking": _bool_or_na(acc.get("wheelchairAccessibleParking")),
        "wheelchair_accessible_restroom": _bool_or_na(acc.get("wheelchairAccessibleRestroom")),
        "wheelchair_accessible_seating": _bool_or_na(acc.get("wheelchairAccessibleSeating")),
        "has_outdoor_seating": _bool_or_na(det.get("outdoorSeating")),
        "has_live_music": _bool_or_na(det.get("liveMusic")),
        "good_for_groups": _bool_or_na(det.get("goodForGroups")),
        "serves_wine": _bool_or_na(det.get("servesWine")),
        "serves_beer": _bool_or_na(det.get("servesBeer")),
        "is_reservable": _bool_or_na(det.get("reservable")),
        "parking_free_lot": _parking_bool(park, "freeParkingLot"),
        "parking_paid": _parking_bool(park, "paidParkingLot"),
        "parking_valet": _parking_bool(park, "valetParking"),
        "parking_street": _parking_bool(park, "freeStreetParking"),
        **review_cols,
    }


def _apply_low_confidence_partial(
    df: pd.DataFrame,
    idx: int,
    *,
    google_name: str,
    fuzzy_score: int,
    place_id: str,
    used_query: str,
    biz: str,
    website: str,
    phone_local: str,
    opening_json: str,
    primary_type: str,
) -> None:
    _nan_out_google_details_for_row(df, idx)
    df.loc[idx, "google_name"] = google_name
    df.loc[idx, "fuzzy_match_score"] = fuzzy_score
    df.loc[idx, "place_id"] = place_id
    df.loc[idx, "places_search_query"] = used_query
    df.loc[idx, "business_status"] = biz
    df.loc[idx, "website_from_google"] = website
    df.loc[idx, "phone_local"] = phone_local
    df.loc[idx, "opening_hours"] = opening_json
    df.loc[idx, "google_primary_type"] = primary_type
    _clear_address_rating_coords(df, idx)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    api_key = _load_google_api_key()
    if not api_key:
        raise SystemExit(
            "No Google Places API key found. Set GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY "
            "in .env.local (or env.local)."
        )

    today = date.today().isoformat()
    df = _load_or_init_dataframe()
    for col in NEW_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    _flag_wedshed_website(df)

    processed = _load_checkpoint_indices()
    to_process: list[int] = []
    for idx in df.index:
        if int(idx) in processed:
            continue
        if _row_already_enriched(df, idx):
            continue
        to_process.append(idx)

    if TEST_MODE:
        to_process = to_process[:5]

    total_target = len(to_process)
    LOG.info("Venues to process this run: %s (checkpoint already had %s)", total_target, len(processed))

    n_high = n_medium = n_low = no_match = errors = 0
    permanently_closed_names: list[str] = []
    checkpoint_buffer: list[dict[str, Any]] = []
    completed_in_run = 0

    def flush_checkpoint() -> None:
        nonlocal checkpoint_buffer
        if checkpoint_buffer:
            _write_checkpoint(checkpoint_buffer)
            checkpoint_buffer = []

    def maybe_save_progress(force: bool = False) -> None:
        nonlocal completed_in_run, checkpoint_buffer
        if force or len(checkpoint_buffer) >= CHECKPOINT_EVERY:
            flush_checkpoint()
            try:
                df.to_excel(OUTPUT_PATH, index=False, engine="openpyxl")
            except Exception as e:  # noqa: BLE001
                LOG.error("Failed to write Excel snapshot: %s", e)

    with httpx.Client() as client:
        for idx in to_process:
            row = df.loc[idx]
            name = row.get("NAME", "")
            region = row.get("REGION", "")
            state = row.get("STATE", "")
            used_query = ""
            outcome = "ERROR"
            place_id_out = ""
            fuzzy_out: int | None = None
            name_s = ""

            try:
                if pd.isna(name) or not str(name).strip():
                    LOG.warning("Row %s: empty NAME, skipping", idx)
                    errors += 1
                    outcome = "ERROR"
                    _append_error_log("(empty NAME)", "empty NAME")
                    checkpoint_buffer.append(
                        {"row_index": int(idx), "place_id": "", "outcome": outcome, "fuzzy_score": ""}
                    )
                    completed_in_run += 1
                    if completed_in_run % PROGRESS_EVERY == 0:
                        print(
                            f"Progress: {completed_in_run}/{total_target} | "
                            f"HIGH: {n_high} | MEDIUM: {n_medium} | LOW: {n_low} | "
                            f"NO_MATCH: {no_match} | Errors: {errors}"
                        )
                    maybe_save_progress()
                    continue

                name_s = str(name).strip()
                primary_q, fallback_q = _build_queries(
                    name_s,
                    str(region) if not pd.isna(region) else "",
                    str(state) if not pd.isna(state) else "",
                )
                used_query = primary_q

                ts = _places_text_search(client, primary_q, api_key)
                if ts.get("_error"):
                    LOG.error("Text search failed query=%s err=%s", primary_q, ts["_error"])
                    errors += 1
                    outcome = "ERROR"
                    _append_error_log(name_s, f"text_search: {ts['_error']}")
                    df.loc[idx, "places_search_query"] = primary_q
                    checkpoint_buffer.append(
                        {
                            "row_index": int(idx),
                            "place_id": "",
                            "outcome": outcome,
                            "fuzzy_score": "",
                        }
                    )
                    completed_in_run += 1
                    if completed_in_run % PROGRESS_EVERY == 0:
                        print(
                            f"Progress: {completed_in_run}/{total_target} | "
                            f"HIGH: {n_high} | MEDIUM: {n_medium} | LOW: {n_low} | "
                            f"NO_MATCH: {no_match} | Errors: {errors}"
                        )
                    maybe_save_progress()
                    continue

                places_list = ts.get("places") or []
                if not places_list:
                    ts2 = _places_text_search(client, fallback_q, api_key)
                    if ts2.get("_error"):
                        LOG.error(
                            "Fallback text search failed query=%s err=%s",
                            fallback_q,
                            ts2["_error"],
                        )
                        errors += 1
                        outcome = "ERROR"
                        _append_error_log(name_s, f"fallback_text_search: {ts2['_error']}")
                        df.loc[idx, "places_search_query"] = fallback_q
                        checkpoint_buffer.append(
                            {
                                "row_index": int(idx),
                                "place_id": "",
                                "outcome": outcome,
                                "fuzzy_score": "",
                            }
                        )
                        completed_in_run += 1
                        if completed_in_run % PROGRESS_EVERY == 0:
                            print(
                                f"Progress: {completed_in_run}/{total_target} | "
                                f"HIGH: {n_high} | MEDIUM: {n_medium} | LOW: {n_low} | "
                                f"NO_MATCH: {no_match} | Errors: {errors}"
                            )
                        maybe_save_progress()
                        continue
                    places_list = ts2.get("places") or []
                    used_query = fallback_q

                if not places_list:
                    LOG.info("%s → (no match) | —", name_s)
                    no_match += 1
                    outcome = "NO_MATCH"
                    _append_no_match_log(name_s, used_query)
                    df.loc[idx, "places_search_query"] = used_query
                    df.loc[idx, "places_match_confidence"] = "NO_MATCH"
                    df.loc[idx, "google_name"] = ""
                    df.loc[idx, "fuzzy_match_score"] = pd.NA
                    checkpoint_buffer.append(
                        {
                            "row_index": int(idx),
                            "place_id": "",
                            "outcome": outcome,
                            "fuzzy_score": "",
                        }
                    )
                    completed_in_run += 1
                    if completed_in_run % PROGRESS_EVERY == 0:
                        print(
                            f"Progress: {completed_in_run}/{total_target} | "
                            f"HIGH: {n_high} | MEDIUM: {n_medium} | LOW: {n_low} | "
                            f"NO_MATCH: {no_match} | Errors: {errors}"
                        )
                    maybe_save_progress()
                    continue

                top = places_list[0]
                resource_name = top.get("name") or ""
                candidate_place_id = _place_id_from_resource(resource_name) or str(
                    top.get("id") or ""
                ).strip()
                if not candidate_place_id:
                    no_match += 1
                    outcome = "NO_MATCH"
                    _append_no_match_log(name_s, used_query)
                    df.loc[idx, "places_search_query"] = used_query
                    df.loc[idx, "places_match_confidence"] = "NO_MATCH"
                    df.loc[idx, "google_name"] = ""
                    df.loc[idx, "fuzzy_match_score"] = pd.NA
                    checkpoint_buffer.append(
                        {
                            "row_index": int(idx),
                            "place_id": "",
                            "outcome": outcome,
                            "fuzzy_score": "",
                        }
                    )
                    completed_in_run += 1
                    if completed_in_run % PROGRESS_EVERY == 0:
                        print(
                            f"Progress: {completed_in_run}/{total_target} | "
                            f"HIGH: {n_high} | MEDIUM: {n_medium} | LOW: {n_low} | "
                            f"NO_MATCH: {no_match} | Errors: {errors}"
                        )
                    maybe_save_progress()
                    continue

                det = _places_details(client, candidate_place_id, api_key)
                if det.get("_error"):
                    LOG.error(
                        "Place Details failed place_id=%s err=%s",
                        candidate_place_id,
                        det["_error"],
                    )
                    errors += 1
                    outcome = "ERROR"
                    _append_error_log(name_s, f"details: {det['_error']}")
                    df.loc[idx, "places_search_query"] = used_query
                    checkpoint_buffer.append(
                        {
                            "row_index": int(idx),
                            "place_id": candidate_place_id,
                            "outcome": outcome,
                            "fuzzy_score": "",
                        }
                    )
                    completed_in_run += 1
                    if completed_in_run % PROGRESS_EVERY == 0:
                        print(
                            f"Progress: {completed_in_run}/{total_target} | "
                            f"HIGH: {n_high} | MEDIUM: {n_medium} | LOW: {n_low} | "
                            f"NO_MATCH: {no_match} | Errors: {errors}"
                        )
                    maybe_save_progress()
                    continue

                google_name = (_display_name_text(det) or "").strip()
                if not google_name:
                    google_name = (str(det.get("formattedAddress") or ""))[:240]

                fuzzy_score = int(fuzz.token_sort_ratio(name_s, google_name)) if google_name else 0
                fuzzy_out = fuzzy_score
                df.loc[idx, "google_name"] = google_name
                df.loc[idx, "fuzzy_match_score"] = fuzzy_score
                df.loc[idx, "places_search_query"] = used_query

                place_id = _place_id_from_resource(det.get("name")) or candidate_place_id
                place_id_out = place_id
                biz = str(det.get("businessStatus") or "")
                website = str(det.get("websiteUri") or "")
                phone_nat = str(det.get("nationalPhoneNumber") or "")
                opening = det.get("regularOpeningHours")
                opening_json = (
                    json.dumps(opening, ensure_ascii=False, default=str) if opening else ""
                )
                primary_type = str(det.get("primaryType") or "")

                if "CLOSED_PERMANENTLY" in str(biz).upper():
                    permanently_closed_names.append(name_s)

                if fuzzy_score < FUZZY_THRESHOLD:
                    _append_low_confidence_log(name_s, google_name, fuzzy_score)
                    _apply_low_confidence_partial(
                        df,
                        idx,
                        google_name=google_name,
                        fuzzy_score=fuzzy_score,
                        place_id=place_id,
                        used_query=used_query,
                        biz=biz,
                        website=website,
                        phone_local=phone_nat,
                        opening_json=opening_json,
                        primary_type=primary_type,
                    )
                    df.loc[idx, "places_match_confidence"] = "LOW"
                    n_low += 1
                    outcome = "LOW"
                    LOG.info(
                        "%s → LOW fuzzy=%s (google_name=%r)",
                        name_s,
                        fuzzy_score,
                        (google_name[:80] + "…") if len(google_name) > 80 else google_name,
                    )
                else:
                    payload = _extract_place_row_dict(det, candidate_place_id)
                    for k, v in payload.items():
                        if k in df.columns:
                            df.loc[idx, k] = v
                    conf = _match_confidence_high_medium(name_s, google_name)
                    df.loc[idx, "places_match_confidence"] = conf
                    if conf == "HIGH":
                        n_high += 1
                    else:
                        n_medium += 1
                    outcome = conf
                    r_str = f"{payload.get('google_rating')}" if payload.get("google_rating") is not None else "—"
                    c_str = f"{payload.get('google_review_count')}" if payload.get("google_review_count") is not None else "—"
                    LOG.info(
                        "%s → %s | %s (%s) [%s]",
                        name_s,
                        str(payload.get("formatted_address") or "") or "—",
                        r_str,
                        c_str,
                        conf,
                    )

                checkpoint_buffer.append(
                    {
                        "row_index": int(idx),
                        "place_id": place_id_out,
                        "outcome": outcome,
                        "fuzzy_score": fuzzy_out if fuzzy_out is not None else "",
                    }
                )
                completed_in_run += 1
                if completed_in_run % PROGRESS_EVERY == 0:
                    print(
                        f"Progress: {completed_in_run}/{total_target} | "
                        f"HIGH: {n_high} | MEDIUM: {n_medium} | LOW: {n_low} | "
                        f"NO_MATCH: {no_match} | Errors: {errors}"
                    )
                maybe_save_progress()

            except Exception as e:  # noqa: BLE001
                LOG.exception("Row %s error: %s", idx, e)
                errors += 1
                outcome = "ERROR"
                _append_error_log(name_s or str(idx), traceback.format_exc())
                if used_query:
                    df.loc[idx, "places_search_query"] = used_query
                checkpoint_buffer.append(
                    {
                        "row_index": int(idx),
                        "place_id": place_id_out or "",
                        "outcome": outcome,
                        "fuzzy_score": fuzzy_out if fuzzy_out is not None else "",
                    }
                )
                completed_in_run += 1
                if completed_in_run % PROGRESS_EVERY == 0:
                    print(
                        f"Progress: {completed_in_run}/{total_target} | "
                        f"HIGH: {n_high} | MEDIUM: {n_medium} | LOW: {n_low} | "
                        f"NO_MATCH: {no_match} | Errors: {errors}"
                    )
                maybe_save_progress()

    df["enrichment_date"] = today
    maybe_save_progress(force=True)
    LOG.info("Wrote %s", OUTPUT_PATH)

    total_venues = len(df)
    wcol = next((c for c in df.columns if str(c).lower() == "website_needs_review"), None)
    wedshed_flags = (
        int(df[wcol].fillna(False).astype(bool).sum()) if wcol is not None else 0
    )
    outdoor_ct = int(df["has_outdoor_seating"].fillna(False).eq(True).sum()) if "has_outdoor_seating" in df.columns else 0
    parking_mask = pd.Series(False, index=df.index)
    for c in ("parking_free_lot", "parking_paid", "parking_valet", "parking_street"):
        if c in df.columns:
            parking_mask = parking_mask | df[c].fillna(False).eq(True)
    parking_ct = int(parking_mask.sum())
    editorial_ct = 0
    if "editorial_summary" in df.columns:
        editorial_ct = int(df["editorial_summary"].fillna("").astype(str).str.len().gt(0).sum())
    photo_refs_total = 0
    for c in ("photo_ref_1", "photo_ref_2", "photo_ref_3", "photo_ref_4"):
        if c in df.columns:
            photo_refs_total += int(df[c].fillna("").astype(str).str.strip().ne("").sum())

    def pct(part: int, whole: int) -> str:
        if whole <= 0:
            return "0.0%"
        return f"{100.0 * part / whole:.1f}%"

    print("\n--- Final summary ---")
    print(f"Total venues in sheet:  {total_venues}")
    print(f"Venues processed (run): {completed_in_run}")
    print(f"Matched HIGH:           {n_high}  ({pct(n_high, total_venues)} of sheet)")
    print(f"Matched MEDIUM:         {n_medium}  ({pct(n_medium, total_venues)} of sheet)")
    print(f"Matched LOW:            {n_low}  ({pct(n_low, total_venues)} of sheet) ← manual review")
    print(f"No match:               {no_match}  ({pct(no_match, total_venues)} of sheet)")
    print(f"Errors:                 {errors}  ({pct(errors, total_venues)} of sheet)")
    print(f"WedShed URLs flagged:   {wedshed_flags}")
    print(f"Permanently closed:     {len(permanently_closed_names)}")
    if permanently_closed_names:
        print("  Names:", "; ".join(permanently_closed_names[:40]))
        if len(permanently_closed_names) > 40:
            print(f"  … and {len(permanently_closed_names) - 40} more")
    print(f"Has outdoor seating:    {outdoor_ct}")
    print(f"Has parking info:       {parking_ct}")
    print(f"Has editorial summary:  {editorial_ct}")
    print(f"Total photos (refs):    {photo_refs_total} refs across venues (first 4 per venue)")


if __name__ == "__main__":
    main()
