"""Google Places (New) + Claude sentiment for active celebrants in Supabase.

Reads ``public.celebrants`` where ``is_active_market`` is true, ordered by
``content_tier``, Easy Weddings reviews, then directory count. Updates rows in-place.

Run: ``python -m scrapers.celebrant_places_supabase``

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY``, ``GOOGLE_PLACES_API_KEY`` or
``GOOGLE_MAPS_API_KEY``, ``ANTHROPIC_API_KEY``. Apply migration ``011_celebrants_places_reviews.sql`` first.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pandas as pd
from dotenv import load_dotenv
from thefuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

LOG_PATH = _ROOT / "logs" / "celebrant_places.log"
VERIFY = "VERIFY_REQUIRED"
REQUEST_DELAY_S = 0.3
PROGRESS_EVERY = 50
FUZZY_THRESHOLD = 60
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TEXT_SEARCH_FIELD_MASK = (
    "places.id,places.displayName,places.formattedAddress,places.location,places.websiteUri,"
    "places.nationalPhoneNumber,places.rating,places.userRatingCount,places.businessStatus,"
    "places.googleMapsUri,places.primaryType,places.types,places.editorialSummary,places.priceLevel,"
    "places.regularOpeningHours,places.photos,places.reviews,places.accessibilityOptions,"
    "places.goodForGroups,places.servesWine,places.servesBeer,places.parkingOptions,places.outdoorSeating"
)
DETAILS_FIELD_MASK = (
    "id,name,displayName,formattedAddress,location,rating,userRatingCount,"
    "businessStatus,photos,googleMapsUri,websiteUri,nationalPhoneNumber,"
    "regularOpeningHours,priceLevel,editorialSummary,primaryType,types,accessibilityOptions,"
    "outdoorSeating,goodForGroups,servesWine,servesBeer,parkingOptions,reviews"
)

_JSON_BLOCK = re.compile(r"\{[\s\S]*\}")
logging.getLogger("httpx").setLevel(logging.WARNING)
LOG = logging.getLogger("celebrant_places_supabase")


def _setup_file_log() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s"))
    LOG.addHandler(fh)
    LOG.setLevel(logging.INFO)


def _api_key() -> str:
    k = (os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not k:
        raise RuntimeError("GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY required")
    return k


def _place_id_from_resource(name: str | None) -> str:
    if not name:
        return ""
    if name.startswith("places/"):
        return name.split("/", 1)[1]
    return name


def _display_name_text(obj: dict[str, Any] | None) -> str:
    if not obj:
        return ""
    dn = obj.get("displayName")
    if isinstance(dn, dict):
        return str(dn.get("text") or "")
    return str(obj.get("name") or "")


def _text_search(client: httpx.Client, query: str, key: str) -> dict[str, Any]:
    r = client.post(
        TEXT_SEARCH_URL,
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": key,
            "X-Goog-FieldMask": TEXT_SEARCH_FIELD_MASK,
        },
        json={"textQuery": query},
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json()


def _place_details(client: httpx.Client, place_id: str, key: str) -> dict[str, Any]:
    pid = quote(place_id, safe="")
    url = f"https://places.googleapis.com/v1/places/{pid}"
    r = client.get(
        url,
        headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": DETAILS_FIELD_MASK},
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json()


def _price_level_int(raw: Any) -> int | None:
    if raw is None or raw == "" or str(raw) == "PRICE_LEVEL_UNSPECIFIED":
        return None
    if isinstance(raw, int) and 0 <= raw <= 4:
        return raw
    m = {
        "PRICE_LEVEL_FREE": 0,
        "PRICE_LEVEL_INEXPENSIVE": 1,
        "PRICE_LEVEL_MODERATE": 2,
        "PRICE_LEVEL_EXPENSIVE": 3,
        "PRICE_LEVEL_VERY_EXPENSIVE": 4,
    }
    return m.get(str(raw))


def _photo_refs_three(photos: list[dict[str, Any]] | None) -> tuple[str, str, str]:
    out = ["", "", ""]
    if not photos:
        return "", "", ""
    for i, ph in enumerate(photos[:3]):
        out[i] = str(ph.get("name") or ph.get("googleMapsUri") or "")
    return out[0], out[1], out[2]


def _extract_reviews(reviews: list[dict[str, Any]] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    reviews = reviews or []
    for i in range(5):
        n = i + 1
        if i < len(reviews):
            rv = reviews[i]
            t = rv.get("text") or {}
            text = str(t.get("text") or "") if isinstance(t, dict) else str(t or "")
            auth = rv.get("authorAttribution") or {}
            author = str(auth.get("displayName") or "") if isinstance(auth, dict) else ""
            rating_rv = rv.get("rating")
            pub = str(rv.get("publishTime") or "")
        else:
            text, author, rating_rv, pub = "", "", None, ""
        out[f"review_text_{n}"] = text or None
        out[f"review_author_{n}"] = author or None
        out[f"review_rating_{n}"] = str(rating_rv) if rating_rv is not None else None
        out[f"review_date_{n}"] = pub or None
    return out


def _claude_celebrant_sentiment(api_key: str, review_texts: list[str]) -> dict[str, Any] | None:
    if not review_texts or not api_key:
        return None
    try:
        import anthropic
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("Install anthropic package") from e
    joined = "\n---\n".join(f"Review {i + 1}: {t}" for i, t in enumerate(review_texts) if t.strip())
    if not joined.strip():
        return None
    prompt = (
        "Analyse these celebrant reviews. Return JSON only, no other text:\n"
        '{"sentiment_score": 0.0-1.0, "top_themes": [max 3 strings], '
        '"red_flags": [array, empty if none], '
        '"one_line_summary": "max 20 words"}\n\n'
        f"{joined}"
    )
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    text = ""
    for block in msg.content:
        if hasattr(block, "text"):
            text += block.text
    text = text.strip()
    m = _JSON_BLOCK.search(text)
    if not m:
        return None
    try:
        data = json.loads(m.group())
    except json.JSONDecodeError:
        return None
    try:
        score_f = float(data.get("sentiment_score", 0))
        score_f = max(0.0, min(1.0, score_f))
    except (TypeError, ValueError):
        score_f = None
    themes = data.get("top_themes") if isinstance(data.get("top_themes"), list) else []
    flags = data.get("red_flags") if isinstance(data.get("red_flags"), list) else []
    summary = str(data.get("one_line_summary") or "").strip()
    return {
        "sentiment_score": score_f,
        "sentiment_themes": json.dumps([str(x) for x in themes[:3]]),
        "sentiment_red_flags": json.dumps([str(x) for x in flags]),
        "sentiment_summary": summary or None,
    }


def _omit_none(patch: dict[str, Any]) -> dict[str, Any]:
    """PostgREST JSON null would violate NOT NULL text columns; drop unknowns."""
    return {k: v for k, v in patch.items() if v is not None}


def _quality_score(row: dict[str, Any], google_rating: float | None) -> int:
    s = 0.0
    if google_rating is not None and google_rating == google_rating:
        s += (google_rating / 5.0) * 40.0
    try:
        ew = float(str(row.get("easy_weddings_rating") or "").replace("VERIFY_REQUIRED", ""))
        if ew == ew and 0 <= ew <= 5:
            s += (ew / 5.0) * 30.0
    except ValueError:
        pass
    try:
        dlc = int(float(str(row.get("directory_listing_count") or "0")))
    except ValueError:
        dlc = 0
    s += (min(dlc, 3) / 3.0) * 20.0
    ab = str(row.get("abia_winner", "")).lower()
    if ab in ("true", "1", "yes", "y"):
        s += 10.0
    return int(round(min(100.0, s)))


def _tier_rank(val: Any) -> int:
    t = str(val or "").strip().lower()
    order = ("featured", "premium", "standard", "basic", "directory", "")
    try:
        return order.index(t) if t in order else 50
    except ValueError:
        return 50


def _fetch_active_celebrants(client: Any) -> list[dict[str, Any]]:
    """Paginated select (PostgREST max 1000 per request)."""
    page = 0
    page_size = 1000
    all_rows: list[dict[str, Any]] = []
    while True:
        q = (
            client.table("celebrants")
            .select("*")
            .eq("is_active_market", True)
            .order("celebrant_id")
            .range(page, page + page_size - 1)
        )
        resp = q.execute()
        batch = getattr(resp, "data", None) or []
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        page += page_size
    df = pd.DataFrame(all_rows)
    if df.empty:
        return []
    df["_tier"] = df["content_tier"].map(_tier_rank) if "content_tier" in df.columns else 99
    df["_ewrc"] = pd.to_numeric(df.get("easy_weddings_review_count", 0), errors="coerce")
    df["_dlc"] = pd.to_numeric(df.get("directory_listing_count", 0), errors="coerce")
    df = df.sort_values(by=["_tier", "_ewrc", "_dlc"], ascending=[True, False, False], na_position="last")
    return df.drop(columns=["_tier", "_ewrc", "_dlc"], errors="ignore").to_dict("records")


def _details_to_update(
    det: dict[str, Any],
    *,
    fuzzy_score: int,
    query_used: str,
    row: dict[str, Any],
    anth_key: str,
) -> dict[str, Any]:
    pid = _place_id_from_resource(str(det.get("name") or det.get("id") or ""))
    gname = _display_name_text(det)
    addr = str(det.get("formattedAddress") or "")
    loc = det.get("location") or {}
    lat = None
    lng = None
    if isinstance(loc, dict):
        lat = loc.get("latitude")
        lng = loc.get("longitude")
    rating = det.get("rating")
    try:
        rf = float(rating) if rating is not None else None
    except (TypeError, ValueError):
        rf = None
    urc = det.get("userRatingCount")
    try:
        urci = int(urc) if urc is not None else None
    except (TypeError, ValueError):
        urci = None
    p1, p2, p3 = _photo_refs_three(det.get("photos") if isinstance(det.get("photos"), list) else None)
    rev_cols = _extract_reviews(det.get("reviews") if isinstance(det.get("reviews"), list) else None)
    acc = det.get("accessibilityOptions") or {}
    wc = None
    if isinstance(acc, dict):
        wc = acc.get("wheelchairAccessibleEntrance")
        if wc is None:
            wc = acc.get("wheelchairAccessibleParking")
    oh = det.get("regularOpeningHours")
    oh_s = json.dumps(oh) if oh is not None else None
    types = det.get("types")
    types_s = json.dumps(types) if isinstance(types, list) else None
    pt = det.get("primaryType")
    pt_s = str(pt) if pt is not None else None
    ed = det.get("editorialSummary") or {}
    ed_text = str(ed.get("text") or "") if isinstance(ed, dict) else ""
    if fuzzy_score >= 85:
        conf = "HIGH"
    elif fuzzy_score >= 70:
        conf = "MEDIUM"
    else:
        conf = "LOW"
    texts = [str(rev_cols.get(f"review_text_{i}") or "") for i in range(1, 6)]
    sent = _claude_celebrant_sentiment(anth_key, [t for t in texts if t]) if anth_key else None
    qscore = _quality_score({**row, "easy_weddings_rating": row.get("easy_weddings_rating")}, rf)

    patch: dict[str, Any] = {
        "google_place_id": pid or VERIFY,
        "google_name": gname or None,
        "google_address": addr or None,
        "lat": lat,
        "lng": lng,
        "website_from_google": str(det.get("websiteUri") or "").strip() or None,
        "google_phone": str(det.get("nationalPhoneNumber") or "").strip() or None,
        "google_maps_url": str(det.get("googleMapsUri") or "").strip() or None,
        "business_status": str(det.get("businessStatus") or "").strip() or None,
        "google_primary_type": pt_s,
        "google_types_json": types_s,
        "editorial_summary": ed_text or None,
        "price_level": _price_level_int(det.get("priceLevel")),
        "opening_hours": oh_s,
        "google_rating": f"{rf:.1f}" if rf is not None else VERIFY,
        "google_review_count": str(urci) if urci is not None else VERIFY,
        "photo_ref_1": p1 or None,
        "photo_ref_2": p2 or None,
        "photo_ref_3": p3 or None,
        "good_for_groups": det.get("goodForGroups"),
        "serves_wine": det.get("servesWine"),
        "serves_beer": det.get("servesBeer"),
        "outdoor_seating": det.get("outdoorSeating"),
        "wheelchair_accessible": wc,
        "places_match_confidence": conf,
        "places_enriched_date": date.today().isoformat(),
        "celebrant_quality_score": qscore,
        "last_updated_source": "google_places_supabase",
        "last_places_enrich_at": datetime.now(timezone.utc).isoformat(),
    }
    wuri = str(det.get("websiteUri") or "").strip()
    if wuri:
        patch["website_from_places"] = wuri
    phone_pl = str(det.get("nationalPhoneNumber") or "").strip()
    if phone_pl:
        patch["phone_from_places"] = phone_pl
    patch.update(rev_cols)
    if sent:
        for sk, sv in sent.items():
            if sv is not None:
                patch[sk] = sv
    _ = query_used
    return _omit_none(patch)


def run() -> int:
    _setup_file_log()
    from data_builder.config import get_settings

    st = get_settings()
    url = (st.supabase_url or "").strip()
    skey = (st.supabase_service_role_key or "").strip()
    anth = (st.anthropic_api_key or os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not url or not skey:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required")
        return 1
    try:
        places_key = _api_key()
    except RuntimeError as e:
        LOG.error("%s", e)
        return 1
    from supabase import create_client

    sb = create_client(url, skey)
    rows = _fetch_active_celebrants(sb)
    n = len(rows)
    LOG.info("Loaded %s active celebrants from Supabase", n)
    if not n:
        print("No active celebrants to enrich.")
        return 0
    matched = no_match = errors = 0
    with httpx.Client() as http:
        for i, row in enumerate(rows):
            cid = str(row.get("celebrant_id") or "").strip()
            fn = str(row.get("full_name") or "").strip()
            stc = str(row.get("state") or "").strip()
            if not cid or not fn:
                continue
            if (i + 1) % PROGRESS_EVERY == 0 or (i + 1) == n:
                msg = f"Progress: {i + 1}/{n} | Matched: {matched} | No match: {no_match}"
                if errors:
                    msg += f" | Errors: {errors}"
                LOG.info(msg)
                print(msg, flush=True)
            q1 = f"{fn} celebrant {stc} Australia".strip()
            q2 = f"{fn} wedding celebrant Australia".strip()
            chosen: dict[str, Any] | None = None
            best_score = 0
            query_used = ""
            try:
                for q in (q1, q2):
                    data = _text_search(http, q, places_key)
                    time.sleep(REQUEST_DELAY_S)
                    places = data.get("places") or []
                    if not places:
                        continue
                    top = places[0]
                    cand = _display_name_text(top)
                    sc = fuzz.token_sort_ratio(fn.lower(), (cand or "").lower())
                    if sc > best_score:
                        best_score = sc
                        chosen = top
                        query_used = q
                    if sc >= FUZZY_THRESHOLD:
                        break
                if not chosen or best_score < FUZZY_THRESHOLD:
                    no_match += 1
                    LOG.info("NO_MATCH celebrant_id=%s score=%s", cid, best_score)
                    time.sleep(REQUEST_DELAY_S)
                    continue
                raw_id = str(chosen.get("name") or chosen.get("id") or "")
                pid = _place_id_from_resource(raw_id)
                det = _place_details(http, pid, places_key)
                time.sleep(REQUEST_DELAY_S)
                patch = _details_to_update(
                    det, fuzzy_score=best_score, query_used=query_used, row=row, anth_key=anth
                )
                time.sleep(REQUEST_DELAY_S)
                sb.table("celebrants").update(patch).eq("celebrant_id", cid).execute()
                matched += 1
            except Exception as e:  # noqa: BLE001
                errors += 1
                LOG.exception("Error celebrant_id=%s: %s", cid, e)
                time.sleep(REQUEST_DELAY_S)
    print(f"Done. Matched: {matched}, no match: {no_match}, errors: {errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
