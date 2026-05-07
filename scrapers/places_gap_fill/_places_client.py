"""Places API (New) client + Tier 1 field extraction helpers."""

from __future__ import annotations


import httpx

from ._framework import (
    PLACE_DETAILS_COST,
    TEXT_SEARCH_COST,
    polite_delay,
    places_api_key,
)

# Conceptual parity with Tier 1 product field groups — emitted as Places (New)
# camelCase masks (comma-separated).
UNIVERSAL_FIELD_MASK = (
    "id,name,displayName,formattedAddress,addressComponents,location,"
    "internationalPhoneNumber,nationalPhoneNumber,websiteUri,googleMapsUri,types,"
    "businessStatus,editorialSummary,rating,userRatingCount,primaryType"
)
VENUE_ONLY_FIELD_MASK = "regularOpeningHours,currentOpeningHours,reviews"

TEXT_SEARCH_FIELD_MASK = (
    "places.id,places.displayName,places.formattedAddress,places.location"
)


TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"


def place_id_from_resource(name: str | None) -> str:
    if not name:
        return ""
    s = str(name).strip()
    if s.startswith("places/"):
        return s.split("/", 1)[1]
    return s


def display_name_text(obj: dict[str, Any] | None) -> str:
    if not obj:
        return ""
    dn = obj.get("displayName")
    if isinstance(dn, dict):
        return str(dn.get("text") or "").strip()
    return str(obj.get("name") or "").strip()


def _text_search_inner(
    http: httpx.Client, *, query: str, key: str
) -> dict[str, Any]:
    r = http.post(
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
    body = r.json()
    return body if isinstance(body, dict) else {}


def _place_details_inner(
    http: httpx.Client, *, place_id: str, key: str, field_mask: str
) -> dict[str, Any]:
    from urllib.parse import quote

    pid = quote(place_id_from_resource(place_id), safe="")
    url = f"https://places.googleapis.com/v1/places/{pid}"
    r = http.get(
        url,
        headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": field_mask},
        timeout=60.0,
    )
    r.raise_for_status()
    body = r.json()
    return body if isinstance(body, dict) else {}


def text_search_budgeted(
    http: httpx.Client, tracker: Any, *, query: str
) -> dict[str, Any]:
    key = places_api_key()
    if hasattr(tracker, "can_afford") and not tracker.can_afford(TEXT_SEARCH_COST):
        return {"_skipped": "budget"}
    data = _text_search_inner(http, query=query, key=key)
    if hasattr(tracker, "record_call"):
        tracker.record_call("text_search", TEXT_SEARCH_COST)
    polite_delay()
    return data


def place_details_budgeted(
    http: httpx.Client, tracker: Any, *, place_id: str, include_venue_only: bool
) -> dict[str, Any]:
    key = places_api_key()
    if hasattr(tracker, "can_afford") and not tracker.can_afford(
        PLACE_DETAILS_COST,
    ):
        return {"_skipped": "budget"}
    mask = UNIVERSAL_FIELD_MASK
    if include_venue_only:
        mask = f"{UNIVERSAL_FIELD_MASK},{VENUE_ONLY_FIELD_MASK}"
    data = _place_details_inner(http, place_id=place_id, key=key, field_mask=mask)
    if hasattr(tracker, "record_call"):
        tracker.record_call("place_details", PLACE_DETAILS_COST)
    polite_delay()
    return data


def editorial_plain(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        t = str(raw.get("text") or "").strip()
        return t or None
    s = str(raw).strip()
    return s or None


def extract_universal_fields(
    place_details_response: dict[str, Any],
) -> dict[str, Any]:
    """Map Tier 1 universal Places JSON onto shared vendor column nomenclatures."""
    if place_details_response.get("_skipped"):
        return {}
    det = place_details_response
    pid = place_id_from_resource(str(det.get("name") or det.get("id") or ""))
    gname = display_name_text(det) or None
    addr = str(det.get("formattedAddress") or "").strip() or None
    loc = det.get("location") if isinstance(det.get("location"), dict) else {}
    lat = lng = None
    if isinstance(loc, dict):
        try:
            lat = (
                float(loc["latitude"]) if loc.get("latitude") is not None else None
            )
        except (TypeError, ValueError, KeyError):
            lat = None
        try:
            lng = (
                float(loc["longitude"])
                if loc.get("longitude") is not None
                else None
            )
        except (TypeError, ValueError, KeyError):
            lng = None
    rating = det.get("rating")
    try:
        rf = float(rating) if rating is not None else None
    except (TypeError, ValueError):
        rf = None
    urc_raw = det.get("userRatingCount")
    try:
        urci = int(urc_raw) if urc_raw is not None else None
    except (TypeError, ValueError):
        urci = None

    intl_phone = str(det.get("internationalPhoneNumber") or "").strip()
    nat_phone = str(det.get("nationalPhoneNumber") or "").strip()
    phone_out = intl_phone or nat_phone or None

    comps = det.get("addressComponents")
    ac_json = comps if isinstance(comps, list) else None

    types_raw = det.get("types")
    types_json = types_raw if isinstance(types_raw, list) else None

    return {
        "canonical_place_id": pid,
        "google_name": gname,
        "google_rating": rf,
        "google_review_count": urci,
        "google_address": addr,
        "google_maps_url": (
            str(det.get("googleMapsUri") or "").strip() or None
        ),
        "google_primary_type": (
            str(det.get("primaryType") or "").strip() or None
        ),
        "google_phone": phone_out,
        "website_from_google": str(det.get("websiteUri") or "").strip() or None,
        "business_status": (
            str(det.get("businessStatus") or "").strip() or None
        ),
        "lat": lat,
        "lng": lng,
        "google_types_json": types_json,
        "google_address_components_json": ac_json,
        "google_editorial_summary": editorial_plain(det.get("editorialSummary")),
    }


def _extract_review_slots_top5(reviews_raw: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    reviews_list = reviews_raw if isinstance(reviews_raw, list) else []
    for i in range(5):
        n = i + 1
        if i < len(reviews_list):
            rv = reviews_list[i]
            t = rv.get("text") or {}
            text = str(t.get("text") or "") if isinstance(t, dict) else str(t or "")
            auth = rv.get("authorAttribution") or {}
            author = (
                str(auth.get("displayName") or "")
                if isinstance(auth, dict)
                else ""
            )
            rating_rv = rv.get("rating")
            pub = str(rv.get("publishTime") or "").strip()
        else:
            text, author, rating_rv, pub = "", "", None, ""
        out[f"review_text_{n}"] = text or None
        out[f"review_author_{n}"] = author or None
        rr: Any
        if rating_rv is not None:
            try:
                rf = float(rating_rv)
                rr = int(rf) if rf == int(rf) else rf
            except (TypeError, ValueError):
                rr = None
        else:
            rr = None
        out[f"review_rating_{n}"] = rr
        out[f"review_date_{n}"] = pub or None
    return out


def extract_venue_specific_fields(
    place_details_response: dict[str, Any],
) -> dict[str, Any]:
    """Opening hours blob + Places reviews → ``opening_hours_json`` & review_* slots."""
    if place_details_response.get("_skipped"):
        return {}
    det = place_details_response
    reg = det.get("regularOpeningHours")
    cur = det.get("currentOpeningHours")
    payload: dict[str, Any] = {}
    if isinstance(reg, dict):
        payload["regularOpeningHours"] = reg
    if isinstance(cur, dict):
        payload["currentOpeningHours"] = cur
    opening_hours_json = payload if payload else None

    rev_cols = _extract_review_slots_top5(
        det.get("reviews") if isinstance(det.get("reviews"), list) else [],
    )

    out: dict[str, Any] = {"opening_hours_json": opening_hours_json}
    out.update(rev_cols)
    return out



