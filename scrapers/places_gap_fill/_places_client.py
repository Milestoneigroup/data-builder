"""Places API (New) client + Tier 1 field extraction helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import httpx
from thefuzz import fuzz

from ._framework import (
    HIGH_CONFIDENCE_THRESHOLD,
    LOW_CONFIDENCE_THRESHOLD,
    NAME_DOMINANCE_THRESHOLD,
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


_AU_STATE_ALIASES: dict[str, str] = {
    "new south wales": "NSW",
    "nsw": "NSW",
    "victoria": "VIC",
    "vic": "VIC",
    "queensland": "QLD",
    "qld": "QLD",
    "south australia": "SA",
    "sa": "SA",
    "western australia": "WA",
    "wa": "WA",
    "tasmania": "TAS",
    "tas": "TAS",
    "northern territory": "NT",
    "nt": "NT",
    "australian capital territory": "ACT",
    "act": "ACT",
}


def normalise_au_state(value: str | None) -> str:
    """Normalise Australian state names to a short upper token for comparison."""

    if not value:
        return ""
    s = str(value).strip().lower()
    if not s:
        return ""
    return _AU_STATE_ALIASES.get(s, str(value).strip().upper())


def state_from_place_details(details: dict[str, Any]) -> str | None:
    """Resolve administrative_area_level_1 from Places addressComponents."""

    comps = details.get("addressComponents")
    if not isinstance(comps, list):
        return None
    for item in comps:
        if not isinstance(item, dict):
            continue
        types = item.get("types")
        if not isinstance(types, list):
            continue
        if "administrative_area_level_1" not in types:
            continue
        st = str(item.get("shortText") or item.get("longText") or "").strip()
        norm = normalise_au_state(st)
        return norm or st or None
    return None


def name_dominance_state_ok(our_state: str | None, details: dict[str, Any]) -> bool:
    """State gate for name-dominance bypass: unset row state passes; else must match."""

    ours = normalise_au_state(str(our_state or "").strip())
    if not ours:
        return True
    place_st = state_from_place_details(details)
    if not place_st:
        return False
    return ours == normalise_au_state(place_st)


def _types_blob(primary: str | None, types_list: list[str] | None) -> str:
    parts: list[str] = []
    if primary:
        parts.append(str(primary))
    if types_list:
        parts.extend(str(t) for t in types_list if t)
    return " ".join(parts).lower()


def type_plausible_for_vendor(
    vendor_type: str,
    primary: str | None,
    types_list: list[str] | None,
) -> bool:
    """Heuristic guardrail before accepting a Places match."""

    blob = _types_blob(primary, types_list)
    if vendor_type == "venues":
        hints = (
            "wedding",
            "venue",
            "event",
            "banquet",
            "hotel",
            "lodging",
            "resort",
            "winery",
            "restaurant",
            "golf",
            "club",
            "function",
            "estate",
            "barn",
            "convention",
            "spa",
            "retreat",
            "hall",
            "homestead",
            "chapel",
            "farm",
            "reception",
            "castle",
            "grounds",
        )
        return any(h in blob for h in hints)
    if vendor_type == "photographers":
        if any(
            b in blob
            for b in (
                "gas_station",
                "supermarket",
                "car_dealer",
                "electronics_store",
            )
        ):
            return False
        hints = (
            "photo",
            "portrait",
            "professional_service",
            "videographer",
            "art_gallery",
        )
        return any(h in blob for h in hints)
    if vendor_type == "celebrants":
        if any(
            b in blob
            for b in (
                "gas_station",
                "parking",
                "car_wash",
                "supermarket",
            )
        ):
            return False
        return True
    return True


def query_variations_for_vendor(
    vendor_type: str,
    name: str,
    suburb: str | None,
    state: str | None,
) -> list[str]:
    """Up to five ordered text-query variants (None/empty parts omitted)."""

    n = (name or "").strip()
    st = (state or "").strip()
    su = (suburb or "").strip()
    out: list[str] = []

    def push(q: str | None) -> None:
        if not q or not str(q).strip():
            return
        q = str(q).strip()
        if q not in out:
            out.append(q)

    if vendor_type == "venues":
        if su and st:
            push(f"{n}, {su}, {st}, Australia")
        if st:
            push(f"{n}, {st}, Australia")
            push(f"{n}, wedding venue, {st}")
            push(f"{n}, {st}")
        push(n)
    elif vendor_type == "celebrants":
        if su and st:
            push(f"{n}, {su}, {st}, Australia")
        if st:
            push(f"{n}, wedding celebrant, {st}")
            push(f"{n}, marriage celebrant, {st}")
            push(f"{n}, {st}")
        push(n)
    elif vendor_type == "photographers":
        if su and st:
            push(f"{n}, {su}, {st}, Australia")
        if st:
            push(f"{n}, wedding photographer, {st}")
            push(f"{n}, photography, {st}")
            push(f"{n} photography {st}")
            push(f"{n}, {st}")
        elif n:
            push(n)
    else:
        if n:
            out.append(n)
    return out[:5]


@dataclass(frozen=True)
class FindPlaceResult:
    """Outcome of multi-query Places search + details fetch."""

    details: dict[str, Any] | None
    confidence: float
    query_used: str
    queries_tried: int
    budget_exhausted: bool = False


def find_place_with_fallbacks(
    *,
    name: str,
    state: str | None,
    suburb: str | None,
    vendor_type: str,
    http: Any,
    tracker: Any,
    logger: Any,
    place_id_claimed: Callable[[str], bool],
) -> FindPlaceResult:
    """Return Places details, fuzzy confidence (0–1), and winning text query.

    Tries up to five query variations. The first match with confidence >=
    ``HIGH_CONFIDENCE_THRESHOLD`` wins. If none reach that, the strongest match
    in ``[LOW_CONFIDENCE_THRESHOLD``, ``HIGH_CONFIDENCE_THRESHOLD``) is
    returned for website-only capture.

    Vendors with distinctive names matching at 0.85+ are almost certainly the
    same entity. Generic Google place types (point_of_interest, establishment)
    cause valid matches to be rejected by strict type filtering.
    Name-dominance bypasses this protection where it's not needed.
    """

    queries = query_variations_for_vendor(vendor_type, name, suburb, state)
    include_venue_only = vendor_type == "venues"
    label = (name or "").strip()
    our_state = state
    best_low: tuple[dict[str, Any], float, str] | None = None
    queries_tried = 0

    for query in queries:
        if tracker is not None and not tracker.can_afford(TEXT_SEARCH_COST):
            return FindPlaceResult(
                None,
                0.0,
                "",
                queries_tried,
                budget_exhausted=True,
            )

        queries_tried += 1
        ts_payload = text_search_budgeted(http, tracker, query=query)
        if ts_payload.get("_skipped") == "budget":
            return FindPlaceResult(
                None,
                0.0,
                query,
                queries_tried,
                budget_exhausted=True,
            )

        places = ts_payload.get("places") or []
        if not isinstance(places, list) or not places:
            continue

        scored: list[tuple[float, dict[str, Any]]] = []
        for cand in places:
            if not isinstance(cand, dict):
                continue
            cand_nm = display_name_text(cand)
            pct = fuzz.token_sort_ratio(label.lower(), (cand_nm or "").lower())
            fuzzy = pct / 100.0
            if fuzzy < LOW_CONFIDENCE_THRESHOLD:
                continue
            raw_id = str(cand.get("name") or cand.get("id") or "")
            pid = place_id_from_resource(raw_id)
            if not pid or place_id_claimed(pid):
                continue
            scored.append((fuzzy, cand))

        scored.sort(key=lambda x: -x[0])

        for fuzzy, cand in scored:
            if tracker is not None and not tracker.can_afford(PLACE_DETAILS_COST):
                return FindPlaceResult(
                    None,
                    0.0,
                    query,
                    queries_tried,
                    budget_exhausted=True,
                )
            pid = place_id_from_resource(str(cand.get("name") or cand.get("id") or ""))
            details = place_details_budgeted(
                http,
                tracker,
                place_id=pid,
                include_venue_only=include_venue_only,
            )
            if details.get("_skipped") == "budget":
                return FindPlaceResult(
                    None,
                    0.0,
                    query,
                    queries_tried,
                    budget_exhausted=True,
                )

            primary = str(details.get("primaryType") or "").strip() or None
            types_raw = details.get("types")
            types_list: list[str] | None = (
                [str(t) for t in types_raw]
                if isinstance(types_raw, list)
                else None
            )

            st_ok = name_dominance_state_ok(our_state, details)
            if fuzzy >= NAME_DOMINANCE_THRESHOLD and st_ok:
                logger.info(
                    "name-dominant match: fuzzy=%.2f bypasses type filter",
                    fuzzy,
                )
                return FindPlaceResult(details, fuzzy, query, queries_tried)

            if not type_plausible_for_vendor(vendor_type, primary, types_list):
                continue

            if fuzzy >= HIGH_CONFIDENCE_THRESHOLD:
                return FindPlaceResult(details, fuzzy, query, queries_tried)
            if fuzzy >= LOW_CONFIDENCE_THRESHOLD:
                if best_low is None or fuzzy > best_low[1]:
                    best_low = (details, fuzzy, query)

    if best_low is not None:
        det, conf, qu = best_low
        return FindPlaceResult(det, conf, qu, queries_tried)
    return FindPlaceResult(None, 0.0, "", queries_tried)
