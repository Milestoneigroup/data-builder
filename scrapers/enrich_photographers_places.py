"""Google Places (New) enrichment for the photographers v0.3 cohort (``public.photographers``).

Text Search followed by Place Details — augmentation-only writes; curated identity,
location-from-CSV and social URLs are never modified.

Loads ``env.local`` via ``load_dotenv(..., override=True)``. Requires ``GOOGLE_MAPS_API_KEY``,
``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY``.

Run: ``python -m scrapers.enrich_photographers_places``
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import httpx
from dotenv import load_dotenv
from thefuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / "env.local", override=True)

LOG_PATH = _ROOT / "logs" / "enrich_photographers_places.log"
REQUEST_DELAY_S = 0.3
MAX_RUNTIME_S = 60 * 60
MAX_SPEND_USD = 25.0
# Places API (New) — planning rates (adjust if pricing changes).
COST_TEXT_SEARCH_USD = 0.032
COST_DETAILS_USD = 0.04

TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TEXT_SEARCH_FIELD_MASK = "places.id,places.displayName,places.formattedAddress,places.location"
DETAILS_FIELD_MASK = (
    "id,displayName,formattedAddress,location,"
    "websiteUri,nationalPhoneNumber,rating,userRatingCount,"
    "businessStatus,googleMapsUri,primaryType,types,editorialSummary,priceLevel,"
    "regularOpeningHours,photos,reviews"
)

VERIFY = "VERIFY_REQUIRED"

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
LOG = logging.getLogger("enrich_photographers_places")


def _setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s"))
    LOG.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(message)s"))
    LOG.addHandler(sh)
    LOG.setLevel(logging.INFO)


def _api_key() -> str:
    k = (os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or "").strip()
    if not k:
        raise RuntimeError("GOOGLE_MAPS_API_KEY (or GOOGLE_PLACES_API_KEY) required in env.local")
    return k


def _place_id_from_resource(name: str | None) -> str:
    if not name:
        return ""
    s = str(name).strip()
    if s.startswith("places/"):
        return s.split("/", 1)[1]
    return s


def _display_name_text(obj: dict[str, Any] | None) -> str:
    if not obj:
        return ""
    dn = obj.get("displayName")
    if isinstance(dn, dict):
        return str(dn.get("text") or "")
    return str(obj.get("name") or "")


def _sleep() -> None:
    time.sleep(REQUEST_DELAY_S)


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
    pid = quote(_place_id_from_resource(place_id), safe="")
    r = client.get(
        f"https://places.googleapis.com/v1/places/{pid}",
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


def _photo_refs_and_count(
    photos: list[dict[str, Any]] | None,
) -> tuple[str | None, str | None, str | None, int]:
    """First three Places photo resource names plus total gallery length."""
    n = len(photos or [])
    if not photos:
        return None, None, None, 0
    names: list[str | None] = [None, None, None]
    for i, ph in enumerate(photos[:3]):
        nm = ph.get("name")
        names[i] = str(nm).strip() if nm else None
    return names[0], names[1], names[2], n


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
        else:
            text, author, rating_rv = "", "", None
        out[f"review_text_{n}"] = text or None
        out[f"review_author_{n}"] = author or None
        rr: float | int | None
        if rating_rv is not None:
            try:
                rr = int(rating_rv) if float(rating_rv) == int(float(rating_rv)) else float(rating_rv)
            except (TypeError, ValueError):
                rr = None
        else:
            rr = None
        out[f"review_rating_{n}"] = rr
    return out


def _confidence_from_score(score: int) -> str | None:
    if score >= 80:
        return "HIGH"
    if score >= 65:
        return "MEDIUM"
    if score >= 55:
        return "LOW"
    return None


def _needs_curated_website(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return s in ("", VERIFY, "nan") or s.lower() in ("null", "none")


def _clean_query_token(val: Any) -> str:
    s = str(val or "").strip()
    if not s or s == VERIFY or s.lower() in ("nan", "null", "none", "verify_required"):
        return ""
    return s


def _search_label_for_row(row_name: str) -> str:
    """Stable display name used for fuzzy match (curated identity, not rewritten)."""
    return (row_name or "").strip()


def _build_search_queries(name_display: str, suburb: str, state: str) -> list[str]:
    st = _clean_query_token(state)
    su = _clean_query_token(suburb)
    n = (name_display or "").strip()
    out: list[str] = []

    primary = " ".join(p for p in (n, "wedding photographer", st, "Australia") if p).strip()
    if primary:
        out.append(primary)

    if su:
        f1 = " ".join(p for p in (n, "photography", su, st) if p).strip()
        if f1 and f1 not in out:
            out.append(f1)

    f2 = " ".join(p for p in (n, st, "Australia") if p).strip()
    if f2 and f2 not in out:
        out.append(f2)
    return out


def _place_id_taken_by_other(sb: Any, place_id: str, own_pid: str) -> bool:
    norm = _place_id_from_resource(place_id)
    if not norm or norm == VERIFY:
        return True
    r = (
        sb.table("photographers")
        .select("photographer_id")
        .eq("google_place_id", norm)
        .limit(25)
        .execute()
    )
    own = str(own_pid).strip()
    for rec in getattr(r, "data", None) or []:
        if str(rec.get("photographer_id") or "").strip() != own:
            return True
    return False


def _pick_candidate_from_queries(
    http: httpx.Client,
    places_key: str,
    sb: Any,
    *,
    match_name: str,
    queries: list[str],
    photographer_id: str,
    spend: dict[str, float],
    budget_ok_before_search: Callable[[], bool],
    budget_ok_before_details: Callable[[], bool],
) -> tuple[dict[str, Any] | None, int, str, str]:
    """Returns ``(place_json, fuzzy_score, query_used, outcome)``.
    outcome: ``ok`` | ``no_match`` | ``budget``.
    """
    best: tuple[dict[str, Any], int, str] | None = None
    last_q = ""
    for q in queries:
        if not q:
            continue
        if not budget_ok_before_search():
            return None, 0, last_q or q, "budget"
        last_q = q
        try:
            data = _text_search(http, q, places_key)
            spend["usd"] += COST_TEXT_SEARCH_USD
        finally:
            _sleep()

        places = data.get("places") or []
        for p in places:
            cand_name = _display_name_text(p)
            score = int(
                fuzz.token_sort_ratio(match_name.lower(), (cand_name or "").lower())
            )
            if _confidence_from_score(score) is None:
                continue
            raw_id = str(p.get("name") or p.get("id") or "")
            pid = _place_id_from_resource(raw_id)
            if _place_id_taken_by_other(sb, pid, photographer_id):
                continue
            if best is None or score > best[1]:
                best = (p, score, q)
        if best is not None:
            break

    if best is None:
        return None, 0, last_q, "no_match"
    place, score, q_used = best
    if not budget_ok_before_details():
        return None, score, q_used, "budget"
    return place, score, q_used, "ok"


def _details_to_patch(
    det: dict[str, Any],
    *,
    fuzzy_score: int,
    row: dict[str, Any],
) -> dict[str, Any]:
    pid = _place_id_from_resource(str(det.get("name") or det.get("id") or ""))
    gname = _display_name_text(det)
    addr = str(det.get("formattedAddress") or "").strip()
    loc = det.get("location") if isinstance(det.get("location"), dict) else {}
    lat = lng = None
    if isinstance(loc, dict):
        lat = loc.get("latitude")
        lng = loc.get("longitude")
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

    photos_raw = det.get("photos") if isinstance(det.get("photos"), list) else None
    p1, p2, p3, p_total = _photo_refs_and_count(photos_raw)

    oh = det.get("regularOpeningHours")
    oh_out: dict[str, Any] | None = oh if isinstance(oh, dict) else None

    types = det.get("types")

    ed = det.get("editorialSummary") or {}
    ed_text = str(ed.get("text") or "") if isinstance(ed, dict) else ""

    conf_band = _confidence_from_score(fuzzy_score) or "LOW"

    wuri = str(det.get("websiteUri") or "").strip()

    rev_cols = _extract_reviews(
        det.get("reviews") if isinstance(det.get("reviews"), list) else None
    )

    patch: dict[str, Any] = {
        "google_place_id": pid or None,
        "google_name": gname or None,
        "google_address": addr or None,
        "lat": float(lat) if lat is not None else None,
        "lng": float(lng) if lng is not None else None,
        "website_from_google": wuri or None,
        "google_phone": str(det.get("nationalPhoneNumber") or "").strip() or None,
        "google_rating": rf,
        "google_review_count": urci,
        "business_status": str(det.get("businessStatus") or "").strip() or None,
        "google_maps_url": str(det.get("googleMapsUri") or "").strip() or None,
        "google_primary_type": str(det.get("primaryType") or "").strip() or None,
        "google_types_json": types if isinstance(types, list) else None,
        "editorial_summary": ed_text or None,
        "price_level": _price_level_int(det.get("priceLevel")),
        "opening_hours": oh_out,
        "photo_ref_1": p1,
        "photo_ref_2": p2,
        "photo_ref_3": p3,
        "total_photo_count": p_total,
        "places_match_confidence": conf_band,
        "fuzzy_match_score": int(fuzzy_score),
        "places_enriched_date": date.today().isoformat(),
    }
    patch.update(rev_cols)

    if _needs_curated_website(row.get("website")) and wuri:
        patch["website"] = wuri

    return patch


def _fetch_pending_photographers(sb: Any) -> list[dict[str, Any]]:
    """Tier ``A`` first, then ``B``; deterministic ``photographer_id`` order."""

    cols = (
        "photographer_id,name,business_name,suburb,state,website,tier"
    )
    out: list[dict[str, Any]] = []
    page_size = 1000
    for tier in ("A", "B"):
        offset = 0
        while True:
            resp = (
                sb.table("photographers")
                .select(cols)
                .eq("tier", tier)
                .is_("google_place_id", "null")
                .order("photographer_id")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = list(getattr(resp, "data", None) or [])
            if not batch:
                break
            out.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
    return out


def run() -> int:
    _setup_logging()
    try:
        places_key = _api_key()
    except RuntimeError as exc:
        LOG.error("%s", exc)
        return 1

    url = (os.getenv("SUPABASE_URL") or "").strip()
    skey = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not skey:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required in env.local")
        return 1

    from supabase import create_client

    sb = create_client(url, skey)
    rows = _fetch_pending_photographers(sb)
    baseline_pending = len(rows)
    LOG.info(
        "Loaded %s photographer rows pending Places enrichment "
        "(google_place_id IS NULL); tier_order=A then=B",
        baseline_pending,
    )
    print(f"Pending photographers (google_place_id IS NULL): {baseline_pending}")

    if baseline_pending == 0:
        print("Stopping — cohort already enriched (pending = 0). Surface for manual decision.")
        return 0

    run_started = time.monotonic()
    spend = {"usd": 0.0}

    matched = errors = stopped_budget = 0

    def time_ok() -> bool:
        return (time.monotonic() - run_started) < MAX_RUNTIME_S

    def can_afford_text_search() -> bool:
        return time_ok() and (spend["usd"] + COST_TEXT_SEARCH_USD <= MAX_SPEND_USD + 1e-9)

    def can_afford_details() -> bool:
        return time_ok() and (spend["usd"] + COST_DETAILS_USD <= MAX_SPEND_USD + 1e-9)

    with httpx.Client() as http:
        for idx, row in enumerate(rows):
            if not time_ok():
                LOG.warning(
                    "Stopped: sixty-minute runtime cap after %s/%s.",
                    idx,
                    len(rows),
                )
                print("Stopped: sixty-minute runtime cap.")
                break
            photographer_id = str(row.get("photographer_id") or "").strip()
            raw_name = str(row.get("name") or "").strip()
            biz = str(row.get("business_name") or "").strip()
            label = raw_name if raw_name else biz
            st = str(row.get("state") or "").strip()
            suburb = str(row.get("suburb") or "").strip()
            tier = str(row.get("tier") or "").strip()

            if not photographer_id or not label:
                LOG.info(
                    "SKIP missing identity photographer_id=%s label_empty=%s",
                    photographer_id,
                    not label,
                )
                continue

            queries = _build_search_queries(label, suburb, st)
            if not queries:
                LOG.info(
                    "NO_MATCH name=%r tier=%s — no runnable search queries.",
                    label,
                    tier,
                )
                continue

            try:

                place, score, q_used, outcome = _pick_candidate_from_queries(
                    http,
                    places_key,
                    sb,
                    match_name=label,
                    queries=queries,
                    photographer_id=photographer_id,
                    spend=spend,
                    budget_ok_before_search=can_afford_text_search,
                    budget_ok_before_details=can_afford_details,
                )

                if outcome == "budget":
                    stopped_budget += 1
                    LOG.info(
                        "STOPPED_CAP name=%r tier=%s spend_usd=%.4f elapsed_m=%.2f "
                        "(next action would breach cap or runtime)",
                        label,
                        tier,
                        spend["usd"],
                        (time.monotonic() - run_started) / 60.0,
                    )
                    print("Stopped: USD or time guardrail triggered.")
                    break

                if outcome == "no_match" or place is None:
                    LOG.info(
                        "NO_MATCH action=skip_updates name=%r tier=%s query_last=%s "
                        "fuzzy=%s confidence=NONE spend_usd=%.4f",
                        label,
                        tier,
                        q_used or queries[0],
                        score,
                        spend["usd"],
                    )
                    _sleep()
                    continue

                raw_id = str(place.get("name") or place.get("id") or "")
                pid_for_details = _place_id_from_resource(raw_id)
                det = _place_details(http, pid_for_details, places_key)
                spend["usd"] += COST_DETAILS_USD
                _sleep()

                patch = _details_to_patch(det, fuzzy_score=score, row=row)

                sb.table("photographers").update(patch).eq(
                    "photographer_id", photographer_id
                ).execute()
                matched += 1

                LOG.info(
                    "UPDATED name=%r tier=%s query=%s fuzzy=%s confidence=%s "
                    "google_place_id=%s spend_usd=%.4f",
                    label,
                    tier,
                    q_used,
                    score,
                    patch.get("places_match_confidence"),
                    patch.get("google_place_id"),
                    spend["usd"],
                )

            except Exception as exc:  # noqa: BLE001
                errors += 1
                LOG.exception("ERROR name=%r tier=%s: %s", label, tier, exc)

            if (idx + 1) % 25 == 0:
                em = (time.monotonic() - run_started) / 60.0
                print(
                    f"Progress {idx + 1}/{len(rows)} | matched={matched} | "
                    f"errors={errors} | spend~${spend['usd']:.2f} | {em:.1f} min"
                )
            _sleep()

    elapsed_m = (time.monotonic() - run_started) / 60.0
    LOG.info(
        "Finished. pending_start=%s processed_loop=%s matched=%s errors=%s "
        "stopped_budget_events=%s est_usd=%.4f runtime_min=%.2f ended=%s",
        baseline_pending,
        len(rows),
        matched,
        errors,
        stopped_budget,
        spend["usd"],
        elapsed_m,
        datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    print("\n--- Photographer Places enrichment summary ---")
    print(f"Baseline pending rows:           {baseline_pending}")
    print(f"Rows iterated this run:         {len(rows)}")
    print(f"Rows enriched (Supabase PATCH): {matched}")
    print(f"errors:                         {errors}")
    print(f"Estimated API spend:            USD {spend['usd']:.4f}")
    print(f"Runtime minutes:               {elapsed_m:.2f}")
    print(f"See log file:                  {LOG_PATH}")

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(run())
