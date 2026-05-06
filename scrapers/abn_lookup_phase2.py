"""Phase 2 ABN Lookup for all venues — additive columns only.

Queries the Australian Business Register (ABR) JSON services with a smart name
strategy (licensee legal name, then Google name, then venue name), writes results
only to ``abn_phase2_*`` columns on ``public.venues``. Legacy ``abn`` and related
fields are never modified.

Run: ``python -m scrapers.abn_lookup_phase2``

Environment (``env.local``, ``override=True``):

- ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (required)
- ``ABN_GUID`` (required) — registered ABR web services GUID
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import httpx
from dotenv import load_dotenv
from rapidfuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

LOG = logging.getLogger("abn_lookup_phase2")

MATCHING_URL = "https://abr.business.gov.au/json/MatchingNames.aspx"
ABN_DETAILS_URL = "https://abr.business.gov.au/json/AbnDetails.aspx"

MIN_INTERVAL_S = 0.3
RETRY_BACKOFF_S = 2.0
API_CALL_CAP = 1500
PROGRESS_EVERY = 25
BATCH_SIZE = 50
RESULT_PAGE = 1000

Phase2VsExisting = Literal[
    "both_null",
    "net_new",
    "no_existing",
    "confirms_existing",
    "disagrees_with_existing",
]
MatchConfidence = Literal[
    "exact",
    "strong",
    "fuzzy",
    "multiple_candidates",
    "no_match",
]

ALLOWED_UPDATE_KEYS = frozenset(
    {
        "abn_phase2_value",
        "abn_phase2_entity_legal_name",
        "abn_phase2_entity_type",
        "abn_phase2_status",
        "abn_phase2_status_from_date",
        "abn_phase2_gst_registered",
        "abn_phase2_gst_from_date",
        "abn_phase2_state",
        "abn_phase2_postcode",
        "abn_phase2_match_confidence",
        "abn_phase2_match_score",
        "abn_phase2_query_used",
        "abn_phase2_query_strategy",
        "abn_phase2_candidates",
        "abn_phase2_attempted_at",
        "abn_phase2_lookup_count",
        "abn_phase2_vs_existing",
    }
)

_last_abr_call = 0.0

def parse_abr_json_response(resp: httpx.Response) -> dict[str, Any]:
    """The ABR JSON endpoints return JSONP ``callback({...})`` payloads, not raw JSON."""

    text = resp.content.decode("utf-8-sig", errors="replace").strip()
    if not text:
        raise ValueError("abr_body_empty")
    stripped = strip_jsonp_callback(text)
    return json.loads(stripped)


def strip_jsonp_callback(raw: str) -> str:
    """Locate the embedded JSON document (object or array) inside a JSONP wrapper."""
    raw_st = raw.strip().rstrip(";").strip()
    if raw_st.startswith("{") or raw_st.startswith("["):
        _obj, consumed = json.JSONDecoder().raw_decode(raw_st)
        return raw_st[:consumed]

    brace = raw_st.find("{")
    bracket = raw_st.find("[")
    candidates = [(brace, "{"), (bracket, "[")]
    starters = [(i, c) for i, c in candidates if i >= 0]
    if not starters:
        raise ValueError(f"abr_jsonp_no_payload:{raw_st[:220]!r}")
    start_idx, _ch = min(starters, key=lambda ic: ic[0])
    trunc = raw_st[start_idx:]
    _obj, consumed = json.JSONDecoder().raw_decode(trunc)
    out = trunc[:consumed].strip()
    if not out:
        raise ValueError("abr_jsonp_inner_empty")
    return out


def load_env() -> None:
    load_dotenv(_ROOT / "env.local", override=True)


def _abr_throttle() -> None:
    global _last_abr_call
    now = time.monotonic()
    elapsed = now - _last_abr_call
    if elapsed < MIN_INTERVAL_S:
        time.sleep(MIN_INTERVAL_S - elapsed)
    _last_abr_call = time.monotonic()


def normalise_whitespace(val: Any) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s or None


def normalise_abn_digits(val: Any) -> str | None:
    """Return 11-digit ABN string or None."""
    s = normalise_whitespace(val)
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if len(digits) == 11:
        return digits
    return None


def normalise_postcode(val: Any) -> str | None:
    s = normalise_whitespace(val)
    if not s:
        return None
    m = re.search(r"\b(\d{4})\b", s)
    if m:
        return m.group(1)
    if s.isdigit() and len(s) == 4:
        return s
    return None


def normalise_state(val: Any) -> str | None:
    s = normalise_whitespace(val)
    return s.upper() if s else None


def _parse_abr_date(val: Any) -> str | None:
    """ABR dates are often DD/MM/YYYY → ISO ``YYYY-MM-DD`` for Postgres."""
    s = normalise_whitespace(val)
    if not s:
        return None
    if re.fullmatch(r"\d{8}", s):
        try:
            d = datetime.strptime(s, "%d%m%Y").date()
            return d.isoformat()
        except ValueError:
            return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s.split("T")[0], fmt).date()
            return d.isoformat()
        except ValueError:
            continue
    return None


def _dig(obj: Any, *keys: str, default: Any = None) -> Any:
    cur = obj
    for k in keys:
        if isinstance(cur, dict):
            # try exact then common casings
            if k in cur:
                cur = cur[k]
            else:
                found = None
                for dk, dv in cur.items():
                    if str(dk).lower() == k.lower():
                        found = dv
                        break
                cur = found if found is not None else default
        else:
            return default
    return cur


def choose_query_strategy(
    venue: dict[str, Any],
) -> tuple[str | None, str | None]:
    """Return ``(query_string, strategy_label)`` or ``(None, None)`` when unusable."""
    legal = venue.get("licensee_legal_names")
    if isinstance(legal, list) and len(legal) > 0:
        first = normalise_whitespace(legal[0])
        if first:
            return first, "licensee_legal_name"
    g = normalise_whitespace(venue.get("google_name"))
    if g:
        return g, "google_name"
    n = normalise_whitespace(venue.get("name"))
    if n:
        return n, "venue_name"
    return None, None


def _matching_name_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("Names") or payload.get("names")
    items: Any = []
    if isinstance(raw, dict):
        items = raw.get("MatchingName") or raw.get("matchingName") or []
    elif isinstance(raw, list):
        items = raw
    if isinstance(items, dict):
        items = [items]
    if not isinstance(items, list):
        return []
    return [x for x in items if isinstance(x, dict)]


def call_abr_search(
    *,
    query: str,
    postcode: str | None,
    state: str | None,
    guid: str,
    client: httpx.Client,
    api_calls: list[int],
) -> tuple[list[dict[str, Any]], str | None]:
    """MatchingNames request; increments ``api_calls`` on each HTTP attempt."""
    params = {
        "name": query,
        "legalName": "Y",
        "tradingName": "Y",
        "businessName": "Y",
        "activeABNsOnly": "Y",
        "searchWidth": "typical",
        "maxResults": "10",
        "guid": guid,
    }
    pc = normalise_postcode(postcode)
    if pc:
        params["postcode"] = pc
    st = normalise_state(state)
    if st:
        params["state"] = st

    err: str | None = None
    for attempt in range(2):
        _abr_throttle()
        api_calls.append(1)
        try:
            r = client.get(MATCHING_URL, params=params, timeout=45.0)
            if r.status_code >= 400:
                err = f"http_{r.status_code}"
                if attempt == 0:
                    time.sleep(RETRY_BACKOFF_S)
                    continue
                LOG.warning("MatchingNames failed after retry: %s %s", r.status_code, r.text[:200])
                return [], err
            data = parse_abr_json_response(r)
        except Exception as exc:
            err = repr(exc)
            if attempt == 0:
                time.sleep(RETRY_BACKOFF_S)
                continue
            LOG.warning("MatchingNames exception after retry: %s", err)
            return [], err

        out: list[dict[str, Any]] = []
        for row in _matching_name_records(data):
            abn = normalise_abn_digits(
                row.get("Abn") or row.get("abn") or row.get("Identifier")
            )
            name = normalise_whitespace(
                row.get("Name")
                or row.get("name")
                or row.get("OrganisationName")
                or row.get("organisationName")
            )
            abr_score = row.get("Score") or row.get("score")
            try:
                abr_score_i = int(abr_score) if abr_score is not None else None
            except (TypeError, ValueError):
                abr_score_i = None
            status = normalise_whitespace(row.get("AbnStatus") or row.get("abnStatus"))
            c_pc = normalise_postcode(row.get("Postcode") or row.get("postcode"))
            c_st = normalise_state(row.get("State") or row.get("state"))
            out.append(
                {
                    "abn": abn,
                    "name": name,
                    "abr_score": abr_score_i,
                    "abn_status": status,
                    "postcode": c_pc,
                    "state": c_st,
                    "raw": {k: row[k] for k in list(row.keys())[:12]},
                }
            )
        return out, None
    return [], err or "unknown"


def score_and_pick_winner(
    candidates: list[dict[str, Any]],
    query: str,
    venue_postcode: str | None,
    venue_state: str | None,
) -> tuple[MatchConfidence, dict[str, Any] | None, list[dict[str, Any]]]:
    """Token-set ratio scoring, ``multiple_candidates`` guard, single winner."""
    vpc = normalise_postcode(venue_postcode)
    vst = normalise_state(venue_state)
    scored: list[dict[str, Any]] = []
    for c in candidates:
        nm = c.get("name") or ""
        fs = int(fuzz.token_set_ratio(query or "", nm or ""))
        row = {**c, "match_score": fs}
        scored.append(row)

    if not scored:
        return "no_match", None, scored

    high = [r for r in scored if r["match_score"] >= 85]
    if len(high) >= 2:
        return "multiple_candidates", None, scored

    best = max(r["match_score"] for r in scored)
    if best < 70:
        return "no_match", None, scored

    top = [r for r in scored if r["match_score"] == best]

    def postcode_distance(r: dict[str, Any]) -> int:
        cpc = normalise_postcode(r.get("postcode"))
        if vpc and cpc:
            try:
                return abs(int(vpc) - int(cpc))
            except ValueError:
                return 99999
        return 99999

    top.sort(key=postcode_distance)
    winner = top[0]
    fs = winner["match_score"]
    pc_match = bool(vpc and normalise_postcode(winner.get("postcode")) == vpc)
    st_match = bool(vst and normalise_state(winner.get("state")) == vst)

    conf: MatchConfidence
    if fs >= 95 and pc_match:
        conf = "exact"
    elif fs >= 85 and (pc_match or st_match):
        conf = "strong"
    elif fs >= 70:
        conf = "fuzzy"
    else:
        conf = "no_match"

    return conf, winner, scored


def get_abn_details(
    *,
    abn: str,
    guid: str,
    client: httpx.Client,
    api_calls: list[int],
) -> tuple[dict[str, Any] | None, str | None]:
    params = {"abn": abn, "includeHistoricalDetails": "N", "guid": guid}
    err: str | None = None
    for attempt in range(2):
        _abr_throttle()
        api_calls.append(1)
        try:
            r = client.get(ABN_DETAILS_URL, params=params, timeout=45.0)
            if r.status_code >= 400:
                err = f"http_{r.status_code}"
                if attempt == 0:
                    time.sleep(RETRY_BACKOFF_S)
                    continue
                LOG.warning("AbnDetails failed after retry: %s", r.status_code)
                return None, err
            return parse_abr_json_response(r), None
        except Exception as exc:
            err = repr(exc)
            if attempt == 0:
                time.sleep(RETRY_BACKOFF_S)
                continue
            LOG.warning("AbnDetails exception after retry: %s", err)
            return None, err
    return None, err or "unknown"


def map_entity_from_abn_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract phase-2 entity fields from AbnDetails JSON (flat REST shape or wrapped)."""
    wrapped = payload.get("BusinessEntity202001") or payload.get("businessEntity202001")
    root = wrapped if isinstance(wrapped, dict) else payload

    organisation = normalise_whitespace(root.get("EntityName"))
    entity_type = normalise_whitespace(
        root.get("EntityTypeName") or root.get("EntityTypeCode")
    )

    raw_status = root.get("AbnStatus")
    if isinstance(raw_status, dict):
        abn_status = normalise_whitespace(
            raw_status.get("AbnStatus") or raw_status.get("EntityStatusCode")
        )
        status_from = _parse_abr_date(
            raw_status.get("EffectiveFrom") or raw_status.get("effectiveFrom")
        )
    else:
        abn_status = normalise_whitespace(raw_status)
        status_from = _parse_abr_date(root.get("AbnStatusEffectiveFrom"))

    gst_raw = root.get("Gst")
    gst_registered: bool
    gst_from: str | None
    if gst_raw is None or (isinstance(gst_raw, str) and not str(gst_raw).strip()):
        gst_registered = False
        gst_from = None
    else:
        gst_registered = True
        gst_from = _parse_abr_date(str(gst_raw))

    if not gst_registered:
        gst_block = _dig(root, "GoodsAndServicesTax") or _dig(root, "goodsAndServicesTax")
        if isinstance(gst_block, list) and gst_block:
            gst_block = next((g for g in gst_block if isinstance(g, dict)), gst_block[0])
        if isinstance(gst_block, dict) and gst_block:
            gst_registered = True
            gst_from = _parse_abr_date(
                _dig(gst_block, "EffectiveFrom")
                or _dig(gst_block, "effectiveFrom")
            )

    state = normalise_state(root.get("AddressState"))
    postcode = normalise_postcode(root.get("AddressPostcode"))

    if not organisation:
        main = _dig(root, "MainName") or _dig(root, "mainName")
        organisation = normalise_whitespace(
            _dig(main, "OrganisationName") or _dig(main, "organisationName")
        )
        if not organisation and isinstance(main, dict):
            organisation = normalise_whitespace(
                _dig(main, "CommercialName") or _dig(main, "commercialName")
            )

    if not organisation:
        legal = _dig(root, "LegalName") or _dig(root, "legalName")
        if isinstance(legal, dict):
            organisation = organisation or normalise_whitespace(
                _dig(legal, "OrganisationName")
                or _dig(legal, "organisationName")
                or _dig(legal, "FullName")
                or _dig(legal, "fullName")
            )
            if not organisation:
                parts = [
                    normalise_whitespace(legal.get("GivenName")),
                    normalise_whitespace(legal.get("OtherGivenName")),
                    normalise_whitespace(legal.get("FamilyName")),
                ]
                organisation = normalise_whitespace(" ".join(p for p in parts if p))

    if not organisation:
        bn_q = root.get("BusinessName") or root.get("businessName")
        if isinstance(bn_q, list) and bn_q:
            first = bn_q[0]
            organisation = organisation or normalise_whitespace(
                first if isinstance(first, str) else None
            )
        elif isinstance(bn_q, str):
            organisation = organisation or normalise_whitespace(bn_q)

    if not entity_type:
        entity_type = normalise_whitespace(
            _dig(root, "EntityType", "EntityDescription")
            or _dig(root, "EntityType", "EntityTypeCode")
        )

    if not abn_status or not status_from:
        status_block = _dig(root, "EntityStatus") or _dig(root, "AbnStatus")
        if isinstance(status_block, dict):
            cand_status = normalise_whitespace(
                status_block.get("EntityStatusCode")
                or status_block.get("AbnStatus")
                or status_block.get("abnStatus")
            )
            if cand_status:
                abn_status = abn_status or cand_status
            cand_from = _parse_abr_date(
                status_block.get("EffectiveFrom") or status_block.get("effectiveFrom")
            )
            if cand_from:
                status_from = status_from or cand_from

    if not state or not postcode:
        main_loc = _dig(root, "MainBusinessPhysicalAddress") or _dig(root, "BusinessAddress")
        if isinstance(main_loc, list) and main_loc:
            main_loc = main_loc[0]
        if isinstance(main_loc, dict):
            state = state or normalise_state(
                _dig(main_loc, "StateCode") or _dig(main_loc, "stateCode")
            )
            postcode = postcode or normalise_postcode(
                _dig(main_loc, "Postcode") or _dig(main_loc, "postcode")
            )

    return {
        "abn_phase2_entity_legal_name": organisation,
        "abn_phase2_entity_type": entity_type,
        "abn_phase2_status": abn_status,
        "abn_phase2_status_from_date": status_from,
        "abn_phase2_gst_registered": gst_registered,
        "abn_phase2_gst_from_date": gst_from,
        "abn_phase2_state": state,
        "abn_phase2_postcode": postcode,
    }


def cross_reference_existing(
    existing_abn_raw: Any, phase2_abn: str | None
) -> Phase2VsExisting:
    existing = normalise_abn_digits(existing_abn_raw)
    p2 = normalise_abn_digits(phase2_abn) if phase2_abn else None
    if existing is None and p2 is None:
        return "both_null"
    if existing is None and p2 is not None:
        return "net_new"
    if existing is not None and p2 is None:
        return "no_existing"
    assert existing is not None and p2 is not None
    if existing == p2:
        return "confirms_existing"
    return "disagrees_with_existing"


def flush_updates(sb: Any, batch: list[tuple[str, dict[str, Any]]]) -> None:
    for vid, payload in batch:
        extra = set(payload.keys()) - ALLOWED_UPDATE_KEYS
        if extra:
            raise RuntimeError(f"Refusing update with disallowed keys: {sorted(extra)}")
        sb.table("venues").update(payload).eq("id", vid).execute()


@dataclass
class BaselineTallies:
    name_null: int
    address_null: int
    place_id_null: int
    abn_null: int
    abn_entity_legal_name_null: int


def check_preflight(sb: Any) -> tuple[int, BaselineTallies]:
    """Verify required columns exist; return ``(abn_populated_count, q9_baseline)``."""
    rows = (
        sb.table("venues")
        .select(
            "id,abn_phase2_value,abn_phase2_match_confidence,"
            "abn_phase2_query_strategy,abn_phase2_vs_existing"
        )
        .limit(1)
        .execute()
    )
    if not rows.data:
        raise SystemExit("Preflight failed: no rows returned from public.venues.")

    q9 = (
        sb.table("venues")
        .select("id", count="exact")
        .execute()
    )
    total = int(getattr(q9, "count", 0) or 0)

    def count_filter(field: str, is_null: bool) -> int:
        q = sb.table("venues").select("id", count="exact")
        if is_null:
            q = q.is_(field, "null")
        else:
            q = q.not_.is_(field, "null")
        r = q.limit(1).execute()
        return int(getattr(r, "count", 0) or 0)

    abn_pop = total - count_filter("abn", True)
    baseline = BaselineTallies(
        name_null=count_filter("name", True),
        address_null=count_filter("address", True),
        place_id_null=count_filter("place_id", True),
        abn_null=count_filter("abn", True),
        abn_entity_legal_name_null=count_filter("abn_entity_legal_name", True),
    )
    LOG.info(
        "Preflight OK: required abn_phase2 columns present; venues=%s; "
        "rows with abn IS NOT NULL (baseline)=%s",
        total,
        abn_pop,
    )
    LOG.info(
        "Q9 baseline tallies (null counts): name=%s address=%s place_id=%s "
        "abn=%s abn_entity_legal_name=%s",
        baseline.name_null,
        baseline.address_null,
        baseline.place_id_null,
        baseline.abn_null,
        baseline.abn_entity_legal_name_null,
    )
    if abn_pop == 0:
        LOG.warning(
            "Baseline anomaly: expected some rows with legacy ``abn`` from the earlier "
            "40-venue pilot, but ``abn`` is populated on 0 rows. Proceeding anyway."
        )
    return abn_pop, baseline


def fetch_all_venues(sb: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        resp = (
            sb.table("venues")
            .select(
                "id,name,google_name,postcode,suburb,state,abn,licensee_legal_names,"
                "abn_phase2_attempted_at,abn_phase2_lookup_count"
            )
            .order("id")
            .range(offset, offset + RESULT_PAGE - 1)
            .execute()
        )
        batch = resp.data or []
        out.extend(batch)
        if len(batch) < RESULT_PAGE:
            break
        offset += RESULT_PAGE
    return out


def slim_candidates(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    slim: list[dict[str, Any]] = []
    for r in scored:
        slim.append(
            {
                "abn": r.get("abn"),
                "name": r.get("name"),
                "match_score": r.get("match_score"),
                "abr_score": r.get("abr_score"),
                "postcode": r.get("postcode"),
                "state": r.get("state"),
                "abn_status": r.get("abn_status"),
            }
        )
    return slim


def main() -> None:
    import os

    from supabase import create_client  # noqa: PLC0415

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    load_env()
    sb_url = (os.environ.get("SUPABASE_URL") or "").strip()
    sb_key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    guid = (os.environ.get("ABN_GUID") or "").strip()
    if not sb_url or not sb_key:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")
        raise SystemExit(1)
    if not guid:
        LOG.error("ABN_GUID is required.")
        raise SystemExit(1)

    sb = create_client(sb_url, sb_key)
    _, baseline_pre = check_preflight(sb)

    venues = fetch_all_venues(sb)
    LOG.info("Loaded %s venue rows.", len(venues))

    pending = [v for v in venues if v.get("abn_phase2_attempted_at") is None]
    LOG.info("%s venues need phase-2 lookups (skipped %s already attempted).", len(pending), len(venues) - len(pending))

    api_calls: list[int] = []

    processed = 0
    strat_counts: dict[str, int] = {}
    flush_batch: list[tuple[str, dict[str, Any]]] = []
    attempted_counter = 0
    licensee_attempts = 0
    licensee_hits = 0

    t0 = time.perf_counter()

    with httpx.Client(headers={"Accept": "application/json"}) as client:
        for v in pending:
            if sum(api_calls) >= API_CALL_CAP:
                LOG.error(
                    "Hard stop: ABR API call cap (%s) reached after %s venues in this batch.",
                    API_CALL_CAP,
                    processed,
                )
                break

            vid = str(v["id"])
            lookup_n = int(v.get("abn_phase2_lookup_count") or 0) + 1
            attempted = datetime.now(timezone.utc).isoformat()

            q, strat = choose_query_strategy(v)

            existing_abn = v.get("abn")

            if q is None or strat is None:
                payload = {
                    "abn_phase2_value": None,
                    "abn_phase2_entity_legal_name": None,
                    "abn_phase2_entity_type": None,
                    "abn_phase2_status": None,
                    "abn_phase2_status_from_date": None,
                    "abn_phase2_gst_registered": None,
                    "abn_phase2_gst_from_date": None,
                    "abn_phase2_state": None,
                    "abn_phase2_postcode": None,
                    "abn_phase2_match_confidence": "no_match",
                    "abn_phase2_match_score": None,
                    "abn_phase2_query_used": None,
                    "abn_phase2_query_strategy": None,
                    "abn_phase2_candidates": [],
                    "abn_phase2_attempted_at": attempted,
                    "abn_phase2_lookup_count": lookup_n,
                    "abn_phase2_vs_existing": cross_reference_existing(existing_abn, None),
                }
                flush_batch.append((vid, payload))
                strat_counts["(no usable query)"] = strat_counts.get("(no usable query)", 0) + 1
                processed += 1
                attempted_counter += 1
            else:
                raw_list, mn_err = call_abr_search(
                    query=q,
                    postcode=normalise_whitespace(v.get("postcode")),
                    state=normalise_whitespace(v.get("state")),
                    guid=guid,
                    client=client,
                    api_calls=api_calls,
                )
                strat_counts[strat] = strat_counts.get(strat, 0) + 1
                if strat == "licensee_legal_name":
                    licensee_attempts += 1
                attempted_counter += 1

                if mn_err:
                    payload = {
                        "abn_phase2_value": None,
                        "abn_phase2_entity_legal_name": None,
                        "abn_phase2_entity_type": None,
                        "abn_phase2_status": None,
                        "abn_phase2_status_from_date": None,
                        "abn_phase2_gst_registered": None,
                        "abn_phase2_gst_from_date": None,
                        "abn_phase2_state": None,
                        "abn_phase2_postcode": None,
                        "abn_phase2_match_confidence": "no_match",
                        "abn_phase2_match_score": None,
                        "abn_phase2_query_used": q,
                        "abn_phase2_query_strategy": strat,
                        "abn_phase2_candidates": [{"matching_names_error": mn_err}],
                        "abn_phase2_attempted_at": attempted,
                        "abn_phase2_lookup_count": lookup_n,
                        "abn_phase2_vs_existing": cross_reference_existing(existing_abn, None),
                    }
                    flush_batch.append((vid, payload))
                    processed += 1
                else:
                    conf, winner, scored = score_and_pick_winner(
                        raw_list, q, v.get("postcode"), v.get("state")
                    )
                    slim_c = slim_candidates(scored)

                    if conf == "multiple_candidates" or winner is None or not winner.get("abn"):
                        payload = {
                            "abn_phase2_value": None,
                            "abn_phase2_entity_legal_name": None,
                            "abn_phase2_entity_type": None,
                            "abn_phase2_status": None,
                            "abn_phase2_status_from_date": None,
                            "abn_phase2_gst_registered": None,
                            "abn_phase2_gst_from_date": None,
                            "abn_phase2_state": None,
                            "abn_phase2_postcode": None,
                            "abn_phase2_match_confidence": conf,
                            "abn_phase2_match_score": max((s.get("match_score") or 0 for s in scored), default=None),
                            "abn_phase2_query_used": q,
                            "abn_phase2_query_strategy": strat,
                            "abn_phase2_candidates": slim_c,
                            "abn_phase2_attempted_at": attempted,
                            "abn_phase2_lookup_count": lookup_n,
                            "abn_phase2_vs_existing": cross_reference_existing(existing_abn, None),
                        }
                        flush_batch.append((vid, payload))
                        processed += 1
                    else:
                        if sum(api_calls) >= API_CALL_CAP:
                            LOG.error(
                                "ABR API call cap reached before AbnDetails; persisting no_match "
                                "for this venue and stopping the run."
                            )
                            payload = {
                                "abn_phase2_value": None,
                                "abn_phase2_entity_legal_name": None,
                                "abn_phase2_entity_type": None,
                                "abn_phase2_status": None,
                                "abn_phase2_status_from_date": None,
                                "abn_phase2_gst_registered": None,
                                "abn_phase2_gst_from_date": None,
                                "abn_phase2_state": None,
                                "abn_phase2_postcode": None,
                                "abn_phase2_match_confidence": "no_match",
                                "abn_phase2_match_score": winner.get("match_score"),
                                "abn_phase2_query_used": q,
                                "abn_phase2_query_strategy": strat,
                                "abn_phase2_candidates": slim_c
                                + [{"api_cap_before_abn_details": True}],
                                "abn_phase2_attempted_at": attempted,
                                "abn_phase2_lookup_count": lookup_n,
                                "abn_phase2_vs_existing": cross_reference_existing(
                                    existing_abn, None
                                ),
                            }
                            flush_batch.append((vid, payload))
                            processed += 1
                            break
                        abn_w = str(winner["abn"])
                        detail, det_err = get_abn_details(
                            abn=abn_w, guid=guid, client=client, api_calls=api_calls
                        )
                        if det_err or not detail:
                            payload = {
                                "abn_phase2_value": None,
                                "abn_phase2_entity_legal_name": None,
                                "abn_phase2_entity_type": None,
                                "abn_phase2_status": None,
                                "abn_phase2_status_from_date": None,
                                "abn_phase2_gst_registered": None,
                                "abn_phase2_gst_from_date": None,
                                "abn_phase2_state": None,
                                "abn_phase2_postcode": None,
                                "abn_phase2_match_confidence": "no_match",
                                "abn_phase2_match_score": winner.get("match_score"),
                                "abn_phase2_query_used": q,
                                "abn_phase2_query_strategy": strat,
                                "abn_phase2_candidates": slim_c + [{"abn_details_error": det_err}],
                                "abn_phase2_attempted_at": attempted,
                                "abn_phase2_lookup_count": lookup_n,
                                "abn_phase2_vs_existing": cross_reference_existing(existing_abn, None),
                            }
                            flush_batch.append((vid, payload))
                            processed += 1
                        else:
                            entity = map_entity_from_abn_payload(detail)
                            p2_abn = normalise_abn_digits(detail.get("Abn") or detail.get("abn") or abn_w)
                            payload = {
                                "abn_phase2_value": p2_abn,
                                "abn_phase2_entity_legal_name": entity["abn_phase2_entity_legal_name"],
                                "abn_phase2_entity_type": entity["abn_phase2_entity_type"],
                                "abn_phase2_status": entity["abn_phase2_status"],
                                "abn_phase2_status_from_date": entity["abn_phase2_status_from_date"],
                                "abn_phase2_gst_registered": entity["abn_phase2_gst_registered"],
                                "abn_phase2_gst_from_date": entity["abn_phase2_gst_from_date"],
                                "abn_phase2_state": entity["abn_phase2_state"],
                                "abn_phase2_postcode": entity["abn_phase2_postcode"],
                                "abn_phase2_match_confidence": conf,
                                "abn_phase2_match_score": winner.get("match_score"),
                                "abn_phase2_query_used": q,
                                "abn_phase2_query_strategy": strat,
                                "abn_phase2_candidates": slim_c,
                                "abn_phase2_attempted_at": attempted,
                                "abn_phase2_lookup_count": lookup_n,
                                "abn_phase2_vs_existing": cross_reference_existing(
                                    existing_abn, p2_abn
                                ),
                            }
                            if strat == "licensee_legal_name" and p2_abn:
                                licensee_hits += 1
                            flush_batch.append((vid, payload))
                            processed += 1

            if attempted_counter % PROGRESS_EVERY == 0:
                LOG.info(
                    "Checkpoint: %s venues attempted this run; %s ABR HTTP calls so far.",
                    attempted_counter,
                    sum(api_calls),
                )

            if len(flush_batch) >= BATCH_SIZE:
                flush_updates(sb, flush_batch)
                LOG.info("Flushed %s venue updates to Supabase.", len(flush_batch))
                flush_batch = []

    if flush_batch:
        flush_updates(sb, flush_batch)
        LOG.info("Flushed final %s venue updates to Supabase.", len(flush_batch))

    elapsed = time.perf_counter() - t0
    total_http = sum(api_calls)
    LOG.info(
        "Run complete: venues processed this run=%s, ABR HTTP calls=%s, runtime=%.1fs, "
        "strategy histogram=%s",
        processed,
        total_http,
        elapsed,
        strat_counts,
    )

    if licensee_attempts > 0 and (licensee_hits / licensee_attempts) < 0.5:
        LOG.error(
            "Review threshold: licensee_legal_name hit rate %.1f%% is below the 50%% "
            "review gate (%s hits / %s attempts). Inspect Q3 and matching rules before "
            "trusting licensee-led matches.",
            100.0 * licensee_hits / licensee_attempts,
            licensee_hits,
            licensee_attempts,
        )
    if licensee_attempts:
        LOG.info(
            "Licensee strategy headline: %s / %s hits (%.1f%%).",
            licensee_hits,
            licensee_attempts,
            100.0 * licensee_hits / licensee_attempts,
        )

    # Post-run Q9 tallies (must match baseline if legacy columns were left untouched)
    def count_filter(field: str, is_null: bool) -> int:
        q = sb.table("venues").select("id", count="exact")
        if is_null:
            q = q.is_(field, "null")
        else:
            q = q.not_.is_(field, "null")
        r = q.limit(1).execute()
        return int(getattr(r, "count", 0) or 0)

    q9_post = BaselineTallies(
        name_null=count_filter("name", True),
        address_null=count_filter("address", True),
        place_id_null=count_filter("place_id", True),
        abn_null=count_filter("abn", True),
        abn_entity_legal_name_null=count_filter("abn_entity_legal_name", True),
    )
    if q9_post != baseline_pre:
        LOG.error(
            "Q9 validation FAILED: legacy null tallies shifted. baseline=%s post=%s",
            baseline_pre,
            q9_post,
        )
        raise SystemExit(1)
    LOG.info(
        "Q9 OK: legacy column null counts unchanged (additive update only). snapshot=%s",
        q9_post,
    )


if __name__ == "__main__":
    main()
