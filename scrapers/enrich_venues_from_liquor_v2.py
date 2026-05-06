"""Additive venue enrichment from Australian liquor licence registers (v2).

Reads ``shared.ref_liquor_licenses`` and chain metadata, writes only augmentation
columns on ``public.venues``. Never updates legacy fields (name, address,
coordinates, ABR columns, ``liquor_license_id``, etc.).

Phase A (VIC geocode backfill and materialised view refresh) runs with
``python -m scrapers.enrich_venues_from_liquor_v2 --phase-a`` when ``DATABASE_URL``
is set.

``DATABASE_URL`` is also used to read ``shared.mv_chain_operators`` if the
Supabase REST API cannot (some deployments omit PostgREST grants on that
materialised view).

Run: ``python -m scrapers.enrich_venues_from_liquor_v2``

Environment (``env.local``, ``override=True``):

- ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (required)
- ``DATABASE_URL`` (strongly recommended: Phase A via ``--phase-a``; chain-operator map if REST returns 403 on the MV)
"""

from __future__ import annotations

import logging
import math
import re
import time
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

from dotenv import load_dotenv
from rapidfuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]

LOG = logging.getLogger("enrich_venues_from_liquor_v2")

CHECKPOINT_EVERY = 50
WRITE_BATCH_SIZE = 50
LICENCE_PAGE_SIZE = 1000
GEOFENCE_M = 2000

ARTICLE_PREFIXES = ("the ", "le ", "la ")
ENTITY_SUFFIXES = (
    "function centre",
    "cellar door",
    "restaurant",
    "tavern",
    "hotel",
    "winery",
    "pub",
    "bar",
    "club",
    "cafe",
)

PUNCT_RE = re.compile(r"[^a-z0-9\s]+")

OrgConfidence = str  # high | medium | low
CandidateLevel = str  # exact | strong | fuzzy | weak
Outcome = str  # exact | strong | fuzzy | multiple_candidates | no_match


class LicenceRow(TypedDict, total=False):
    license_id: int
    state_code: str | None
    license_number: str | None
    license_type: str | None
    licensee_type: str | None
    trading_name: str | None
    licensee_legal_name: str | None
    suburb: str | None
    postcode: str | None
    lat: Any
    lng: Any
    abn_from_register: str | None
    acn_from_register: str | None


def load_env() -> None:
    load_dotenv(_ROOT / "env.local", override=True)


def _as_text(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def normalise_trading_name(raw: str | None) -> str:
    if not raw:
        return ""
    s = raw.lower().strip()
    s = PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    while True:
        hit = False
        for prefix in ARTICLE_PREFIXES:
            if s.startswith(prefix):
                s = s[len(prefix) :].strip()
                hit = True
        if not hit:
            break
    while True:
        hit = False
        for suf in ENTITY_SUFFIXES:
            if s.endswith(suf):
                s = s[: -len(suf)].strip()
                hit = True
                break
        if not hit:
            break
    return re.sub(r"\s+", " ", s).strip()


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def map_venue_category(state_code: str | None, license_type: str | None) -> str:
    st = (state_code or "").upper().strip()
    lt = (license_type or "").strip()
    if st == "NSW":
        nsw = {
            "Liquor - hotel licence": "HOTEL",
            "Liquor - on-premises licence": "ON_PREMISES",
            "Liquor - club licence": "CLUB",
            "Liquor - small bar licence": "BAR",
            "Liquor - producer wholesaler licence": "WINERY",
            "Liquor - packaged liquor licence": "BOTTLE_SHOP",
            "Liquor - limited licence": "POPUP",
            "Liquor - pop-up licence": "POPUP",
        }
        return nsw.get(lt, "OTHER")
    if st == "VIC":
        vic = {
            "General Licence": "HOTEL",
            "Restaurant and cafe Licence": "RESTAURANT",
            "On-Premises Licence": "ON_PREMISES",
            "Full Club Licence": "CLUB",
            "Restricted Club Licence": "CLUB",
            "Producer's Licence": "WINERY",
            "Packaged Liquor Licence": "BOTTLE_SHOP",
            "Limited Licence": "OTHER",
            "Pre-retail Licence": "OTHER",
            "Remote Seller's Licence": "OTHER",
        }
        return vic.get(lt, "OTHER")
    if st == "TAS":
        tas = {
            "General": "HOTEL",
            "On-Restaurant": "RESTAURANT",
            "On": "ON_PREMISES",
            "Club": "CLUB",
            "Special": "SPECIAL_OCCASION",
            "Off": "BOTTLE_SHOP",
        }
        return tas.get(lt, "OTHER")
    if st == "SA":
        sa = {
            "Hotel": "HOTEL",
            "Restaurant": "RESTAURANT",
            "Club": "CLUB",
            "Limited Club": "CLUB",
            "Producer": "WINERY",
            "Direct Sales": "WHOLESALE",
            "Wholesale Liquor Merchant": "WHOLESALE",
            "Retail Liquor Merchant": "BOTTLE_SHOP",
            "Special Circumstances": "OTHER",
            "Residential": "ACCOMMODATION",
        }
        return sa.get(lt, "OTHER")
    return "OTHER"


def classify_licensee_org(
    legal_name: str | None,
    chain_upper_keys: dict[str, str],
    licensee_type_from_register: str | None = None,
) -> tuple[str, OrgConfidence]:
    """Return (organisation_type, organisation_type_confidence) for one licensee."""
    if not legal_name or not str(legal_name).strip():
        return "UNKNOWN", "low"
    u = str(legal_name).strip().upper()
    canon = chain_upper_keys.get(u)
    if canon is not None:
        return "GROUP", "high"

    if any(
        x in u
        for x in (
            "COUNCIL",
            "CITY OF ",
            "CROWN",
            "GOVERNMENT",
            "DEPARTMENT OF",
        )
    ):
        return "GOVERNMENT", "high"
    if (
        u.startswith("MR ")
        or u.startswith("MRS ")
        or u.startswith("MS ")
        or u.startswith("MISS ")
        or u.startswith("DR ")
        or u.startswith("PROF ")
    ):
        return "INDIVIDUAL", "high"
    if any(
        x in u
        for x in (
            "RSL",
            "SUB-BRANCH",
            "BOWLING CLUB",
            "CLUB INC",
            "GOLF CLUB",
        )
    ):
        return "CLUB", "high"
    if (
        "TRUST" in u
        or "TRUSTEE" in u
        or "AS TRUSTEE FOR" in u
        or re.search(r"\bATF\b", u) is not None
    ):
        return "TRUST", "medium"
    company_suffixes = ("PTY LTD", "PTY. LTD.", "LIMITED", "LTD", "LIMITED.")
    if any(u.endswith(s) for s in company_suffixes):
        return "COMPANY", "high"
    reg = (licensee_type_from_register or "").strip().upper()
    if reg == "PERSON":
        return "INDIVIDUAL", "medium"
    if reg in ("ORGANISATION", "ORGANIZATION"):
        return "COMPANY", "low"
    return "UNKNOWN", "low"


_CONF_RANK = {"high": 3, "medium": 2, "low": 1}
_TYPE_PRIORITY = (
    "GOVERNMENT",
    "GROUP",
    "INDIVIDUAL",
    "CLUB",
    "TRUST",
    "COMPANY",
    "UNKNOWN",
)


def merge_org_types(types_with_conf: list[tuple[str, OrgConfidence]]) -> tuple[str, OrgConfidence]:
    if not types_with_conf:
        return "UNKNOWN", "low"
    by_t: dict[str, list[OrgConfidence]] = {}
    for t, c in types_with_conf:
        by_t.setdefault(t, []).append(c)
    # Mode by count; tie-break by _TYPE_PRIORITY
    best_count = -1
    candidates: list[str] = []
    for t, confs in by_t.items():
        n = len(confs)
        if n > best_count:
            best_count = n
            candidates = [t]
        elif n == best_count:
            candidates.append(t)
    if len(candidates) == 1:
        chosen = candidates[0]
    else:
        chosen = min(candidates, key=lambda x: _TYPE_PRIORITY.index(x) if x in _TYPE_PRIORITY else 99)
    # Confidence: strongest among contributors for chosen type
    chosen_conf = max(by_t[chosen], key=lambda x: _CONF_RANK.get(x, 0))
    return chosen, chosen_conf


def abn_agreement(venue_abn: str | None, candidates: list[str]) -> str:
    clean_candidates = [c for c in candidates if c]
    if not clean_candidates:
        return "no_licence_evidence"
    if venue_abn is None or not str(venue_abn).strip():
        return "licence_only"
    v = str(venue_abn).strip().replace(" ", "")
    norm_set = {c.strip().replace(" ", "") for c in clean_candidates}
    if v in norm_set:
        return "confirmed"
    return "disagreement"


def candidate_level(
    name_score: int, suburb_match: bool, distance_m: float | None
) -> CandidateLevel:
    if name_score >= 95 and suburb_match and (distance_m is None or distance_m < 500):
        return "exact"
    if name_score >= 85 and suburb_match and (distance_m is None or distance_m < 1000):
        return "strong"
    if name_score >= 70 and (
        suburb_match or (distance_m is not None and distance_m < GEOFENCE_M)
    ):
        return "fuzzy"
    return "weak"


def evidence_match_label(
    name_score: int, suburb_match: bool, distance_m: float | None
) -> str:
    lvl = candidate_level(name_score, suburb_match, distance_m)
    if lvl != "weak":
        return lvl
    if name_score >= 50 and suburb_match:
        return "suburb_signal"
    return "weak"


def fetch_all_licences(sb: Any) -> list[LicenceRow]:
    out: list[LicenceRow] = []
    offset = 0
    t = sb.schema("shared").table("ref_liquor_licenses")
    while True:
        resp = (
            t.select(
                "license_id,state_code,license_number,license_type,licensee_type,trading_name,"
                "licensee_legal_name,suburb,postcode,lat,lng,abn_from_register,acn_from_register"
            )
            .order("license_id")
            .range(offset, offset + LICENCE_PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        out.extend(batch)
        if len(batch) < LICENCE_PAGE_SIZE:
            break
        offset += LICENCE_PAGE_SIZE
    return out


def fetch_chain_map_rest(sb: Any) -> dict[str, str]:
    """Map UPPER(trimmed licensee) -> canonical name from ``mv_chain_operators`` (REST)."""
    upper_to_canon: dict[str, str] = {}
    offset = 0
    t = sb.schema("shared").table("mv_chain_operators")
    while True:
        resp = (
            t.select("licensee_legal_name")
            .range(offset, offset + LICENCE_PAGE_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        for row in batch:
            raw = row.get("licensee_legal_name")
            if not raw:
                continue
            key = str(raw).strip().upper()
            upper_to_canon.setdefault(key, str(raw).strip())
        if len(batch) < LICENCE_PAGE_SIZE:
            break
        offset += LICENCE_PAGE_SIZE
    return upper_to_canon


def fetch_chain_map_psycopg(db_url: str) -> dict[str, str]:
    import psycopg

    upper_to_canon: dict[str, str] = {}
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT licensee_legal_name FROM shared.mv_chain_operators "
                "WHERE licensee_legal_name IS NOT NULL"
            )
            for (raw,) in cur.fetchall():
                if not raw:
                    continue
                s = str(raw).strip()
                upper_to_canon.setdefault(s.upper(), s)
    return upper_to_canon


def fetch_chain_map(sb: Any, db_url: str | None) -> dict[str, str]:
    if (db_url or "").strip():
        return fetch_chain_map_psycopg(db_url.strip())
    return fetch_chain_map_rest(sb)


class ScoredCandidate:
    __slots__ = ("lic", "name_score", "suburb_match", "distance_m", "level")

    def __init__(
        self,
        lic: LicenceRow,
        name_score: int,
        suburb_match: bool,
        distance_m: float | None,
        level: CandidateLevel,
    ) -> None:
        self.lic = lic
        self.name_score = name_score
        self.suburb_match = suburb_match
        self.distance_m = distance_m
        self.level = level


def build_licence_indices(licences: list[LicenceRow]) -> tuple[
    dict[str, list[LicenceRow]],
    dict[str, list[LicenceRow]],
    list[tuple[LicenceRow, float, float]],
]:
    by_pc: dict[str, list[LicenceRow]] = {}
    by_sub: dict[str, list[LicenceRow]] = {}
    geo: list[tuple[LicenceRow, float, float]] = []
    for lic in licences:
        pc = _as_text(lic.get("postcode"))
        if pc:
            by_pc.setdefault(pc, []).append(lic)
        sub = _as_text(lic.get("suburb"))
        if sub:
            by_sub.setdefault(sub.strip().lower(), []).append(lic)
        lat, lng = _as_float(lic.get("lat")), _as_float(lic.get("lng"))
        if lat is not None and lng is not None:
            geo.append((lic, lat, lng))
    return by_pc, by_sub, geo


def narrow_candidates(
    v_pc: str | None,
    v_sub: str | None,
    v_lat: float | None,
    v_lng: float | None,
    by_pc: dict[str, list[LicenceRow]],
    by_sub: dict[str, list[LicenceRow]],
    geo: list[tuple[LicenceRow, float, float]],
) -> dict[int, LicenceRow]:
    acc: dict[int, LicenceRow] = {}
    if v_pc:
        for lic in by_pc.get(v_pc, []):
            lid = int(lic["license_id"])
            acc[lid] = lic
    if v_sub:
        key = v_sub.strip().lower()
        for lic in by_sub.get(key, []):
            lid = int(lic["license_id"])
            acc[lid] = lic
    if v_lat is not None and v_lng is not None:
        for lic, lat, lng in geo:
            if haversine_m(v_lat, v_lng, lat, lng) < GEOFENCE_M:
                lid = int(lic["license_id"])
                acc[lid] = lic
    return acc


def score_venue_candidates(
    venue_name: str | None,
    venue_suburb: str | None,
    v_lat: float | None,
    v_lng: float | None,
    candidates: dict[int, LicenceRow],
) -> list[ScoredCandidate]:
    vn = normalise_trading_name(_as_text(venue_name))
    vs = (_as_text(venue_suburb) or "").strip().lower()
    out: list[ScoredCandidate] = []
    for lic in candidates.values():
        lsub = (_as_text(lic.get("suburb")) or "").strip().lower()
        suburb_match = bool(vs and lsub and vs == lsub)
        lat_i, lng_i = _as_float(lic.get("lat")), _as_float(lic.get("lng"))
        distance_m: float | None = None
        if v_lat is not None and v_lng is not None and lat_i is not None and lng_i is not None:
            distance_m = haversine_m(v_lat, v_lng, lat_i, lng_i)
        tn = normalise_trading_name(_as_text(lic.get("trading_name")))
        name_score = int(fuzz.token_set_ratio(vn, tn)) if vn or tn else 0
        level = candidate_level(name_score, suburb_match, distance_m)
        out.append(ScoredCandidate(lic, name_score, suburb_match, distance_m, level))
    return out


def decide_match(
    scored: list[ScoredCandidate],
) -> tuple[Outcome, ScoredCandidate | None, list[CandidateLevel]]:
    if not scored:
        return "no_match", None, []
    levels = [s.level for s in scored]
    strong_plus = [s for s in scored if s.level in ("exact", "strong")]
    if len(strong_plus) >= 2:
        return "multiple_candidates", None, levels
    eligible = [s for s in scored if s.level in ("exact", "strong", "fuzzy")]
    if not eligible:
        return "no_match", None, levels

    def sort_key(s: ScoredCandidate) -> tuple[int, float]:
        dist = s.distance_m if s.distance_m is not None else float("inf")
        return (s.name_score, -dist)

    best = max(eligible, key=sort_key)
    return best.level, best, levels


def build_evidence_payload(
    scored: list[ScoredCandidate],
    outcome: Outcome,
    winner: ScoredCandidate | None,
    evaluated_at: str,
) -> dict[str, Any]:
    candidates_out: list[dict[str, Any]] = []
    for s in scored:
        lbl = evidence_match_label(s.name_score, s.suburb_match, s.distance_m)
        if not (s.name_score >= 70 or (s.name_score >= 50 and s.suburb_match)):
            continue
        lic = s.lic
        candidates_out.append(
            {
                "license_id": int(lic["license_id"]),
                "license_number": _as_text(lic.get("license_number")),
                "trading_name": _as_text(lic.get("trading_name")),
                "licensee_legal_name": _as_text(lic.get("licensee_legal_name")),
                "license_type": _as_text(lic.get("license_type")),
                "state_code": _as_text(lic.get("state_code")),
                "abn_from_register": _as_text(lic.get("abn_from_register")),
                "acn_from_register": _as_text(lic.get("acn_from_register")),
                "match_confidence": lbl,
                "match_signals": {
                    "name_score": s.name_score,
                    "suburb_match": s.suburb_match,
                    "geocode_distance_m": (int(round(s.distance_m)) if s.distance_m is not None else None),
                },
            }
        )
    winner_id = int(winner.lic["license_id"]) if winner is not None else None
    win_conf = outcome if outcome != "no_match" else None
    if outcome == "multiple_candidates":
        win_conf = "multiple_candidates"
    elif outcome == "no_match":
        winner_id = None
        win_conf = "no_match"
    return {
        "winner_license_id": winner_id,
        "winner_confidence": win_conf,
        "candidates": candidates_out,
        "evaluated_at": evaluated_at,
    }


def derive_enrichment_fields(
    scored: list[ScoredCandidate],
    chain_upper_keys: dict[str, str],
    venue_abn: str | None,
) -> dict[str, Any]:
    """Aggregate augmentation columns from fuzzy-or-better licence matches."""
    fuzzy_plus = [s for s in scored if s.level in ("exact", "strong", "fuzzy")]
    if not fuzzy_plus:
        return {}

    legal_names: list[str] = []
    licence_nums: list[str] = []
    categories: list[str] = []
    abns: list[str] = []
    acns: list[str] = []
    chain_hits: list[str] = []

    seen_l = set()
    seen_n = set()
    seen_c = set()
    seen_a = set()
    seen_acn = set()

    for s in fuzzy_plus:
        lic = s.lic
        ln = _as_text(lic.get("licensee_legal_name"))
        if ln and ln not in seen_l:
            seen_l.add(ln)
            legal_names.append(ln)
        num = _as_text(lic.get("license_number"))
        if num and num not in seen_n:
            seen_n.add(num)
            licence_nums.append(num)
        cat = map_venue_category(_as_text(lic.get("state_code")), _as_text(lic.get("license_type")))
        if cat not in seen_c:
            seen_c.add(cat)
            categories.append(cat)
        abn = _as_text(lic.get("abn_from_register"))
        if abn and abn not in seen_a:
            seen_a.add(abn)
            abns.append(abn)
        acn = _as_text(lic.get("acn_from_register"))
        if acn and acn not in seen_acn:
            seen_acn.add(acn)
            acns.append(acn)
        if ln:
            ck = ln.strip().upper()
            if ck in chain_upper_keys:
                chain_hits.append(chain_upper_keys[ck])

    name_to_reg_type: dict[str, str] = {}
    for s in fuzzy_plus:
        ln = _as_text(s.lic.get("licensee_legal_name"))
        if not ln:
            continue
        lt = _as_text(s.lic.get("licensee_type"))
        if lt and ln not in name_to_reg_type:
            name_to_reg_type[ln] = lt

    types_with_conf: list[tuple[str, OrgConfidence]] = []
    for name in legal_names:
        types_with_conf.append(
            classify_licensee_org(name, chain_upper_keys, name_to_reg_type.get(name))
        )
    org_type, org_conf = merge_org_types(types_with_conf)
    is_chain = bool(chain_hits)
    chain_name = chain_hits[0] if chain_hits else None

    agreement = abn_agreement(venue_abn, abns)

    return {
        "organisation_type": org_type,
        "organisation_type_confidence": org_conf,
        "venue_category_evidence": categories,
        "licensee_legal_names": legal_names,
        "licence_numbers": licence_nums,
        "licence_count": len(licence_nums),
        "is_chain_operator": is_chain,
        "chain_operator_name": chain_name,
        "abn_candidates": abns,
        "acn_candidates": acns,
        "abn_evidence_agreement": agreement,
    }


def no_match_payload() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "organisation_type": None,
        "organisation_type_confidence": None,
        "venue_category_evidence": None,
        "licensee_legal_names": None,
        "licence_numbers": None,
        "licence_count": 0,
        "is_chain_operator": None,
        "chain_operator_name": None,
        "abn_candidates": None,
        "acn_candidates": None,
        "abn_evidence_agreement": "no_licence_evidence",
        "liquor_license_evidence": None,
        "enrichment_run_at": now.isoformat(),
    }


def matched_payload(
    evidence: dict[str, Any],
    derived: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    out = {**derived, "liquor_license_evidence": evidence, "enrichment_run_at": now.isoformat()}
    return out


ALLOWED_UPDATE_KEYS = frozenset(
    {
        "organisation_type",
        "organisation_type_confidence",
        "venue_category_evidence",
        "licensee_legal_names",
        "licence_numbers",
        "licence_count",
        "is_chain_operator",
        "chain_operator_name",
        "abn_candidates",
        "acn_candidates",
        "abn_evidence_agreement",
        "liquor_license_evidence",
        "enrichment_run_at",
    }
)


def flush_updates(sb: Any, batch: list[tuple[str, dict[str, Any]]]) -> None:
    for vid, payload in batch:
        extra = set(payload.keys()) - ALLOWED_UPDATE_KEYS
        if extra:
            raise RuntimeError(f"Refusing update with disallowed keys: {sorted(extra)}")
        sb.table("venues").update(payload).eq("id", vid).execute()


def preflight(sb: Any, db_url: str | None) -> None:
    try:
        sb.table("venues").select(
            "id,organisation_type,licensee_legal_names,abn_candidates,"
            "abn_evidence_agreement,liquor_license_evidence,enrichment_run_at"
        ).limit(1).execute()
    except Exception as exc:
        LOG.error("Preflight failed: venues augmentation columns missing: %s", exc)
        raise SystemExit(1) from exc
    try:
        sb.schema("shared").table("ref_liquor_licenses").select("license_id").limit(1).execute()
    except Exception as exc:
        LOG.error("Preflight failed: cannot read shared.ref_liquor_licenses: %s", exc)
        raise SystemExit(1) from exc
    if (db_url or "").strip():
        import psycopg

        try:
            with psycopg.connect(db_url.strip()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM shared.mv_chain_operators LIMIT 1")
                    if cur.fetchone() is None:
                        LOG.error("Preflight failed: shared.mv_chain_operators returned no rows.")
                        raise SystemExit(1)
        except Exception as exc:
            LOG.error("Preflight failed: cannot read shared.mv_chain_operators via Postgres: %s", exc)
            raise SystemExit(1) from exc
    else:
        try:
            sb.schema("shared").table("mv_chain_operators").select("licensee_legal_name").limit(1).execute()
        except Exception as exc:
            LOG.error(
                "Preflight failed: shared.mv_chain_operators inaccessible via REST (%s). "
                "Set DATABASE_URL to read it over Postgres.",
                exc,
            )
            raise SystemExit(1) from exc

    tallies: dict[str, int] = {}
    tbl = sb.schema("shared").table("ref_liquor_licenses")
    for st in ("NSW", "VIC", "TAS", "SA"):
        resp = tbl.select("license_id", count="exact").eq("state_code", st).limit(1).execute()
        n = getattr(resp, "count", None)
        if n is None:
            LOG.error("Preflight failed: could not count licences for %s", st)
            raise SystemExit(1)
        tallies[st] = int(n)
    missing_states = [s for s in ("NSW", "VIC", "TAS", "SA") if tallies.get(s, 0) == 0]
    if missing_states:
        LOG.error("Preflight failed: liquor register missing states: %s", missing_states)
        raise SystemExit(1)
    total = sum(tallies.get(s, 0) for s in ("NSW", "VIC", "TAS", "SA"))
    if total < 40_000:
        LOG.error("Preflight failed: ref_liquor_licenses row count %s < 40,000", total)
        raise SystemExit(1)
    LOG.info(
        "Preflight OK: liquor rows NSW=%s VIC=%s TAS=%s SA=%s (total targeted=%s)",
        tallies.get("NSW", 0),
        tallies.get("VIC", 0),
        tallies.get("TAS", 0),
        tallies.get("SA", 0),
        total,
    )


def fetch_venues(sb: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            sb.table("venues")
            .select("id,name,address,suburb,postcode,state,lat,lng,abn,place_id")
            .order("id")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def run_phase_a_db(url: str) -> None:
    import psycopg

    vic_update = """
    UPDATE shared.ref_liquor_licenses
    SET lat = NULLIF(raw_data->>'Latitude', '')::numeric,
        lng = NULLIF(raw_data->>'Longitude', '')::numeric
    WHERE state_code='VIC'
      AND lat IS NULL
      AND raw_data ? 'Latitude'
      AND raw_data->>'Latitude' != '';
    """
    refresh = "REFRESH MATERIALIZED VIEW shared.mv_chain_operators;"
    with psycopg.connect(url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(vic_update)
            cur.execute(
                "SELECT count(*) FROM shared.ref_liquor_licenses "
                "WHERE state_code='VIC' AND lat IS NOT NULL;"
            )
            n = cur.fetchone()[0]
            LOG.info("Phase A: VIC rows with lat after backfill = %s", n)
            if n < 15_000:
                raise SystemExit("Phase A verification failed: VIC lat count < 15,000")
        with conn.cursor() as cur:
            cur.execute(refresh)
    LOG.info("Phase A complete (materialised view refreshed).")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = ArgumentParser(description="Additive venue enrichment from liquor registers v2.")
    parser.add_argument(
        "--phase-a",
        action="store_true",
        help="Run Phase A SQL against DATABASE_URL (VIC lat/lng + refresh mv_chain_operators).",
    )
    args = parser.parse_args()
    load_env()
    if args.phase_a:
        import os

        db = (os.environ.get("DATABASE_URL") or "").strip()
        if not db:
            LOG.error("DATABASE_URL required for --phase-a.")
            raise SystemExit(1)
        run_phase_a_db(db)
        return

    import os

    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required.")
        raise SystemExit(1)

    from supabase import create_client

    sb = create_client(url, key)
    db_url = (os.environ.get("DATABASE_URL") or "").strip()
    preflight(sb, db_url or None)

    t0 = time.perf_counter()
    LOG.info("Loading licence register into memory...")
    licences = fetch_all_licences(sb)
    by_pc, by_sub, geo = build_licence_indices(licences)
    chain_keys = fetch_chain_map(sb, db_url or None)
    LOG.info("Indexed %s licences; %s chain operator rows.", len(licences), len(chain_keys))

    venues = fetch_venues(sb)
    LOG.info("Processing %s venues.", len(venues))

    batch: list[tuple[str, dict[str, Any]]] = []
    stats: dict[str, int] = {}
    matched_unknown = 0
    matched_total = 0

    for i, v in enumerate(venues, start=1):
        vid = str(v["id"])
        v_pc = _as_text(v.get("postcode"))
        v_sub = _as_text(v.get("suburb"))
        v_lat, v_lng = _as_float(v.get("lat")), _as_float(v.get("lng"))
        cand_map = narrow_candidates(v_pc, v_sub, v_lat, v_lng, by_pc, by_sub, geo)
        scored = score_venue_candidates(v.get("name"), v_sub, v_lat, v_lng, cand_map)
        outcome, winner, _levels = decide_match(scored)
        evaluated_at = datetime.now(timezone.utc).isoformat()

        if outcome == "no_match":
            payload = no_match_payload()
        else:
            matched_total += 1
            evidence = build_evidence_payload(scored, outcome, winner, evaluated_at)
            stats[outcome] = stats.get(outcome, 0) + 1
            derived = derive_enrichment_fields(scored, chain_keys, _as_text(v.get("abn")))
            if derived.get("organisation_type") == "UNKNOWN":
                matched_unknown += 1
            payload = matched_payload(evidence, derived)

        batch.append((vid, payload))
        if i % CHECKPOINT_EVERY == 0:
            LOG.info("Checkpoint: processed %s / %s venues.", i, len(venues))
        if len(batch) >= WRITE_BATCH_SIZE:
            flush_updates(sb, batch)
            batch = []

    if batch:
        flush_updates(sb, batch)

    elapsed = time.perf_counter() - t0
    unk_pct = (matched_unknown / matched_total * 100) if matched_total else 0.0
    LOG.info(
        "Finished enrichment: matched=%s (breakdown=%s), matched UNKNOWN share=%.1f%%, runtime=%.1fs",
        matched_total,
        stats,
        unk_pct,
        elapsed,
    )
    if matched_total and unk_pct > 20.0:
        LOG.error("Validation threshold exceeded: >20%% of matched venues typed UNKNOWN.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()