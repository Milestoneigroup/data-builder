"""Enrich ``public.venues`` from ``shared.ref_liquor_licenses`` (local database only).

Run: ``python -m scrapers.enrich_venues_from_liquor``

Requires ``DATABASE_URL`` (direct Postgres URI) in ``env.local`` for ``REFRESH`` and bulk
``UPDATE`` statements. Also requires ``SUPABASE_URL`` / ``SUPABASE_SERVICE_ROLE_KEY`` only if
you later extend this module; the default path uses Psycopg only.

**Augmentation only:** this job must never write to original venue fields such as ``name``,
``address``, ``suburb``, ``postcode``, ``state``, ``lat``, or ``lng``.
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Literal

from dotenv import load_dotenv
from rapidfuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]

LOG = logging.getLogger("enrich_venues_from_liquor")

MIN_LICENCE_ROWS: Final[int] = 40_000
REQUIRED_STATES: Final[frozenset[str]] = frozenset({"NSW", "VIC", "TAS", "SA"})
VIC_LAT_MIN_EXPECTED: Final[int] = 15_000

MatchConfidence = Literal["exact", "strong", "fuzzy", "multiple_candidates", "no_match"]
MatchTier = Literal["exact", "strong", "fuzzy", "weak"]

REQUIRED_VENUE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "organisation_type",
        "organisation_type_confidence",
        "licensee_legal_names",
        "licence_numbers",
        "licence_count",
        "venue_category_evidence",
        "abn_candidates",
        "acn_candidates",
        "liquor_license_evidence",
        "is_chain_operator",
        "chain_operator_name",
        "enrichment_run_at",
        "liquor_license_id",
        "liquor_license_match_confidence",
        "liquor_license_matched_at",
        "suburb",
    }
)

_ARTICLE_LEAD = re.compile(r"^(the|le|la)\s+", re.I)
_PUNCT = re.compile(r"[^\w\s]+")
_WS = re.compile(r"\s+")

# Longer phrases first within the trailing-entity strip list.
_TRAILING_ENTITIES: Final[tuple[str, ...]] = (
    "function centre",
    "cellar door",
    "hotel",
    "restaurant",
    "pub",
    "bar",
    "tavern",
    "club",
    "cafe",
    "winery",
)


def load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres (WGS84 sphere approximation)."""
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _strip_trailing_entities(s: str) -> str:
    t = s
    changed = True
    while changed:
        changed = False
        low = t.lower()
        for ent in _TRAILING_ENTITIES:
            if low.endswith(ent):
                t = t[: -len(ent)].strip()
                low = t.lower()
                changed = True
                break
    return t


def normalise_venue_licence_name(raw: str) -> str:
    """Normalise free-text names before ``token_set_ratio`` comparison."""
    s = (raw or "").lower().strip()
    s = _PUNCT.sub(" ", s)
    s = _ARTICLE_LEAD.sub("", s)
    s = _strip_trailing_entities(s)
    s = _WS.sub(" ", s).strip()
    return s


def normalise_suburb_key(s: str | None) -> str:
    return (s or "").strip().lower()


def normalise_postcode(s: str | None) -> str:
    return (s or "").strip()


def normalise_chain_key(s: str | None) -> str:
    return _WS.sub(" ", (s or "").strip().lower())


def trading_text(lic: dict[str, Any]) -> str:
    t = lic.get("trading_name")
    if t is not None and str(t).strip():
        return str(t).strip()
    leg = lic.get("licensee_legal_name")
    return str(leg or "").strip()


def map_venue_category(state_code: str | None, license_type: str | None) -> str:
    s = (state_code or "").strip().upper()
    lt = (license_type or "").strip()
    if not lt:
        return "OTHER"

    if s == "NSW":
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

    if s == "VIC":
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

    if s == "TAS":
        tas = {
            "General": "HOTEL",
            "On-Restaurant": "RESTAURANT",
            "On": "ON_PREMISES",
            "Club": "CLUB",
            "Special": "SPECIAL_OCCASION",
            "Off": "BOTTLE_SHOP",
        }
        return tas.get(lt, "OTHER")

    if s == "SA":
        sa = {
            "Hotel Licence": "HOTEL",
            "Restaurant Licence": "RESTAURANT",
            "Club Licence": "CLUB",
            "Producer's Licence": "WINERY",
            "Direct Sales Licence": "WHOLESALE",
            "Wholesale Licence": "WHOLESALE",
            "Limited": "OTHER",
            "Special Circumstances": "OTHER",
        }
        return sa.get(lt, "OTHER")

    if "Limited" in lt or "Special Circumstances" in lt:
        return "OTHER"

    return "OTHER"


RuleConf = Literal["high", "medium", "low"]


def organisation_type_from_licensee(name: str | None) -> tuple[str, RuleConf]:
    """Return ``(organisation_type, rule_confidence)`` from licensee legal/trading text."""
    if not name or not str(name).strip():
        return "UNKNOWN", "low"

    raw = str(name).strip().upper()

    gov_markers = (
        "COUNCIL",
        "CITY OF ",
        "CROWN",
        "GOVERNMENT",
        "DEPARTMENT OF",
    )
    if any(m in raw for m in gov_markers):
        return "GOVERNMENT", "high"

    if re.match(r"^(MR|MRS|MS|MISS|DR|PROF)\s+", raw):
        return "INDIVIDUAL", "high"

    club_markers = (
        "RSL",
        "SUB-BRANCH",
        "BOWLING CLUB",
        "CLUB INC",
        "GOLF CLUB",
    )
    if any(m in raw for m in club_markers):
        return "CLUB", "high"

    trust_markers = ("TRUST", "TRUSTEE", "AS TRUSTEE FOR", " ATF ", " ATF")
    if any(m in raw for m in trust_markers):
        return "TRUST", "medium"

    company_suffixes = ("PTY LTD", "PTY. LTD.", "LIMITED", "LTD", "LIMITED.")
    for suf in company_suffixes:
        if raw.endswith(suf):
            return "COMPANY", "high"

    return "UNKNOWN", "low"


def _rule_rank(t: str) -> int:
    order = ("GOVERNMENT", "INDIVIDUAL", "CLUB", "TRUST", "COMPANY", "GROUP", "UNKNOWN")
    try:
        return order.index(t)
    except ValueError:
        return len(order)


def derive_organisation_profiles(
    evidence: list[dict[str, Any]],
    chain_licensee_norms: frozenset[str],
) -> tuple[str, RuleConf, bool, str | None]:
    """Pick venue-level organisation labels from evidence payloads."""
    if not evidence:
        return "UNKNOWN", "low", False, None

    # Collect one row per distinct licensee string appearing in evidence.
    licensees: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence:
        leg = row.get("licensee_legal_name")
        if leg and str(leg).strip():
            licensees[str(leg).strip()].append(row)

    if not licensees:
        return "UNKNOWN", "low", False, None

    type_scores: dict[str, int] = defaultdict(int)
    type_best_conf: dict[str, RuleConf] = {}
    chain_hit = False
    chain_display: str | None = None

    for lic_name in licensees:
        key = normalise_chain_key(lic_name)
        base_type, base_conf = organisation_type_from_licensee(lic_name)
        resolved_type = base_type
        resolved_conf: RuleConf = base_conf

        if key and key in chain_licensee_norms:
            resolved_type = "GROUP"
            resolved_conf = "high"
            chain_hit = True
            chain_display = chain_display or lic_name

        type_scores[resolved_type] += len(licensees[lic_name])
        prev = type_best_conf.get(resolved_type)
        if prev is None or _conf_rank(resolved_conf) > _conf_rank(prev):
            type_best_conf[resolved_type] = resolved_conf

    best_type = max(type_scores, key=lambda t: (type_scores[t], -_rule_rank(t)))
    best_conf: RuleConf | None = type_best_conf.get(best_type)
    return best_type, best_conf or "low", chain_hit, chain_display


def _conf_rank(c: RuleConf) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(c, 0)


@dataclass
class ScoredCandidate:
    lic: dict[str, Any]
    name_score: float
    suburb_match: bool
    geocode_distance_m: float | None
    tier: MatchTier = "weak"


def classify_tier(
    name_score: float, suburb_match: bool, geocode_distance_m: float | None
) -> MatchTier:
    if name_score >= 95 and suburb_match and (geocode_distance_m is None or geocode_distance_m < 500):
        return "exact"
    if name_score >= 85 and suburb_match and (geocode_distance_m is None or geocode_distance_m < 1000):
        return "strong"
    if name_score >= 70 and suburb_match:
        return "fuzzy"
    return "weak"


def _tiebreak_winner(pool: list[ScoredCandidate]) -> ScoredCandidate:
    """Prefer higher name score, then smaller geodesic distance, then stable licence id."""

    def sort_key(sc: ScoredCandidate) -> tuple[float, float, int]:
        dist = sc.geocode_distance_m
        dist_key = dist if dist is not None else float("inf")
        lid = int(sc.lic.get("license_id") or 0)
        return (-sc.name_score, dist_key, lid)

    return sorted(pool, key=sort_key)[0]


def pick_winner(
    scored: list[ScoredCandidate],
) -> tuple[MatchConfidence, ScoredCandidate | None]:
    """Return overall match label and single winner (if unambiguous)."""
    exact = [s for s in scored if s.tier == "exact"]
    strong_only = [s for s in scored if s.tier == "strong"]
    strong_plus = [s for s in scored if s.tier in ("exact", "strong")]
    fuzzy_list = [s for s in scored if s.tier == "fuzzy"]

    if len(strong_plus) >= 2:
        return "multiple_candidates", None
    if len(exact) == 1:
        return "exact", _tiebreak_winner(exact)
    if not exact and len(strong_only) == 1:
        return "strong", strong_only[0]
    if not strong_plus:
        if len(fuzzy_list) >= 2:
            return "multiple_candidates", None
        if len(fuzzy_list) == 1:
            return "fuzzy", fuzzy_list[0]

    return "no_match", None


def build_evidence_payload(
    scored: list[ScoredCandidate],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sc in scored:
        if sc.name_score < 50:
            continue
        if sc.name_score < 70 and not sc.suburb_match:
            continue
        lic = sc.lic
        mc: MatchConfidence
        if sc.tier == "exact":
            mc = "exact"
        elif sc.tier == "strong":
            mc = "strong"
        elif sc.tier == "fuzzy":
            mc = "fuzzy"
        else:
            mc = "no_match"

        geo_i: int | None
        if sc.geocode_distance_m is None:
            geo_i = None
        else:
            geo_i = int(round(sc.geocode_distance_m))

        out.append(
            {
                "license_id": int(lic["license_id"]),
                "license_number": str(lic.get("license_number") or ""),
                "trading_name": str(lic.get("trading_name") or ""),
                "licensee_legal_name": str(lic.get("licensee_legal_name") or ""),
                "license_type": str(lic.get("license_type") or ""),
                "state_code": str(lic.get("state_code") or ""),
                "match_confidence": mc,
                "match_signals": {
                    "name_score": round(float(sc.name_score), 2),
                    "suburb_match": sc.suburb_match,
                    "geocode_distance_m": geo_i,
                },
            }
        )
    return out


def aggregate_enrichment(evidence: list[dict[str, Any]]) -> dict[str, Any]:
    legals: set[str] = set()
    numbers: set[str] = set()
    cats: set[str] = set()
    abns: set[str] = set()
    acns: set[str] = set()

    for row in evidence:
        leg = row.get("licensee_legal_name")
        if leg and str(leg).strip():
            legals.add(str(leg).strip())
        num = row.get("license_number")
        if num and str(num).strip():
            numbers.add(str(num).strip())
        lt = row.get("license_type")
        st = row.get("state_code")
        cats.add(map_venue_category(str(st or ""), str(lt or "") if lt else None))

        # ABN / ACN only available on licence rows; evidence JSON lacks them — caller merges.

    sorted_legal = sorted(legals)
    sorted_nums = sorted(numbers)
    sorted_cats = sorted(cats)
    return {
        "licensee_legal_names": sorted_legal,
        "licence_numbers": sorted_nums,
        "licence_count": len(sorted_nums),
        "venue_category_evidence": sorted_cats,
        "abn_candidates": sorted(abns),
        "acn_candidates": sorted(acns),
    }


@dataclass
class PhaseTimers:
    t0: float = field(default_factory=time.perf_counter)

    def total_s(self) -> float:
        return time.perf_counter() - self.t0


def preflight_tables(cur: Any) -> None:
    cur.execute(
        "SELECT state_code, count(*)::bigint AS n FROM shared.ref_liquor_licenses GROUP BY state_code;"
    )
    rows = cur.fetchall()
    counts = {str(r[0]).upper(): int(r[1]) for r in rows}
    missing_states = REQUIRED_STATES - frozenset(counts)
    total = sum(counts.values())

    LOG.info(
        "Preflight Liquor licence counts — total=%s by_state=%s",
        total,
        json.dumps(counts, sort_keys=True),
    )
    if total < MIN_LICENCE_ROWS:
        raise SystemExit(
            f"Preflight failed: ref_liquor_licenses has {total} rows (<{MIN_LICENCE_ROWS})."
        )
    if missing_states:
        raise SystemExit(
            f"Preflight failed: missing state rows in ref_liquor_licenses: {sorted(missing_states)}."
        )

    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'venues';
        """
    )
    have = {r[0] for r in cur.fetchall()}
    missing_cols = REQUIRED_VENUE_COLUMNS - have
    if missing_cols:
        raise SystemExit(
            "Preflight failed: public.venues is missing Step 0 augmentation columns "
            f"(apply venue enrichment migration): {sorted(missing_cols)}"
        )

    cur.execute(
        """
        SELECT 1 FROM pg_matviews
        WHERE schemaname = 'shared' AND matviewname = 'mv_chain_operators'
        LIMIT 1;
        """
    )
    if cur.fetchone() is None:
        raise SystemExit(
            "Preflight failed: materialised view shared.mv_chain_operators not found "
            "(create or refresh Step 0 objects before running this job)."
        )

    LOG.info(
        "Preflight passed — venues has %s required augmentation columns present; liquor states OK.",
        len(REQUIRED_VENUE_COLUMNS),
    )


def phase_a_vic_geocode(cur: Any) -> int:
    """Backfill Victorian rows where lat/lng live in JSON only. Idempotent."""
    LOG.info(
        "Phase A — inspecting VIC geocode coverage "
        "(raw_data latitude/longitude JSON keys)."
    )
    keys_sql = """
    SELECT DISTINCT jsonb_object_keys(raw_data) AS key
    FROM shared.ref_liquor_licenses
    WHERE state_code = 'VIC'
    ORDER BY 1;
    """
    cur.execute(keys_sql)
    vic_keys = [r[0] for r in cur.fetchall()]
    LOG.info("VIC raw_data distinct keys (%s): %s", len(vic_keys), vic_keys)

    lat_key, lng_key = "Latitude", "Longitude"
    if lat_key not in vic_keys or lng_key not in vic_keys:
        raise SystemExit(
            f"Phase A failed: expected JSON keys {lat_key!r} and {lng_key!r}; "
            f"found key set {vic_keys!r}."
        )

    cur.execute(
        f"""
        UPDATE shared.ref_liquor_licenses
        SET lat = NULLIF(raw_data->>'{lat_key}', '')::numeric,
            lng = NULLIF(raw_data->>'{lng_key}', '')::numeric
        WHERE state_code = 'VIC'
          AND lat IS NULL
          AND raw_data ? '{lat_key}'
          AND raw_data ? '{lng_key}'
          AND raw_data->>'{lat_key}' <> ''
          AND raw_data->>'{lng_key}' <> '';
        """
    )
    cur.execute(
        """
        SELECT count(*)::bigint
        FROM shared.ref_liquor_licenses
        WHERE state_code = 'VIC' AND lat IS NOT NULL;
        """
    )
    n = int(cur.fetchone()[0])
    LOG.info("Phase A — VIC rows with non-null lat after update: %s", n)
    if n < VIC_LAT_MIN_EXPECTED:
        raise SystemExit(
            f"Phase A failed: only {n} VIC rows have lat (expected ≥{VIC_LAT_MIN_EXPECTED})."
        )

    LOG.info("Phase A — refreshing shared.mv_chain_operators …")
    cur.execute("REFRESH MATERIALIZED VIEW shared.mv_chain_operators;")
    return n


def load_chain_norms(cur: Any) -> frozenset[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'shared' AND table_name = 'mv_chain_operators';
        """
    )
    cols = {r[0] for r in cur.fetchall()}
    # Prefer a column that holds the canonical licensee / operator name.
    preferred = (
        "licensee_legal_name",
        "operator_licensee_name",
        "canonical_licensee",
        "chain_licensee",
        "operator_name",
    )
    chosen = next((c for c in preferred if c in cols), None)
    if chosen is None:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'shared' AND table_name = 'mv_chain_operators'
              AND data_type IN ('text', 'character varying');
            """
        )
        text_cols = [r[0] for r in cur.fetchall()]
        raise SystemExit(
            "Could not pick a text column on shared.mv_chain_operators for chain matching. "
            f"Columns present: {sorted(cols)}; text columns: {text_cols}"
        )

    from psycopg import sql

    cur.execute(
        sql.SQL("SELECT DISTINCT {} FROM shared.mv_chain_operators").format(sql.Identifier(chosen))
    )
    norms: set[str] = set()
    for (val,) in cur.fetchall():
        if val is None or not str(val).strip():
            continue
        norms.add(normalise_chain_key(str(val)))
    LOG.info(
        "Loaded %s distinct normalised chain operator names from mv_chain_operators.%s.",
        len(norms),
        chosen,
    )
    return frozenset(norms)


def load_licences(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT license_id, license_number, trading_name, licensee_legal_name,
               license_type, state_code, suburb, postcode, premises_state,
               lat, lng, abn_from_register, acn_from_register
        FROM shared.ref_liquor_licenses;
        """
    )
    cols = [d[0] for d in cur.description]
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        out.append(dict(zip(cols, row, strict=True)))
    return out


def load_venues(cur: Any) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT id, name, address, suburb, postcode, state, lat, lng
        FROM public.venues;
        """
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def index_licences(licences: list[dict[str, Any]]) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    by_suburb: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_postcode: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for lic in licences:
        sk = normalise_suburb_key(str(lic.get("suburb") or ""))
        if sk:
            by_suburb[sk].append(lic)
        pk = normalise_postcode(str(lic.get("postcode") or ""))
        if pk:
            by_postcode[pk].append(lic)
    return by_suburb, by_postcode


def candidate_licences_for_venue(
    venue: dict[str, Any],
    by_suburb: dict[str, list[dict]],
    by_postcode: dict[str, list[dict]],
) -> list[dict[str, Any]]:
    v_sub_k = normalise_suburb_key(str(venue.get("suburb") or ""))
    v_pc = normalise_postcode(str(venue.get("postcode") or ""))

    cand: dict[int, dict[str, Any]] = {}

    def state_ok(lic: dict[str, Any]) -> bool:
        vs = str(venue.get("state") or "").strip().upper()
        ps = str(lic.get("premises_state") or "").strip().upper()
        if not vs or not ps:
            return True
        return ps == vs

    if v_sub_k:
        for lic in by_suburb.get(v_sub_k, []):
            if state_ok(lic):
                cand[int(lic["license_id"])] = lic

    v_lat = venue.get("lat")
    v_lng = venue.get("lng")

    try:
        vlat_f = float(v_lat) if v_lat is not None else None
        vlng_f = float(v_lng) if v_lng is not None else None
    except (TypeError, ValueError):
        vlat_f, vlng_f = None, None

    if v_pc and vlat_f is not None and vlng_f is not None:
        for lic in by_postcode.get(v_pc, []):
            if not state_ok(lic):
                continue
            llat, llng = lic.get("lat"), lic.get("lng")
            try:
                flat = float(llat) if llat is not None else None
                flng = float(llng) if llng is not None else None
            except (TypeError, ValueError):
                flat, flng = None, None
            if flat is None or flng is None:
                continue
            d = haversine_m(vlat_f, vlng_f, flat, flng)
            if d >= 2000:
                continue
            cand[int(lic["license_id"])] = lic

    return list(cand.values())


def score_candidates(venue: dict[str, Any], candidates: list[dict[str, Any]]) -> list[ScoredCandidate]:
    v_name = normalise_venue_licence_name(str(venue.get("name") or ""))
    v_sub_k = normalise_suburb_key(str(venue.get("suburb") or ""))

    v_lat = venue.get("lat")
    v_lng = venue.get("lng")
    try:
        vlat_f = float(v_lat) if v_lat is not None else None
        vlng_f = float(v_lng) if v_lng is not None else None
    except (TypeError, ValueError):
        vlat_f, vlng_f = None, None

    out: list[ScoredCandidate] = []

    for lic in candidates:
        l_sub_k = normalise_suburb_key(str(lic.get("suburb") or ""))
        suburb_match = bool(v_sub_k and l_sub_k and v_sub_k == l_sub_k)

        tname = trading_text(lic)
        l_name = normalise_venue_licence_name(tname)
        score = float(fuzz.token_set_ratio(v_name, l_name)) if (v_name or l_name) else 0.0

        geo: float | None = None
        llat, llng = lic.get("lat"), lic.get("lng")
        try:
            flat = float(llat) if llat is not None else None
            flng = float(llng) if llng is not None else None
        except (TypeError, ValueError):
            flat, flng = None, None

        if vlat_f is not None and vlng_f is not None and flat is not None and flng is not None:
            geo = haversine_m(vlat_f, vlng_f, flat, flng)

        tier = classify_tier(score, suburb_match, geo)
        out.append(
            ScoredCandidate(
                lic=lic,
                name_score=score,
                suburb_match=suburb_match,
                geocode_distance_m=geo,
                tier=tier,
            )
        )
    return out


def merge_abn_acn_into_aggregate(
    aggregate: dict[str, Any],
    licence_by_id: dict[int, dict[str, Any]],
    evidence: list[dict[str, Any]],
) -> None:
    abns: set[str] = set(aggregate["abn_candidates"])
    acns: set[str] = set(aggregate["acn_candidates"])
    for row in evidence:
        lid = row.get("license_id")
        if lid is None:
            continue
        lic = licence_by_id.get(int(lid))
        if not lic:
            continue
        abn = lic.get("abn_from_register")
        acn = lic.get("acn_from_register")
        if abn and str(abn).strip():
            abns.add(str(abn).strip())
        if acn and str(acn).strip():
            acns.add(str(acn).strip())
    aggregate["abn_candidates"] = sorted(abns)
    aggregate["acn_candidates"] = sorted(acns)


def run_validation_queries(cur: Any) -> None:
    """Print §7 validation outputs verbatim to the log (copy into runbook)."""
    blocks = [
        (
            "Q1 Match confidence distribution",
            """
            SELECT liquor_license_match_confidence, count(*)
            FROM public.venues
            WHERE enrichment_run_at IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC;
            """,
        ),
        (
            "Q2 Organisation type distribution",
            """
            SELECT organisation_type, organisation_type_confidence, count(*)
            FROM public.venues
            WHERE organisation_type IS NOT NULL
            GROUP BY 1, 2 ORDER BY 1, 3 DESC;
            """,
        ),
        (
            "Q3 Chain operators detected",
            """
            SELECT chain_operator_name, count(*) AS venues_in_chain
            FROM public.venues
            WHERE is_chain_operator = true
            GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
            """,
        ),
        (
            "Q4 ABN/ACN candidates discovered",
            """
            SELECT
              count(*) FILTER (WHERE array_length(abn_candidates, 1) > 0) AS venues_with_abn_candidate,
              count(*) FILTER (WHERE array_length(acn_candidates, 1) > 0) AS venues_with_acn_candidate,
              count(*) FILTER (WHERE array_length(licensee_legal_names, 1) > 0) AS venues_with_legal_name,
              count(*) AS total_venues
            FROM public.venues;
            """,
        ),
        (
            "Q5 Venue category evidence (top 10)",
            """
            SELECT unnest(venue_category_evidence) AS category, count(*) AS n
            FROM public.venues WHERE venue_category_evidence IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
            """,
        ),
        (
            "Q6 Sanity: no override of original fields",
            """
            SELECT id, name, address, suburb FROM public.venues
            WHERE name IS NULL OR address IS NULL LIMIT 10;
            """,
        ),
        (
            "Q7 Spot check — random matched venues",
            """
            SELECT id, name, suburb,
                   organisation_type,
                   is_chain_operator,
                   licensee_legal_names,
                   licence_numbers,
                   venue_category_evidence,
                   liquor_license_match_confidence
            FROM public.venues
            WHERE liquor_license_id IS NOT NULL
            ORDER BY random() LIMIT 5;
            """,
        ),
    ]
    for title, sql in blocks:
        LOG.info("=== %s ===", title)
        cur.execute(sql)
        rows = cur.fetchall()
        colnames = [d[0] for d in cur.description]
        for line in _format_query_table(colnames, rows):
            LOG.info("%s", line)


def _format_query_table(cols: list[str], rows: list[tuple[Any, ...]]) -> list[str]:
    if not rows:
        return ["(0 rows)"]
    width = {c: len(c) for c in cols}
    for row in rows:
        for c, v in zip(cols, row, strict=True):
            width[c] = max(width[c], len(repr(v)))
    header = " | ".join(c.ljust(width[c]) for c in cols)
    sep = "-+-".join("-" * width[c] for c in cols)
    lines = [header, sep]
    for row in rows:
        lines.append(" | ".join(repr(v).ljust(width[c]) for c, v in zip(cols, row, strict=True)))
    lines.append(f"({len(rows)} rows)")
    return lines


def main() -> None:
    import os

    load_env()
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )
    timers = PhaseTimers()

    db_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not db_url:
        raise SystemExit("DATABASE_URL missing from env (required for Postgres writes).")

    import psycopg
    from psycopg.types.json import Json


    match_dist: dict[MatchConfidence, int] = defaultdict(int)
    total_venues = 0
    vic_lat_n = 0

    with psycopg.connect(db_url) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            preflight_tables(cur)
            vic_lat_n = phase_a_vic_geocode(cur)
            chain_norms = load_chain_norms(cur)
            licences = load_licences(cur)
            licence_by_id = {int(r["license_id"]): r for r in licences}
            by_suburb, by_postcode = index_licences(licences)
            venues = load_venues(cur)
            total_venues = len(venues)

            updates = 0
            for venue in venues:
                vid = venue["id"]
                cands = candidate_licences_for_venue(venue, by_suburb, by_postcode)
                scored = score_candidates(venue, cands)
                match_conf, winner = pick_winner(scored)
                evidence = build_evidence_payload(scored)

                if match_conf == "no_match":
                    cur.execute(
                        """
                        UPDATE public.venues SET
                          liquor_license_id = NULL,
                          liquor_license_match_confidence = 'no_match',
                          liquor_license_matched_at = NULL,
                          organisation_type = NULL,
                          organisation_type_confidence = NULL,
                          venue_category_evidence = NULL,
                          licensee_legal_names = NULL,
                          licence_numbers = NULL,
                          licence_count = NULL,
                          is_chain_operator = NULL,
                          chain_operator_name = NULL,
                          abn_candidates = NULL,
                          acn_candidates = NULL,
                          liquor_license_evidence = NULL,
                          enrichment_run_at = now()
                        WHERE id = %s;
                        """,
                        (vid,),
                    )
                    match_dist["no_match"] += 1
                    updates += 1
                    continue

                org_type, org_conf, is_chain, chain_name = derive_organisation_profiles(
                    evidence, chain_norms
                )
                agg = aggregate_enrichment(evidence)
                merge_abn_acn_into_aggregate(agg, licence_by_id, evidence)

                lic_id_final: int | None
                lc_final: MatchConfidence

                if match_conf == "multiple_candidates":
                    lic_id_final = None
                    lc_final = "multiple_candidates"
                elif winner:
                    lic_id_final = int(winner.lic["license_id"])
                    lc_final = match_conf  # exact | strong | fuzzy
                else:
                    lic_id_final = None
                    lc_final = "no_match"

                if lc_final == "no_match":
                    cur.execute(
                        """
                        UPDATE public.venues SET
                          liquor_license_id = NULL,
                          liquor_license_match_confidence = 'no_match',
                          liquor_license_matched_at = NULL,
                          organisation_type = NULL,
                          organisation_type_confidence = NULL,
                          venue_category_evidence = NULL,
                          licensee_legal_names = NULL,
                          licence_numbers = NULL,
                          licence_count = NULL,
                          is_chain_operator = NULL,
                          chain_operator_name = NULL,
                          abn_candidates = NULL,
                          acn_candidates = NULL,
                          liquor_license_evidence = NULL,
                          enrichment_run_at = now()
                        WHERE id = %s;
                        """,
                        (vid,),
                    )
                    match_dist["no_match"] += 1
                else:
                    cur.execute(
                        """
                        UPDATE public.venues SET
                          liquor_license_id = %s,
                          liquor_license_match_confidence = %s,
                          liquor_license_matched_at = CURRENT_DATE,
                          organisation_type = %s,
                          organisation_type_confidence = %s,
                          venue_category_evidence = %s,
                          licensee_legal_names = %s,
                          licence_numbers = %s,
                          licence_count = %s,
                          is_chain_operator = %s,
                          chain_operator_name = %s,
                          abn_candidates = %s,
                          acn_candidates = %s,
                          liquor_license_evidence = %s,
                          enrichment_run_at = now()
                        WHERE id = %s;
                        """,
                        (
                            lic_id_final,
                            lc_final,
                            org_type,
                            org_conf,
                            agg["venue_category_evidence"],
                            agg["licensee_legal_names"],
                            agg["licence_numbers"],
                            agg["licence_count"],
                            is_chain,
                            chain_name,
                            agg["abn_candidates"],
                            agg["acn_candidates"],
                            Json(evidence),
                            vid,
                        ),
                    )
                    match_dist[lc_final] += 1

                updates += 1

            conn.commit()

            LOG.info("Committed enrichment updates for %s venues.", updates)
            run_validation_queries(cur)
            conn.commit()

    elapsed = timers.total_s()
    LOG.info(
        "Finished venue enrichment — venues=%s, VIC_lat_backfill_rows=%s, "
        "match_distribution=%s, runtime_seconds=%.1f",
        total_venues,
        vic_lat_n,
        json.dumps(match_dist, sort_keys=True),
        elapsed,
    )


if __name__ == "__main__":
    main()
