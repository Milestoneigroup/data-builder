"""Map chain seed venues to ``public.venues`` and link them via ``public.venue_group_membership``.

- Upserts one row in ``public.venue_groups`` (keyed by ``group_slug``).
- Fuzzy-matches seed venues to existing rows (augmentation only: never updates venues).
- Falls back to Google Places text search, then inserts new venue rows when needed.
- Idempotent group memberships on ``(venue_id, group_id)``; extra seed lines that resolve
  to the same physical venue append audit detail to ``venue_group_membership.notes`` because
  the schema allows only one row per venue per group.

Run::

    python -m scrapers.map_venues_to_groups data/chain_seeds/merivale.json

Fuzzy matcher checks only::

    python -m scrapers.map_venues_to_groups --dry-run

Requires ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY``, ``GOOGLE_MAPS_API_KEY`` (or
``GOOGLE_PLACES_API_KEY``). Loads ``env.local`` from the repo root with ``override=True``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from dotenv import load_dotenv
from rapidfuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scrapers._utils.address_parser import parse_au_address  # noqa: E402
from scrapers._utils.chain_loader import load_chain_seed  # noqa: E402

TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TEXT_SEARCH_FIELD_MASK = (
    "places.id,places.displayName,places.formattedAddress,places.location,"
    "places.addressComponents,places.types,places.primaryType,places.websiteUri,"
    "places.nationalPhoneNumber"
)
PLACES_THROTTLE_S = 0.3
VENUE_PAGE_SIZE = 1000

logging.basicConfig(level=logging.INFO, format="%(asctime)s\t%(levelname)s\t%(message)s")
LOG = logging.getLogger("chain_mapper")


@dataclass
class MatchInfo:
    """Audit metadata for a membership row."""

    score: int
    confidence_label: str
    db_confidence: str
    matched_via: str
    notes: str


def load_env() -> None:
    load_dotenv(_ROOT / ".env", override=False)
    load_dotenv(_ROOT / ".env.local", override=True)
    load_dotenv(_ROOT / "env.local", override=True)


def _api_key() -> str:
    k = (os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or "").strip()
    if not k:
        raise RuntimeError("GOOGLE_MAPS_API_KEY (or GOOGLE_PLACES_API_KEY) is required")
    return k


def _supabase_client():
    from supabase import create_client  # noqa: PLC0415

    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    return create_client(url, key)


def check_preflight(sb: Any) -> None:
    for tbl in ("venue_groups", "venue_group_membership"):
        try:
            sb.table(tbl).select("*").limit(1).execute()
        except Exception as e:
            raise RuntimeError(
                f"Pre-flight failed: table public.{tbl} is not reachable ({e!s}). "
                "Apply the venue group schema before running this job."
            ) from e


def load_seed(path: Path) -> dict[str, Any]:
    return load_chain_seed(path)


def normalise_venue_name(name: str) -> str:
    s = name.lower().strip()
    for prefix in ("the ", "le ", "la "):
        if s.startswith(prefix):
            s = s[len(prefix) :]
    for suffix in (" sydney", " melbourne"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    s = s.replace("'", "").replace("-", " ")
    s = " ".join(s.split())
    return s


def normalise_suburb(s: Optional[str]) -> str:
    if not s:
        return ""
    return " ".join(str(s).lower().strip().split())


def normalise_state(s: Optional[str]) -> str:
    if not s:
        return ""
    return str(s).strip().upper()


def street_tokens(address: str) -> set[str]:
    """
    Extract street-name tokens from address text, dropping numbers and street types.
    Returns a set of lowercase tokens that represent the street name itself.
    """
    if not address:
        return set()
    address_lower = address.lower()
    raw_tokens: list[str] = []
    for chunk in address_lower.replace(",", " ").split():
        raw_tokens.append(chunk)

    DROP_TYPES = {
        "st",
        "street",
        "rd",
        "road",
        "ave",
        "avenue",
        "ln",
        "lane",
        "pl",
        "place",
        "dr",
        "drive",
        "hwy",
        "highway",
        "ct",
        "court",
        "pde",
        "parade",
        "terr",
        "terrace",
        "cres",
        "crescent",
        "blvd",
        "boulevard",
        "way",
        "mews",
        "cir",
        "circuit",
        "circle",
        "esp",
        "esplanade",
        "sq",
        "square",
    }
    DROP_STATES = {"nsw", "vic", "qld", "wa", "sa", "tas", "nt", "act", "australia"}

    keep: set[str] = set()
    for tok in raw_tokens:
        clean = tok.strip(".,;:'\"()-")
        if not clean:
            continue
        if any(c.isdigit() for c in clean):
            continue
        if clean in DROP_TYPES or clean in DROP_STATES:
            continue
        keep.add(clean)
    return keep


def street_numbers(address: str) -> list[int]:
    """
    Extract leading numeric prefixes from an address.
    For '252 George St, Sydney NSW 2000', returns [252].
    For '11/35 Smith St, Sydney NSW 2000', returns [11, 35] (unit number first).
    Four-digit values (postcodes, years) are excluded.
    """
    if not address:
        return []
    tokens = address.replace(",", " ").replace("/", " ").split()
    numbers: list[int] = []
    for tok in tokens:
        clean = "".join(c for c in tok if c.isdigit())
        if clean:
            n = int(clean)
            if n < 1000:
                numbers.append(n)
    return numbers


def address_overlap_passes(seed_addr: str, candidate_addr: str) -> tuple[bool, str]:
    """
    Returns (passes, reason).
    ``passes=False`` means reject the fuzzy match on address grounds.
    """
    seed_t = street_tokens(seed_addr)
    cand_t = street_tokens(candidate_addr)

    if not seed_t or not cand_t:
        return True, "no_address_tokens_either_side"

    common = seed_t & cand_t
    if not common:
        return False, f"no_street_name_overlap (seed={seed_t} cand={cand_t})"

    seed_nums = street_numbers(seed_addr)
    cand_nums = street_numbers(candidate_addr)
    if seed_nums and cand_nums:
        seed_min = min(seed_nums)
        cand_min = min(cand_nums)
        if abs(seed_min - cand_min) > 50:
            return False, f"street_numbers_too_far ({seed_min} vs {cand_min})"

    return True, "address_check_passed"


def find_venue_match(
    seed_venue: dict[str, Any], all_venues: list[dict[str, Any]]
) -> tuple[Optional[str], int, str]:
    """Return ``(venue_id, score, confidence_label)`` with label in ``exact|strong|fuzzy|none``."""
    seed_state = normalise_state(seed_venue.get("state"))
    seed_sub = normalise_suburb(seed_venue.get("suburb"))
    seed_name_n = normalise_venue_name(str(seed_venue.get("name") or ""))
    seed_addr = str(seed_venue.get("address_hint") or "")
    seed_name = str(seed_venue.get("name") or "")

    best: tuple[int, int, dict[str, Any], str] | None = None
    # (tier, score, row, label) — tier order: exact > strong > fuzzy

    for row in all_venues:
        if normalise_state(row.get("state")) != seed_state:
            continue
        cand_name = str(row.get("name") or "")
        score = int(fuzz.token_set_ratio(seed_name_n, normalise_venue_name(cand_name)))
        suburb_ok = bool(seed_sub) and seed_sub == normalise_suburb(row.get("suburb"))
        candidate_suburb = str(row.get("suburb") or "")

        tier = 0
        label = "none"

        if not seed_state:
            pass
        elif score >= 95 and suburb_ok:
            tier, label = 3, "exact"
        elif score >= 90 and suburb_ok:
            tier, label = 2, "strong"
        elif suburb_ok:
            cand_addr = str(row.get("address") or "")
            passes, addr_reason = address_overlap_passes(seed_addr, cand_addr)

            if score >= 80:
                if passes:
                    tier, label = 1, "fuzzy"
                    LOG.info(
                        "ACCEPT fuzzy match: seed='%s' -> candidate='%s', score=%s, address_check=%s",
                        seed_name,
                        cand_name,
                        score,
                        addr_reason,
                    )
                else:
                    LOG.info(
                        "REJECT fuzzy match: seed='%s' @ %s -> candidate='%s' @ %s, score=%s, reason=%s",
                        seed_name,
                        seed_sub or "",
                        cand_name,
                        candidate_suburb,
                        score,
                        addr_reason,
                    )
            elif score >= 69:
                # Dual trading names at one street address: token_set_ratio can sit just
                # under the fuzzy floor while numbers and tokens still align strongly.
                seed_nums = street_numbers(seed_addr)
                cand_nums = street_numbers(cand_addr)
                if (
                    passes
                    and seed_nums
                    and cand_nums
                    and min(seed_nums) == min(cand_nums)
                ):
                    tier, label = 1, "fuzzy"
                    LOG.info(
                        "ACCEPT fuzzy match: seed='%s' -> candidate='%s', score=%s, address_check=%s",
                        seed_name,
                        cand_name,
                        score,
                        f"{addr_reason}; same_primary_street_no_fallback",
                    )

        if tier == 0:
            continue
        cand = (tier, score, row, label)
        if best is None or cand[:2] > best[:2]:
            best = cand

    if best is None:
        return None, 0, "none"
    _tier, score, row, label = best
    return str(row["id"]), score, label


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


def _place_state(formatted_address: str, components: list[dict[str, Any]] | None) -> Optional[str]:
    _su, st, _pc, _conf = parse_au_address(formatted_address)
    if st:
        return st
    for comp in components or []:
        types = comp.get("types") or []
        if "administrative_area_level_1" in types:
            raw = (comp.get("shortText") or comp.get("text") or "").strip()
            return raw.upper() if raw else None
    return None


def lookup_venue_via_places(
    client: httpx.Client,
    api_calls: list[int],
    name: str,
    address_hint: str,
    state: str,
) -> Optional[dict[str, Any]]:
    time.sleep(PLACES_THROTTLE_S)
    key = _api_key()
    query = f"{name} {address_hint}".strip()
    api_calls[0] += 1
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
    body = r.json()
    places = body.get("places") or []
    want = normalise_state(state)
    for place in places:
        fmt = str(place.get("formattedAddress") or "")
        pst = _place_state(fmt, place.get("addressComponents"))
        if pst and normalise_state(pst) == want:
            return place
    return None


def _venue_by_place_id(sb: Any, place_id: str) -> Optional[str]:
    r = sb.table("venues").select("id").eq("place_id", place_id).limit(1).execute()
    rows = r.data or []
    if not rows:
        return None
    return str(rows[0]["id"])


def insert_new_venue(
    sb: Any,
    places_result: dict[str, Any],
    seed_venue: dict[str, Any],
    group_slug: str,
) -> tuple[str, bool]:
    raw_pid = places_result.get("id")
    place_id = _place_id_from_resource(str(raw_pid or ""))
    if not place_id:
        raise ValueError("Places result missing place id")

    existing = _venue_by_place_id(sb, place_id)
    if existing:
        LOG.info(
            "place_id already present in public.venues; reusing existing row (no update): %s",
            existing,
        )
        return existing, False

    fmt = str(places_result.get("formattedAddress") or "")
    parsed_sub, _parsed_st, postcode, parse_conf = parse_au_address(fmt)
    suburb = parsed_sub or seed_venue.get("suburb")
    seed_state = str(seed_venue.get("state") or "")
    loc = places_result.get("location") or {}
    lat = loc.get("latitude")
    lng = loc.get("longitude")
    name = str(seed_venue.get("name") or "")
    slug_tag = group_slug.strip().lower()
    now = datetime.now(timezone.utc).isoformat()
    payload: dict[str, Any] = {
        "name": name,
        "place_id": place_id,
        "address": fmt or None,
        "suburb": suburb,
        "postcode": postcode,
        "state": seed_state or None,
        "lat": lat,
        "lng": lng,
        "google_name": _display_name_text(places_result) or None,
        "google_primary_type": places_result.get("primaryType"),
        "website": places_result.get("websiteUri"),
        "phone": places_result.get("nationalPhoneNumber"),
        "data_source": f"chain_mapper:{slug_tag}",
        "enrichment_status": "chain_seeded",
        "suburb_confidence": parse_conf if parsed_sub else "low",
        "created_at": now,
        "updated_at": now,
    }
    ins = sb.table("venues").insert(payload).execute()
    rows = ins.data or []
    if not rows:
        raise RuntimeError("Insert into public.venues returned no row")
    return str(rows[0]["id"]), True


def upsert_group(sb: Any, seed: dict[str, Any]) -> str:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "group_slug": seed["group_slug"],
        "group_name": seed["group_name"],
        "group_website": seed.get("group_website"),
        "group_weddings_url": seed.get("group_weddings_url"),
        "group_hq_state": seed.get("group_hq_state"),
        "group_hq_suburb": seed.get("group_hq_suburb"),
        "group_notes": seed.get("group_notes"),
        "updated_at": now,
    }
    sb.table("venue_groups").upsert(payload, on_conflict="group_slug").execute()
    sel = (
        sb.table("venue_groups")
        .select("group_id")
        .eq("group_slug", seed["group_slug"])
        .limit(1)
        .execute()
    )
    rows = sel.data or []
    if not rows:
        raise RuntimeError("Upsert venue_groups failed to return group_id")
    return str(rows[0]["group_id"])


def _matchinfo_for_seed_match(score: int, label: str) -> MatchInfo:
    if label == "exact":
        return MatchInfo(
            score=score,
            confidence_label=label,
            db_confidence="confirmed",
            matched_via="exact_name_match",
            notes=f"Matched existing venue (token_set_ratio={score}, tier=exact).",
        )
    if label == "strong":
        return MatchInfo(
            score=score,
            confidence_label=label,
            db_confidence="likely",
            matched_via="fuzzy_name_suburb",
            notes=f"Matched existing venue (token_set_ratio={score}, tier=strong).",
        )
    if label == "fuzzy":
        return MatchInfo(
            score=score,
            confidence_label=label,
            db_confidence="unconfirmed",
            matched_via="fuzzy_name_suburb",
            notes=f"Matched existing venue (token_set_ratio={score}, tier=fuzzy, suburb/state plus address-token checks).",
        )
    raise ValueError(f"Unsupported match label: {label}")


def apply_membership(
    sb: Any,
    venue_id: str,
    group_id: str,
    seed_venue: dict[str, Any],
    match_info: MatchInfo,
) -> None:
    """
    One row per (venue_id, group_id). Additional seed lines that resolve to the same
    physical venue append audit text to ``notes`` (schema unique constraint).
    """
    evidence = (seed_venue.get("evidence_url") or "").strip()
    seed_label = str(seed_venue.get("name") or "").strip()
    sel = (
        sb.table("venue_group_membership")
        .select("membership_id,source_url,notes,match_score,confidence")
        .eq("venue_id", venue_id)
        .eq("group_id", group_id)
        .limit(1)
        .execute()
    )
    rows = sel.data or []
    slot_line = (
        f"Additional function-room seed: {seed_label!s} — {evidence} "
        f"(score={match_info.score}, via={match_info.matched_via})."
    )

    if not rows:
        sb.table("venue_group_membership").insert(
            {
                "venue_id": venue_id,
                "group_id": group_id,
                "source": "chain_website_scrape",
                "source_url": evidence or None,
                "confidence": match_info.db_confidence,
                "matched_via": match_info.matched_via,
                "match_score": match_info.score,
                "notes": match_info.notes,
            }
        ).execute()
        return

    row = rows[0]
    mid = row["membership_id"]
    prev_url = (row.get("source_url") or "").strip()
    prev_notes = row.get("notes") or ""
    if evidence and (evidence == prev_url or evidence in prev_notes):
        return
    merged = f"{prev_notes}\n{slot_line}".strip() if prev_notes else slot_line
    new_score = max(int(row.get("match_score") or 0), match_info.score)
    sb.table("venue_group_membership").update(
        {"notes": merged, "match_score": new_score}
    ).eq("membership_id", mid).execute()


def fetch_all_venues(sb: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    start = 0
    while True:
        end = start + VENUE_PAGE_SIZE - 1
        r = (
            sb.table("venues")
            .select("id,name,suburb,state,postcode,place_id,address")
            .range(start, end)
            .execute()
        )
        chunk = r.data or []
        out.extend(chunk)
        if len(chunk) < VENUE_PAGE_SIZE:
            break
        start += VENUE_PAGE_SIZE
    return out


def _venue_count(sb: Any) -> int:
    r = sb.table("venues").select("id", count="exact").limit(1).execute()
    return int(r.count or 0)


def _null_audit_counts(sb: Any, chain_tag: str) -> tuple[int, int, int]:
    """Return (name_nulls, address_nulls, placeid_nulls_excluding_chain_tag)."""
    r1 = sb.table("venues").select("id", count="exact").is_("name", "null").limit(1).execute()
    r2 = sb.table("venues").select("id", count="exact").is_("address", "null").limit(1).execute()
    q = (
        sb.table("venues")
        .select("id", count="exact")
        .is_("place_id", "null")
        .neq("data_source", chain_tag)
        .limit(1)
        .execute()
    )
    return int(r1.count or 0), int(r2.count or 0), int(q.count or 0)


def run_chain_mapper_dry_run_tests() -> None:
    """Exercise ``find_venue_match`` guards without touching Supabase (see §4)."""
    print("--- Chain mapper fuzzy match dry-run tests ---")

    addr_lorne_shared = "176 Mountjoy Parade, Lorne VIC 3232"

    test1_seed: dict[str, Any] = {
        "name": "House of Merivale",
        "suburb": "Sydney",
        "state": "NSW",
        "address_hint": "Sydney CBD",
    }
    test1_candidates: list[dict[str, Any]] = [
        {
            "id": "VENUE-DRYRUN-1",
            "name": "Somerley House",
            "suburb": "Sutton Forest",
            "state": "NSW",
            "address": "1 Old Surrey Rd, Sutton Forest NSW 2577",
            "postcode": None,
            "place_id": "",
        },
    ]
    vid1, sc1, lb1 = find_venue_match(test1_seed, test1_candidates)
    if vid1 is None:
        why1 = "no fuzzy/exact tier (suburbs differ - Sydney vs Sutton Forest)."
    else:
        why1 = f"unexpected venue_id={vid1} score={sc1} label={lb1}"
    verdict1 = "REJECTED" if vid1 is None else "ACCEPTED (unexpected)"
    print(f"Test 1 (BUG 1): {verdict1} - {why1}")

    test2_seed: dict[str, Any] = {
        "name": "ivy Ballroom",
        "suburb": "Sydney",
        "state": "NSW",
        "address_hint": "330 George St, Sydney NSW 2000",
    }
    test2_candidates: list[dict[str, Any]] = [
        {
            "id": "VENUE-DRYRUN-2",
            "name": "Establishment Ballroom",
            "suburb": "Sydney",
            "state": "NSW",
            "address": "252 George St, Sydney NSW 2000",
            "postcode": None,
            "place_id": "",
        },
    ]
    vid2, sc2, lb2 = find_venue_match(test2_seed, test2_candidates)
    if vid2 is None:
        why2 = "no fuzzy match after address gate - street_numbers_too_far (330 vs 252)."
    else:
        why2 = f"unexpected venue_id={vid2} score={sc2} label={lb2}"
    verdict2 = "REJECTED" if vid2 is None else "ACCEPTED (unexpected)"
    print(f"Test 2 (BUG 2): {verdict2} - {why2}")

    test3_seed: dict[str, Any] = {
        "name": "Totti's Lorne",
        "suburb": "Lorne",
        "state": "VIC",
        "address_hint": addr_lorne_shared,
    }
    test3_candidates: list[dict[str, Any]] = [
        {
            "id": "VENUE-DRYRUN-3",
            "name": "Lorne Hotel",
            "suburb": "Lorne",
            "state": "VIC",
            "address": addr_lorne_shared,
            "postcode": None,
            "place_id": "",
        },
    ]
    vid3, sc3, lb3 = find_venue_match(test3_seed, test3_candidates)
    if vid3 is not None and lb3 == "fuzzy":
        why3 = f"matched duplicate-address trading names via fuzzy tier (score={sc3}), address tokens align."
    elif vid3 is not None:
        why3 = f"matched tier={lb3} score={sc3} (expected fuzzy)."
    else:
        why3 = f"no match (score_floor). last_score={sc3}"
    verdict3 = (
        "ACCEPTED"
        if vid3 is not None and lb3 == "fuzzy"
        else ("PARTIAL" if vid3 else "REJECTED")
    )
    print(f"Test 3 (regression): {verdict3} - {why3}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Map chain seed venues to venue groups.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run fuzzy matcher regression checks only (no database or Places calls)",
    )
    parser.add_argument(
        "seed_path",
        nargs="?",
        default=str(_ROOT / "data" / "chain_seeds" / "merivale.json"),
        help="Path to chain JSON seed file",
    )
    args = parser.parse_args()
    load_env()

    if args.dry_run:
        run_chain_mapper_dry_run_tests()
        return

    seed_path = Path(args.seed_path).expanduser()
    if not seed_path.is_file():
        raise SystemExit(f"Seed file not found: {seed_path}")
    started = time.monotonic()
    api_calls = [0]

    seed = load_seed(seed_path)
    slug = str(seed["group_slug"])
    chain_data_source = f"chain_mapper:{slug}"

    sb = _supabase_client()
    check_preflight(sb)

    total_before = _venue_count(sb)
    na1, na2, na3 = _null_audit_counts(sb, chain_data_source)
    LOG.info("Baseline: total_venues=%s name_nulls=%s address_nulls=%s placeid_nulls_excl_chain=%s", total_before, na1, na2, na3)

    group_id = upsert_group(sb, seed)
    LOG.info("Upserted venue_groups row group_id=%s slug=%s", group_id, slug)

    all_venues = fetch_all_venues(sb)
    LOG.info("Loaded %s venue rows for matching", len(all_venues))

    matched_n = added_n = skipped_n = 0
    per_venue_log: list[str] = []

    with httpx.Client() as http:
        for seed_venue in seed["venues"]:
            vname = str(seed_venue.get("name") or "")
            vid_m, score_m, label_m = find_venue_match(seed_venue, all_venues)

            if vid_m:
                matched_n += 1
                mi = _matchinfo_for_seed_match(score_m, label_m)
                apply_membership(sb, vid_m, group_id, seed_venue, mi)
                LOG.info(
                    "AUDIT matched seed=%r decision=matched venue_id=%s score=%s label=%s via=%s",
                    vname,
                    vid_m,
                    score_m,
                    label_m,
                    mi.matched_via,
                )
                per_venue_log.append(f"matched\t{vname}\tvenue_id={vid_m}\tscore={score_m}\t{label_m}")
                continue

            pr = lookup_venue_via_places(
                http,
                api_calls,
                vname,
                str(seed_venue.get("address_hint") or ""),
                str(seed_venue.get("state") or ""),
            )
            if not pr:
                skipped_n += 1
                LOG.warning(
                    "AUDIT seed=%r decision=could_not_resolve (no fuzzy match, Places empty or wrong state)",
                    vname,
                )
                per_venue_log.append(f"skipped\t{vname}\tcould_not_resolve")
                continue

            vid_new, inserted_fresh = insert_new_venue(sb, pr, seed_venue, slug)
            pid_s = _place_id_from_resource(str(pr.get("id") or ""))
            if inserted_fresh:
                added_n += 1
                mi_add = MatchInfo(
                    score=0,
                    confidence_label="new",
                    db_confidence="confirmed",
                    matched_via="newly_added",
                    notes="Inserted new venue from Google Places (existing rows are never modified).",
                )
                LOG.info(
                    "AUDIT seed=%r decision=added venue_id=%s place_id=%s",
                    vname,
                    vid_new,
                    pid_s,
                )
                per_venue_log.append(f"added\t{vname}\tvenue_id={vid_new}\tplace_id={pid_s}")
                all_venues.append(
                    {
                        "id": vid_new,
                        "name": vname,
                        "suburb": normalise_suburb(seed_venue.get("suburb")) or None,
                        "state": seed_venue.get("state"),
                        "postcode": None,
                        "place_id": pid_s,
                        "address": fmt or None,
                    }
                )
            else:
                matched_n += 1
                mi_add = MatchInfo(
                    score=100,
                    confidence_label="places_dedupe",
                    db_confidence="confirmed",
                    matched_via="manual",
                    notes="Google Places place_id already mapped to an existing venue row; insert skipped (augmentation-only).",
                )
                LOG.info(
                    "AUDIT seed=%r decision=matched_existing_place_id venue_id=%s place_id=%s",
                    vname,
                    vid_new,
                    pid_s,
                )
                per_venue_log.append(
                    f"matched_place_id\t{vname}\tvenue_id={vid_new}\tplace_id={pid_s}"
                )
            apply_membership(sb, vid_new, group_id, seed_venue, mi_add)

    elapsed = time.monotonic() - started
    est_cost = api_calls[0] * 0.017
    LOG.info(
        "Summary: matched=%s added=%s skipped=%s places_calls=%s est_cost_usd=%.4f runtime_s=%.2f",
        matched_n,
        added_n,
        skipped_n,
        api_calls[0],
        est_cost,
        elapsed,
    )
    for line in per_venue_log:
        LOG.info("per_venue\t%s", line)

    total_after = _venue_count(sb)
    nb1, nb2, nb3 = _null_audit_counts(sb, chain_data_source)
    LOG.info(
        "Post-run: total_venues=%s name_nulls=%s address_nulls=%s placeid_nulls_excl_chain=%s",
        total_after,
        nb1,
        nb2,
        nb3,
    )

    if nb1 != na1 or nb2 != na2 or nb3 != na3:
        raise RuntimeError(
            "Augmentation invariant failed: venue null-count audit changed unexpectedly "
            f"(before name/address/place {na1}/{na2}/{na3}, after {nb1}/{nb2}/{nb3}). "
            "Stop and inspect — existing venue rows must not be modified."
        )

    if skipped_n > 3:
        raise RuntimeError(
            f"Too many venues could not be resolved ({skipped_n} > 3). "
            "Stop for manual review before shipping."
        )

    if total_after - total_before != added_n:
        raise RuntimeError(
            f"Venue count delta mismatch: expected +{added_n} new rows, "
            f"got {total_after - total_before}. Investigate before shipping."
        )


if __name__ == "__main__":
    main()
