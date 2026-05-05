"""ABR name search (Advanced Simple Protocol 2017) — Method B for venue ABN enrichment.

Writes ``abn_from_name_search``, ``abn_name_search_confidence``, ``abn_name_search_result_count``,
and ``abn_name_search_attempted_at`` only. Does **not** set verified ``abn``.

ABR endpoint: ``POST .../AbrXmlSearch.asmx/ABRSearchByNameAdvancedSimpleProtocol2017``
(documented on https://abr.business.gov.au/abrxmlsearch/Forms/ABRSearchByNameAdvancedSimpleProtocol2017.aspx).

Environment:

- ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY`` with service role)
- ``ABN_GUID`` — web services authentication GUID (do not log this value or raw ABR XML responses at INFO).
- ``TEST_VENUE_IDS`` — optional comma-separated UUIDs; if set, only those rows are processed.
  Otherwise rows with ``abn_name_search_attempted_at`` IS NULL.

Politeness: ~1.5s between ABR calls; HTTP timeout 30s; User-Agent identifies MIG.

Run: ``python -m scrapers.abr_name_search_for_venues``
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from thefuzz import fuzz

from scrapers.abn_util import abn_checksum_valid, normalize_abn_digits

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

LOG = logging.getLogger(__name__)

USER_AGENT = "MIG-Data-Builder/1.0 (data@milestoneigroup.com; venue ABN enrichment)"
ABR_URL = (
    "https://abr.business.gov.au/abrxmlsearch/AbrXmlSearch.asmx/"
    "ABRSearchByNameAdvancedSimpleProtocol2017"
)
ABR_DELAY_S = 1.5
HTTP_TIMEOUT_S = 30.0

NS = "http://abr.business.gov.au/ABRXMLSearch/"


def _q(tag: str) -> str:
    return f"{{{NS}}}{tag}"


_STATE_ALIASES: dict[str, str] = {
    "australian capital territory": "ACT",
    "new south wales": "NSW",
    "victoria": "VIC",
    "queensland": "QLD",
    "south australia": "SA",
    "western australia": "WA",
    "tasmania": "TAS",
    "northern territory": "NT",
}


def normalize_state_code(raw: str | None) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    up = s.upper()
    if len(up) <= 3 and up.isalpha():
        return up
    low = s.strip().lower()
    return _STATE_ALIASES.get(low, "")


def _norm_venue_name(s: str) -> str:
    t = s.lower()
    t = re.sub(r"[^a-z0-9\s]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _postcode_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, int):
        return f"{v:04d}" if 0 < v < 10000 else str(v)
    s = str(v).strip()
    if not s or s.lower() == "none":
        return ""
    m = re.sub(r"\D", "", s)
    if not m:
        return ""
    if len(m) >= 4:
        return m[:4]
    return m.zfill(4)


@dataclass(frozen=True)
class _Candidate:
    abn: str
    organisation_name: str
    api_score: int
    rec_state: str
    rec_postcode: str


@dataclass(frozen=True)
class _ScoredAbn:
    abn: str
    blended: float
    api_score: int
    fuzzy: int
    organisation_name: str
    rec_state: str
    rec_postcode: str


def _state_flags_for_venue(state_code: str) -> dict[str, str]:
    codes = ("NSW", "SA", "ACT", "VIC", "WA", "NT", "QLD", "TAS")
    st = state_code.strip().upper() if state_code else ""
    return {c: "Y" if c == st else "N" for c in codes}


def _entity_scores_from_record(rec: ET.Element) -> tuple[str, int]:
    best_org = ""
    best_api = 0
    for tag in ("mainName", "businessName", "legalName", "otherName"):
        block = rec.find(_q(tag))
        if block is None:
            continue
        on = block.find(_q("organisationName"))
        sc = block.find(_q("score"))
        if on is None or not (on.text or "").strip():
            continue
        org = on.text.strip().strip('"').replace('""', '"')
        api = int(sc.text) if sc is not None and (sc.text or "").isdigit() else 0
        if api >= best_api:
            best_org = org
            best_api = api
    return best_org, best_api


def _parse_abr_response(xml_text: str) -> tuple[int, list[_Candidate], str | None]:
    """Returns (raw ``numberOfRecords`` from ABR, checksum-valid deduped candidates, exception)."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        return 0, [], f"xml_parse_error:{e}"

    resp = root.find(_q("response"))
    if resp is None:
        return 0, [], "no_response"

    exc = resp.find(_q("exception"))
    if exc is not None:
        desc = exc.find(_q("exceptionDescription"))
        msg = (desc.text or "abr_exception").strip() if desc is not None else "abr_exception"
        if msg.lower().startswith("no records found") or "no records found" in msg.lower():
            return 0, [], None
        return 0, [], msg

    srl = resp.find(_q("searchResultsList"))
    if srl is None:
        return 0, [], None

    nr_el = srl.find(_q("numberOfRecords"))
    n_raw = int(nr_el.text) if nr_el is not None and (nr_el.text or "").strip().isdigit() else 0

    by_abn: dict[str, _Candidate] = {}
    for rec in srl.findall(_q("searchResultsRecord")):
        abn_wr = rec.find(_q("ABN"))
        if abn_wr is None:
            continue
        idv_el = abn_wr.find(_q("identifierValue"))
        if idv_el is None or not (idv_el.text or "").strip():
            continue
        abn = normalize_abn_digits(idv_el.text.strip())
        if not abn or not abn_checksum_valid(abn):
            continue

        org, api = _entity_scores_from_record(rec)
        addr = rec.find(_q("mainBusinessPhysicalAddress"))
        rec_state = ""
        rec_pc = ""
        if addr is not None:
            st_el = addr.find(_q("stateCode"))
            pc_el = addr.find(_q("postcode"))
            if st_el is not None and st_el.text:
                rec_state = st_el.text.strip().upper()
            if pc_el is not None and pc_el.text:
                rec_pc = _postcode_str(pc_el.text)

        cand = _Candidate(
            abn=abn,
            organisation_name=org,
            api_score=api,
            rec_state=rec_state,
            rec_postcode=rec_pc,
        )
        prev = by_abn.get(abn)
        if prev is None or api > prev.api_score:
            by_abn[abn] = cand

    return n_raw, list(by_abn.values()), None


def _rank_candidates(
    venue_name: str,
    venue_pc: str,
    venue_state: str,
    candidates: list[_Candidate],
) -> list[_ScoredAbn]:
    v_norm = _norm_venue_name(venue_name)
    out: list[_ScoredAbn] = []
    for c in candidates:
        o_norm = _norm_venue_name(c.organisation_name or "")
        fz = fuzz.token_set_ratio(v_norm, o_norm) if o_norm else 0
        post_bonus = (
            12.0 if venue_pc and c.rec_postcode and venue_pc == c.rec_postcode else 0.0
        )
        state_bonus = (
            8.0 if venue_state and c.rec_state and venue_state == c.rec_state else 0.0
        )
        blended = 0.52 * fz + 0.28 * float(c.api_score) + post_bonus + state_bonus
        out.append(
            _ScoredAbn(
                abn=c.abn,
                blended=blended,
                api_score=c.api_score,
                fuzzy=fz,
                organisation_name=c.organisation_name,
                rec_state=c.rec_state,
                rec_postcode=c.rec_postcode,
            )
        )
    out.sort(key=lambda x: x.blended, reverse=True)
    return out


def _pick_abn_and_confidence(
    venue_pc: str,
    venue_state: str,
    ranked: list[_ScoredAbn],
) -> tuple[str | None, str]:
    """Return chosen ABN and ``venues_abn_name_search_confidence_check`` enum value."""

    if not ranked:
        return None, "no_match"

    top = ranked[0]
    gap = top.blended - ranked[1].blended if len(ranked) > 1 else 999.0

    if len(ranked) >= 3 and gap < 10.0:
        tight = sum(1 for r in ranked[:3] if top.blended - r.blended < 10.0)
        if tight >= 3:
            return None, "multiple_candidates"

    strong_geo = (
        bool(venue_pc)
        and bool(venue_state)
        and venue_pc == top.rec_postcode
        and venue_state == top.rec_state
        and top.api_score >= 83
    )

    if gap < 8.0 and len(ranked) > 1:
        if top.fuzzy >= 80:
            return top.abn, "exact_name_match"
        if strong_geo:
            return top.abn, "strong_state_match"
        return top.abn, "fuzzy"

    if top.fuzzy >= 82 or top.api_score >= 96:
        return top.abn, "exact_name_match"

    if strong_geo and top.fuzzy >= 62:
        return top.abn, "strong_state_match"

    if top.fuzzy >= 52:
        return top.abn, "fuzzy"

    if len(ranked) >= 2 and ranked[1].fuzzy >= 50 and gap < 15.0:
        return None, "multiple_candidates"

    return top.abn, "fuzzy"


def _abr_post_body(
    name: str,
    postcode: str,
    auth_guid: str,
    state_code: str,
) -> dict[str, str]:
    flags = _state_flags_for_venue(state_code)
    return {
        "name": name.strip()[:200],
        "postcode": postcode.strip()[:12],
        "legalName": "Y",
        "tradingName": "Y",
        "businessName": "Y",
        "activeABNsOnly": "Y",
        "NSW": flags["NSW"],
        "SA": flags["SA"],
        "ACT": flags["ACT"],
        "VIC": flags["VIC"],
        "WA": flags["WA"],
        "NT": flags["NT"],
        "QLD": flags["QLD"],
        "TAS": flags["TAS"],
        "authenticationGuid": auth_guid,
        "searchWidth": "Typical",
        "minimumScore": "58",
        "maxSearchResults": "25",
    }


def _venue_rows(supabase: Any, test_ids: list[str] | None) -> list[dict[str, Any]]:
    cols = "id, name, postcode, state"
    if test_ids:
        resp = supabase.table("venues").select(cols).in_("id", test_ids).execute()
        rows = list(getattr(resp, "data", None) or [])
        order = {v: i for i, v in enumerate(test_ids)}
        rows.sort(key=lambda r: order.get(str(r.get("id")), 9999))
        return rows

    batch: list[dict[str, Any]] = []
    offset = 0
    page = 500
    while True:
        resp = (
            supabase.table("venues")
            .select(cols)
            .is_("abn_name_search_attempted_at", "null")
            .range(offset, offset + page - 1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if not rows:
            break
        batch.extend(rows)
        if len(rows) < page:
            break
        offset += page
    return batch


def _today_utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def run() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from data_builder.config import get_settings

    settings = get_settings()
    sb_url = (settings.supabase_url or os.getenv("SUPABASE_URL") or "").strip()
    sb_key = (
        settings.supabase_service_role_key
        or settings.supabase_key
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_KEY")
        or ""
    ).strip()
    auth_guid = (os.getenv("ABN_GUID") or "").strip()
    if not sb_url or not sb_key:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")
        return 1
    if not auth_guid:
        LOG.error("ABN_GUID is required for ABR name search.")
        return 1

    raw_test = os.getenv("TEST_VENUE_IDS", "").strip()
    test_ids = [x.strip() for x in raw_test.split(",") if x.strip()] if raw_test else None

    from supabase import create_client

    supabase = create_client(sb_url, sb_key)
    rows = _venue_rows(supabase, test_ids)
    LOG.info("Venues to process (Method B): %s", len(rows))

    stats: dict[str, int] = {
        "exact_name_match": 0,
        "strong_state_match": 0,
        "fuzzy": 0,
        "multiple_candidates": 0,
        "no_match": 0,
        "api_error": 0,
    }

    with httpx.Client(headers={"User-Agent": USER_AGENT}) as client:
        for i, row in enumerate(rows):
            if i > 0:
                time.sleep(ABR_DELAY_S)
            vid = str(row.get("id", ""))
            vname = str(row.get("name", "") or "")
            st = normalize_state_code(row.get("state"))
            pc = _postcode_str(row.get("postcode"))

            if not st:
                LOG.warning("venue=%s missing_normalised_state name=%s", vid, vname[:80])
            body = _abr_post_body(vname, pc, auth_guid, st)
            try:
                r = client.post(ABR_URL, data=body, timeout=HTTP_TIMEOUT_S)
            except Exception as e:
                LOG.info("venue=%s abr_transport_error name=%s err=%s", vid, vname[:80], e)
                stats["api_error"] += 1
                supabase.table("venues").update(
                    {
                        "abn_name_search_attempted_at": _today_utc_date(),
                        "abn_name_search_confidence": "api_error",
                        "abn_name_search_result_count": 0,
                        "abn_from_name_search": None,
                    }
                ).eq("id", vid).execute()
                continue

            if r.status_code >= 400:
                LOG.info("venue=%s abr_http_%s", vid, r.status_code)
                stats["api_error"] += 1
                supabase.table("venues").update(
                    {
                        "abn_name_search_attempted_at": _today_utc_date(),
                        "abn_name_search_confidence": "api_error",
                        "abn_name_search_result_count": 0,
                        "abn_from_name_search": None,
                    }
                ).eq("id", vid).execute()
                continue

            n_raw, candidates, err = _parse_abr_response(r.text)
            update: dict[str, Any] = {
                "abn_name_search_attempted_at": _today_utc_date(),
                "abn_name_search_result_count": n_raw,
                "abn_from_name_search": None,
            }

            if err:
                LOG.info("venue=%s abr_error name=%s msg=%s", vid, vname[:80], err[:120])
                stats["api_error"] += 1
                update["abn_name_search_confidence"] = "api_error"
                supabase.table("venues").update(update).eq("id", vid).execute()
                continue

            ranked = _rank_candidates(vname, pc, st, candidates)
            chosen, conf = _pick_abn_and_confidence(pc, st, ranked)
            if chosen:
                update["abn_from_name_search"] = chosen
            update["abn_name_search_confidence"] = conf
            stats[conf] = stats.get(conf, 0) + 1

            if chosen:
                LOG.info(
                    "venue=%s abn_ok conf=%s n_raw=%s name=%s",
                    vid,
                    conf,
                    n_raw,
                    vname[:80],
                )
            else:
                LOG.info(
                    "venue=%s abn_none conf=%s n_raw=%s name=%s",
                    vid,
                    conf,
                    n_raw,
                    vname[:80],
                )

            supabase.table("venues").update(update).eq("id", vid).execute()

    LOG.info("Method B summary: %s", stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
