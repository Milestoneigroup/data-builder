"""Load Busy Index v1 seed JSON into Supabase (migration ``014_busy_index_calendar.sql``).

Upserts ``shared.ref_school_holidays``, ``shared.ref_public_holidays``, and ``shared.ref_major_events``.
Skips any row whose primary window is entirely before **today** (defence in depth).

Run: ``python -m scrapers.load_busy_index_seeds``

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY`` with service privileges).
"""

from __future__ import annotations

import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

SCHOOL_PATH = _ROOT / "data" / "seed_school_holidays_AU.json"
PUBLIC_PATH = _ROOT / "data" / "seed_public_holidays_AU.json"
EVENTS_PATH = _ROOT / "data" / "seed_major_events_AU.json"


def _today() -> date:
    return date.today()


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _long_weekend(observed: date) -> bool:
    wd = observed.weekday()  # Mon=0
    return wd in (0, 4)


def _load_json(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(str(path))
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array")
    return data


def _filter_school(rows: list[dict[str, Any]], today: date) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        sd = _parse_date(str(r["start_date"]))
        if sd < today:
            LOG.info("skip school row (start before today): %s %s", r.get("state_code"), r.get("term_or_break_label"))
            continue
        out.append(r)
    return out


def _filter_public(rows: list[dict[str, Any]], today: date) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        od = _parse_date(str(r["observed_date"]))
        if od < today:
            LOG.info("skip public holiday (before today): %s %s", r.get("state_code"), r.get("holiday_name"))
            continue
        out.append(r)
    return out


def _filter_events(rows: list[dict[str, Any]], today: date) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        sd = _parse_date(str(r["start_date"]))
        if sd < today:
            LOG.info("skip major event (start before today): %s", r.get("event_slug"))
            continue
        out.append(r)
    return out


def _school_rows_for_upsert(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "state_code": r["state_code"],
            "year": r["year"],
            "term_or_break_label": r["term_or_break_label"],
            "start_date": r["start_date"],
            "end_date": r["end_date"],
            "data_source": r["data_source"],
            "source_url": r.get("source_url"),
            "verified_at": r["verified_at"],
        }
        for r in rows
    ]


def _public_rows_for_upsert(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    upserted: list[dict[str, Any]] = []
    for r in rows:
        od = _parse_date(str(r["observed_date"]))
        upserted.append(
            {
                "state_code": r["state_code"],
                "year": r["year"],
                "holiday_name": r["holiday_name"],
                "observed_date": r["observed_date"],
                "is_national": r.get("is_national", False),
                "creates_long_weekend": _long_weekend(od),
                "data_source": r["data_source"],
                "source_url": r.get("source_url"),
                "verified_at": r["verified_at"],
            }
        )
    return upserted


def _event_rows_for_upsert(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "event_slug": r["event_slug"],
            "event_name": r["event_name"],
            "event_type": r["event_type"],
            "state_code": r["state_code"],
            "lga_or_area": r.get("lga_or_area"),
            "event_lat": r.get("event_lat"),
            "event_lng": r.get("event_lng"),
            "start_date": r["start_date"],
            "end_date": r["end_date"],
            "is_recurring_annual": r.get("is_recurring_annual", True),
            "expected_visitors_label": r.get("expected_visitors_label"),
            "notes": r.get("notes"),
            "data_source": r["data_source"],
            "source_url": r.get("source_url"),
            "verified_at": r["verified_at"],
            "is_active": r.get("is_active", True),
        }
        for r in rows
    ]


def _validate_school_coverage(rows: list[dict[str, Any]]) -> None:
    """Require at least one forward row per state for years 2026 and 2027 (when present in seed)."""
    by_state_year: dict[str, set[int]] = defaultdict(set)
    for r in rows:
        by_state_year[str(r["state_code"])].add(int(r["year"]))
    states = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}
    for st in states:
        ys = by_state_year.get(st, set())
        if 2026 not in ys or 2027 not in ys:
            raise SystemExit(
                f"Validation failed: state {st} must have school holiday rows for both 2026 and 2027 "
                f"(forward seed window); got years {sorted(ys)}"
            )


def _log_school_breakdown(rows: list[dict[str, Any]]) -> None:
    c_state = Counter(str(r["state_code"]) for r in rows)
    LOG.info("School holidays loaded: %s rows", len(rows))
    for k in sorted(c_state):
        LOG.info("  %s: %s", k, c_state[k])


def _log_public_breakdown(rows: list[dict[str, Any]]) -> None:
    c_state = Counter(str(r["state_code"]) for r in rows)
    LOG.info("Public holidays loaded: %s rows", len(rows))
    for k in sorted(c_state):
        LOG.info("  %s: %s", k, c_state[k])


def main() -> None:
    import os

    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) required.")

    today = _today()
    school_raw = _load_json(SCHOOL_PATH)
    public_raw = _load_json(PUBLIC_PATH)
    events_raw = _load_json(EVENTS_PATH)

    _validate_school_coverage(school_raw)

    school = _filter_school(school_raw, today)
    public = _filter_public(public_raw, today)
    events = _filter_events(events_raw, today)

    from supabase import create_client

    sb = create_client(url, key)
    sh = sb.schema("shared")

    sch_payload = _school_rows_for_upsert(school)
    if sch_payload:
        sh.table("ref_school_holidays").upsert(sch_payload, on_conflict="state_code,year,term_or_break_label").execute()
    _log_school_breakdown(sch_payload)

    pub_payload = _public_rows_for_upsert(public)
    if pub_payload:
        sh.table("ref_public_holidays").upsert(pub_payload, on_conflict="state_code,year,holiday_name").execute()
    _log_public_breakdown(pub_payload)

    ev_payload = _event_rows_for_upsert(events)
    if ev_payload:
        sh.table("ref_major_events").upsert(ev_payload, on_conflict="event_slug").execute()
    LOG.info("Major events loaded: %s rows", len(ev_payload))

    LOG.info("Done at %s (cutoff today=%s)", datetime.now(timezone.utc).isoformat(), today.isoformat())


if __name__ == "__main__":
    main()
