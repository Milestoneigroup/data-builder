"""Re-score stored ``abn_phase2_candidates`` and auto-pick a winner when clearly ahead.

Skips venues that already have ``abn_phase2_value`` or ``abn_phase3_attempted_at``.
Batches of 50 rows with ``COMMIT`` after each batch.

Run: ``python scrapers/score_phase2_candidates.py``

Requires: ``DATABASE_URL`` in ``env.local`` (``load_dotenv(..., override=True)``).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from rapidfuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger("score_phase2_candidates")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BATCH = 50
WALL_CLOCK_BUDGET_S = 30 * 60
MAX_CONSECUTIVE_FAILURES = 5

COMPANY_ENTITY_CODES = frozenset({"PRV", "PUB"})

FETCH_SQL = """
SELECT id, name, suburb, state, postcode, abn_phase2_candidates
FROM public.venues
WHERE abn_phase2_value IS NULL
  AND abn_phase2_candidates IS NOT NULL
  AND jsonb_typeof(abn_phase2_candidates) = 'array'
  AND jsonb_array_length(abn_phase2_candidates) > 0
  AND abn_phase3_attempted_at IS NULL
ORDER BY id
LIMIT %s
"""

UPDATE_WIN_SQL = """
UPDATE public.venues
SET abn_phase2_value = %(abn)s,
    abn_phase2_entity_legal_name = %(ename)s,
    abn_phase2_entity_type = %(etype)s,
    abn_phase2_match_confidence = 'fuzzy',
    abn_phase2_query_strategy = 'manual',
    abn_phase2_query_used = 'phase3_stored_candidate_score',
    abn_phase3_method = 'candidate_score_winner',
    abn_phase3_score = %(score)s,
    abn_phase3_attempted_at = now(),
    abn_phase3_notes = %(notes)s
WHERE id = %(id)s
  AND abn_phase2_value IS NULL
"""

UPDATE_NO_WIN_SQL = """
UPDATE public.venues
SET abn_phase3_method = 'candidate_score_no_winner',
    abn_phase3_score = %(score)s,
    abn_phase3_attempted_at = now(),
    abn_phase3_notes = %(notes)s
WHERE id = %(id)s
  AND abn_phase2_value IS NULL
"""


def is_likely_company(venue_name: str) -> bool:
    """Returns True if the venue name suggests an incorporated business."""
    company_signals = (
        "pty ltd",
        "pty",
        "limited",
        "ltd",
        "group",
        "corporation",
        "corp",
        "company",
        "co.",
        "incorporated",
        "inc",
        "hotel",
        "restaurant",
        "bar",
        "club",
        "lounge",
        "tavern",
        "reception",
        "function",
        "venue",
        "centre",
        "cafe",
        "estate",
    )
    name_lower = venue_name.lower() if venue_name else ""
    return any(signal in name_lower for signal in company_signals)


def _norm_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _candidate_abn(c: dict[str, Any]) -> str:
    return normalise_digits(_norm_str(c.get("Abn") or c.get("abn")))


def _candidate_entity_name(c: dict[str, Any]) -> str:
    return _norm_str(
        c.get("EntityName") or c.get("name") or c.get("Name")
    )


def _candidate_state(c: dict[str, Any]) -> str:
    return _norm_str(c.get("State") or c.get("state")).upper()


def _candidate_postcode(c: dict[str, Any]) -> str:
    return _norm_str(c.get("Postcode") or c.get("postcode"))


def _candidate_entity_code(c: dict[str, Any]) -> str:
    return _norm_str(
        c.get("EntityTypeCode") or c.get("entity_type_code")
    ).upper()


def _candidate_entity_type_name(c: dict[str, Any]) -> str | None:
    v = _norm_str(c.get("EntityTypeName") or c.get("entity_type_name"))
    return v or None


def _is_current(c: dict[str, Any]) -> bool:
    ic = c.get("IsCurrentIndicator")
    if ic in ("Y", "y", "true", True):
        return True
    if c.get("is_current") is True:
        return True
    if c.get("IsCurrent") is True:
        return True
    st = _norm_str(c.get("abn_status") or c.get("AbnStatus"))
    if st == "0000000001":
        return True
    return False


def normalise_digits(s: str) -> str:
    return "".join(ch for ch in s if ch.isdigit())


def postcode_match(vp: str, cp: str) -> bool:
    if not vp or not cp:
        return False
    vn = normalise_digits(vp)
    cn = normalise_digits(cp)
    if vn == cn:
        return True
    if len(vn) >= 4 and len(cn) >= 4 and vn[:4] == cn[:4]:
        return True
    return False


def suburb_hits(venue_suburb: str, entity_name: str) -> bool:
    if not venue_suburb or not entity_name:
        return False
    n_low = entity_name.lower()
    for tok in venue_suburb.replace(",", " ").split():
        t = tok.strip().lower()
        if len(t) < 3:
            continue
        if t in n_low:
            return True
    return False


def score_candidate(
    venue_name: str,
    venue_suburb: str | None,
    venue_state: str | None,
    venue_postcode: str | None,
    c: dict[str, Any],
) -> int:
    total = 0
    st = _candidate_state(c)
    if venue_state and st == venue_state.strip().upper():
        total += 30
    pc = _candidate_postcode(c)
    if postcode_match(_norm_str(venue_postcode), pc):
        total += 20
    en = _candidate_entity_name(c)
    if suburb_hits(_norm_str(venue_suburb), en):
        total += 15

    code = _candidate_entity_code(c)
    likely_co = is_likely_company(venue_name or "")
    if code in COMPANY_ENTITY_CODES and likely_co:
        total += 10
    elif code == "IND" and likely_co:
        total -= 15

    if _is_current(c):
        total += 10

    ratio = fuzz.partial_ratio((venue_name or "").lower(), en.lower())
    total += int(round((ratio / 100.0) * 20))
    return total


def main() -> None:
    db = os.getenv("DATABASE_URL", "").strip()
    if not db:
        raise SystemExit("DATABASE_URL is required in env.local")

    started = time.monotonic()
    stats = {
        "processed": 0,
        "winners": 0,
        "no_winner": 0,
        "errors": 0,
    }
    consec_fail = 0
    logged_candidate_keys = False

    while True:
        if time.monotonic() - started > WALL_CLOCK_BUDGET_S:
            LOG.error("Wall-clock budget (30 min) exceeded; stopping.")
            break

        with psycopg.connect(db, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(FETCH_SQL, (BATCH,))
                rows = cur.fetchall()
            if not rows:
                conn.commit()
                break

            with conn.cursor() as ucur:
                for row in rows:
                    vid, name, suburb, state, postcode, raw_cands = row
                    try:
                        if isinstance(raw_cands, str):
                            candidates: list[Any] = json.loads(raw_cands)
                        else:
                            candidates = list(raw_cands)

                        if (
                            (not logged_candidate_keys)
                            and candidates
                            and isinstance(candidates[0], dict)
                        ):
                            LOG.info(
                                "Candidate keys (diagnostic): %s",
                                list(candidates[0].keys()),
                            )
                            logged_candidate_keys = True

                        scored: list[tuple[int, dict[str, Any]]] = []
                        for c in candidates:
                            if not isinstance(c, dict):
                                continue
                            try:
                                sc = score_candidate(
                                    _norm_str(name),
                                    suburb,
                                    state,
                                    postcode,
                                    c,
                                )
                                scored.append((sc, c))
                            except Exception as ex:
                                LOG.warning(
                                    "Scoring skip venue=%s: %s", vid, ex
                                )
                                continue

                        if not scored:
                            ucur.execute(
                                UPDATE_NO_WIN_SQL,
                                {
                                    "id": vid,
                                    "score": None,
                                    "notes": "No scorable candidates (parse error or empty)",
                                },
                            )
                            stats["no_winner"] += 1
                            continue

                        scored.sort(key=lambda x: -x[0])
                        max_score = scored[0][0]
                        second_score = scored[1][0] if len(scored) > 1 else -1
                        winner = scored[0][1]

                        clear = max_score >= 70 and (
                            len(scored) == 1 or (max_score - second_score >= 15)
                        )

                        w_abn = _candidate_abn(winner)
                        w_name = _candidate_entity_name(winner)
                        w_etype = _candidate_entity_type_name(winner)

                        notes_win = (
                            f"Auto-picked winner score={max_score} "
                            f"(runner-up={second_score}) from {len(scored)} stored candidates"
                        )
                        notes_nowin = (
                            f"No clear winner: top score={max_score}, "
                            f"second={second_score} from {len(scored)} candidates"
                        )

                        if clear and len(w_abn) == 11:
                            ucur.execute(
                                UPDATE_WIN_SQL,
                                {
                                    "id": vid,
                                    "abn": w_abn,
                                    "ename": w_name or None,
                                    "etype": w_etype,
                                    "score": max_score,
                                    "notes": notes_win,
                                },
                            )
                            stats["winners"] += 1
                        else:
                            ucur.execute(
                                UPDATE_NO_WIN_SQL,
                                {
                                    "id": vid,
                                    "score": max_score,
                                    "notes": notes_nowin,
                                },
                            )
                            stats["no_winner"] += 1

                        stats["processed"] += 1
                        consec_fail = 0
                    except Exception as ex:
                        stats["errors"] += 1
                        consec_fail += 1
                        LOG.exception("Venue %s failed: %s", vid, ex)
                        if consec_fail >= MAX_CONSECUTIVE_FAILURES:
                            conn.rollback()
                            raise SystemExit(
                                f"Stopping after {MAX_CONSECUTIVE_FAILURES} consecutive failures"
                            ) from ex
                        try:
                            ucur.execute(
                                UPDATE_NO_WIN_SQL,
                                {
                                    "id": vid,
                                    "score": None,
                                    "notes": f"Processing error: {ex!s}",
                                },
                            )
                        except Exception:
                            pass

            conn.commit()

    elapsed = time.monotonic() - started
    LOG.info(
        "W3 complete in %.1f s; processed=%s winners=%s no_winner=%s errors=%s",
        elapsed,
        stats["processed"],
        stats["winners"],
        stats["no_winner"],
        stats["errors"],
    )
    print(json.dumps({**stats, "elapsed_s": round(elapsed, 1)}, indent=2))


if __name__ == "__main__":
    main()
