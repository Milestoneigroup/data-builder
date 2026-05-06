"""Australian address parsing helpers for enrichment jobs."""

from __future__ import annotations

import re
from typing import Optional, Tuple

AU_STATES = {"NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"}

# Word boundary before state abbreviations (WA/SA must not match inside longer tokens).
_STATE_POSTCODE_RE = re.compile(
    r"\b(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\s+(\d{4})\b"
)


def parse_au_address(address: str) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    """
    Parse an Australian address string into (suburb, state, postcode, confidence).

    Strategy: anchor on '<STATE> <POSTCODE>' near end, then walk backwards through
    commas to find suburb. Handles messy multi-comma addresses by taking the LAST
    comma-separated segment before the state+postcode anchor.

    Examples:
      '73 Wickham Ter, Spring Hill QLD 4000, Australia'
        -> ('Spring Hill', 'QLD', '4000', 'high')
      '52-54 Stanley St, Richmond VIC 3121, Australia'
        -> ('Richmond', 'VIC', '3121', 'high')
      Messy Moonshadow case (multiple commas, parenthetical asides, suburb mid-string)
        -> ('Nelson Bay', 'NSW', '2315', 'medium')   # state+postcode anchor still works
      Address with no recognisable AU state+postcode pattern
        -> (None, None, None, 'low')

    Confidence:
      high   = clean parse, single state+postcode anchor near end
      medium = state+postcode anchor found but address has unusual structure
      low    = no state+postcode anchor found, or multiple conflicting anchors
    """
    if not address or not str(address).strip():
        return None, None, None, "low"

    raw = str(address).strip()
    # Normalise commas only (preserve casing; trim runs of commas).
    normalised = re.sub(r"\s*,\s*", ", ", raw)
    normalised = re.sub(r"(?:,\s*)+", ", ", normalised).strip().strip(",")

    matches = list(_STATE_POSTCODE_RE.finditer(normalised))
    if not matches:
        return None, None, None, "low"

    anchor_pairs = {(m.group(1), m.group(2)) for m in matches}
    if len(anchor_pairs) != 1:
        return None, None, None, "low"

    last = matches[-1]
    state, postcode = last.group(1), last.group(2)
    prefix = normalised[: last.start()].strip().strip(",")

    segments = [seg.strip() for seg in prefix.split(",") if seg.strip()]
    suburb_token = segments[-1] if segments else ""

    if not suburb_token:
        return None, None, None, "low"

    suburb_words = [w for w in suburb_token.split() if w]
    comma_total = normalised.count(",")
    has_paren = "(" in raw or ")" in raw

    # ``high``: at most two commas, no parentheses, suburb token is one to four words.
    # Busier strings (three or more commas and/or parentheses) stay ``medium`` even when
    # the state/postcode anchor is trustworthy (matches regression fixtures).
    structured_noise = has_paren or comma_total >= 3 or len(suburb_words) > 4
    confidence = "medium" if structured_noise else "high"

    return suburb_token, state, postcode, confidence


if __name__ == "__main__":
    _CASES: list[tuple[str, tuple[Optional[str], Optional[str], Optional[str], str]]] = [
        (
            "73 Wickham Ter, Spring Hill QLD 4000, Australia",
            ("Spring Hill", "QLD", "4000", "high"),
        ),
        (
            "52-54 Stanley St, Richmond VIC 3121, Australia",
            ("Richmond", "VIC", "3121", "high"),
        ),
        (
            "1 Waldron Dr, City Beach WA 6015, Australia",
            ("City Beach", "WA", "6015", "high"),
        ),
        (
            "4484 Hamilton Hwy, Hesse VIC 3321, Australia",
            ("Hesse", "VIC", "3321", "high"),
        ),
        (
            "Some Building, Level 5/123 Pitt St, Sydney NSW 2000, Australia",
            ("Sydney", "NSW", "2000", "medium"),
        ),
        (
            "3/35 Stockton Street (Booking Office (Cruises depart from a separate location- "
            "Nelson Bay Marina, Check your ticket confirmation for departure address and dock, "
            "3/35 Stockton St, Nelson Bay NSW 2315, Australia",
            ("Nelson Bay", "NSW", "2315", "medium"),
        ),
        (
            "Random Street with no state",
            (None, None, None, "low"),
        ),
    ]

    failures = 0
    for addr, expected in _CASES:
        got = parse_au_address(addr)
        if got != expected:
            failures += 1
            print(f"FAIL\n  addr: {addr!r}\n  got:  {got}\n  exp:  {expected}")

    if failures:
        raise SystemExit(f"{failures} test(s) failed")

    print(f"OK ({len(_CASES)} tests)")
