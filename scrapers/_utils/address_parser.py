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

    structured_noise = has_paren or comma_total >= 3 or len(suburb_words) > 4
    confidence = "medium" if structured_noise else "high"

    return suburb_token, state, postcode, confidence
