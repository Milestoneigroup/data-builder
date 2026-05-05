"""Shared ABN normalization and ATO mod-89 checksum (ABR-compatible).

Algorithm: subtract 1 from the first digit, apply weights (10,1,3,5,7,9,11,13,15,17,19),
sum products, valid when sum % 89 == 0.

Reference: https://abr.business.gov.au/Help/AbnFormat (Australian Business Register).
"""

from __future__ import annotations

import re

_ABN_WEIGHTS: tuple[int, ...] = (10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19)


def normalize_abn_digits(raw: str) -> str | None:
    """Extract exactly 11 digits, or return None."""
    d = re.sub(r"\D", "", raw)
    return d if len(d) == 11 else None


def abn_checksum_valid(eleven: str) -> bool:
    """True if ``eleven`` is 11 digits and satisfies the mod-89 check."""
    if len(eleven) != 11 or not eleven.isdigit():
        return False
    digits = [int(c) for c in eleven]
    digits[0] -= 1
    if digits[0] < 0:
        return False
    total = sum(d * w for d, w in zip(digits, _ABN_WEIGHTS, strict=True))
    return total % 89 == 0
