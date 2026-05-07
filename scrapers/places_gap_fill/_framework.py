"""Shared scaffolding: budgeting, delays, paths, logging, Supabase factories."""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_PACKAGE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PACKAGE_DIR.parents[1]
_SRC = _REPO_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Authoritative runtime env bundle (Richard's machine + Railway parity).
load_dotenv(_REPO_ROOT / "env.local", override=True)

TEXT_SEARCH_COST = 0.032
PLACE_DETAILS_COST = 0.017
AVG_PER_VENDOR = 0.049
REQUEST_DELAY_S = 0.2
HIGH_CONFIDENCE_THRESHOLD = 0.65
LOW_CONFIDENCE_THRESHOLD = 0.50
NAME_DOMINANCE_THRESHOLD = 0.85
MATCH_SCORE_THRESHOLD = HIGH_CONFIDENCE_THRESHOLD
VERIFY_SENTINEL = "VERIFY_REQUIRED"


class BudgetTracker:
    """Strict USD guardrail tracker (calls are charged at planning rates below)."""

    def __init__(self, max_spend_usd: float) -> None:
        self._cap = float(max_spend_usd)
        self._spent = 0.0
        self._calls: list[tuple[str, float]] = []

    def can_afford(self, cost: float) -> bool:
        return (self._spent + float(cost)) <= self._cap + 1e-9

    def record_call(self, kind: str, cost: float) -> None:
        c = float(cost)
        self._spent += c
        self._calls.append((kind, c))

    @property
    def spent_usd(self) -> float:
        return self._spent

    def report(self) -> dict[str, Any]:
        return {
            "max_spend_usd": self._cap,
            "spent_usd": round(self._spent, 6),
            "remaining_usd": round(max(0.0, self._cap - self._spent), 6),
            "call_count": len(self._calls),
            "calls": [{"kind": k, "usd": round(v, 6)} for k, v in self._calls],
        }


def polite_delay() -> None:
    time.sleep(REQUEST_DELAY_S)


def setup_logging() -> tuple[logging.Logger, Path]:
    """Writes ``logs/places_gap_fill_<UTC-timestamp>.log`` plus stderr INFO."""
    log_dir = _REPO_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = log_dir / f"places_gap_fill_{ts}.log"

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    fmt = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)

    log = logging.getLogger("places_gap_fill")
    log.handlers.clear()
    log.addHandler(fh)
    log.addHandler(sh)
    log.setLevel(logging.INFO)
    return log, log_path


def places_api_key() -> str:
    k = (
        os.getenv("GOOGLE_PLACES_API_KEY")
        or os.getenv("GOOGLE_MAPS_API_KEY")
        or ""
    ).strip()
    if not k:
        raise RuntimeError(
            "GOOGLE_PLACES_API_KEY (or GOOGLE_MAPS_API_KEY) is required "
            "(set in Railway or env.local)."
        )
    return k


def supabase_clients() -> tuple[Any, Any]:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required.")
    from supabase import create_client

    return create_client(url, key), url


def is_blank_for_augment(val: Any) -> bool:
    """True when Places-derived fields should be permitted to populate."""
    if val is None:
        return True
    if val is False:
        # Default false on new boolean columns is augmentable (e.g. low-confidence flag).
        return True
    if isinstance(val, str):
        s = val.strip()
        if not s or s.upper() == "NAN":
            return True
        if s == VERIFY_SENTINEL:
            return True
    return False


def confidence_band_from_pct(pct: int) -> str:
    """Translate thefuzz token-sort score (0–100) into operator-facing buckets."""

    if pct >= 85:
        return "HIGH"
    if pct >= 65:
        return "MEDIUM"
    if pct >= 50:
        return "LOW"
    return "LOW"


def augmented_subset(
    existing_row: dict[str, Any],
    proposed: dict[str, Any],
    *,
    augment_keys: set[str],
) -> dict[str, Any]:
    """NULL / sentinel augmentation only for the requested keys."""

    out: dict[str, Any] = {}
    for k in augment_keys:
        if k not in proposed:
            continue
        v = proposed[k]
        if v is None:
            continue
        if not is_blank_for_augment(existing_row.get(k)):
            continue
        out[k] = v
    return out
