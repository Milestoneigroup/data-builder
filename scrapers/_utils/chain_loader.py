"""Load structured chain seed JSON files for ``map_venues_to_groups``."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REQUIRED_TOP_LEVEL = ("group_name", "group_slug", "venues")


def load_chain_seed(path: Path) -> dict[str, Any]:
    """Read and return a chain definition dict; validate minimal shape."""
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"Seed root must be an object: {path}")
    missing = [k for k in REQUIRED_TOP_LEVEL if k not in data]
    if missing:
        raise ValueError(f"Seed missing keys {missing}: {path}")
    venues = data.get("venues")
    if not isinstance(venues, list) or not venues:
        raise ValueError(f"Seed 'venues' must be a non-empty list: {path}")
    return data
