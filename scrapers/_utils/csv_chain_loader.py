"""Load chain seed definitions from CSV (v3) for bulk chain mapping.

Coexists with ``chain_loader.load_chain_seed`` (JSON). Produces group + venue
structures aligned with ``data/chain_seeds/*.json`` so ``find_venue_match`` can
consume the same ``name``, ``suburb``, ``state``, ``address_hint`` keys.
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

GROUP_COLUMNS = (
    "group_slug",
    "group_name",
    "group_legal_name_guess",
    "group_website",
    "group_weddings_url",
    "group_hq_state",
    "group_hq_suburb",
    "group_size_estimate",
    "abn_lookup_strategy",
    "group_notes",
)

VENUE_COLUMNS = (
    "group_slug",
    "venue_name",
    "suburb_hint",
    "state",
    "address_hint",
    "evidence_url",
    "notes",
)


def parse_group_size_estimate(raw: str | None) -> int | None:
    """Take the first integer from strings such as ``70+`` or ``150+``."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = re.search(r"\d+", s)
    return int(m.group()) if m else None


def validate_headers(path: Path, expected: tuple[str, ...]) -> list[str]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"Empty CSV: {path}")
    normalised = [h.strip() for h in header]
    if tuple(normalised) != expected:
        raise ValueError(
            f"Unexpected header row in {path}.\nGot: {normalised}\nExpected: {expected}"
        )
    return normalised


def load_groups_by_slug(path: Path) -> dict[str, dict[str, Any]]:
    validate_headers(path, GROUP_COLUMNS)
    out: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row or not row.get("group_slug"):
                continue
            slug = str(row["group_slug"]).strip()
            out[slug] = row
    return out


def load_venues_grouped(path: Path) -> dict[str, list[dict[str, str]]]:
    validate_headers(path, VENUE_COLUMNS)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row or not row.get("group_slug"):
                continue
            slug = str(row["group_slug"]).strip()
            grouped[slug].append({k: (row.get(k) or "").strip() if row.get(k) else "" for k in VENUE_COLUMNS})
    return dict(grouped)


def _normalise_optional(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def build_group_payload(row: dict[str, Any]) -> dict[str, Any]:
    """Map CSV group row to ``public.venue_groups`` keys used by the bulk runner."""
    return {
        "group_slug": str(row["group_slug"]).strip(),
        "group_name": str(row["group_name"]).strip(),
        "group_legal_name": _normalise_optional(row.get("group_legal_name_guess")),
        "group_website": _normalise_optional(row.get("group_website")),
        "group_weddings_url": _normalise_optional(row.get("group_weddings_url")),
        "group_hq_state": str(row["group_hq_state"]).strip(),
        "group_hq_suburb": _normalise_optional(row.get("group_hq_suburb")),
        "group_size_estimate": parse_group_size_estimate(row.get("group_size_estimate") or ""),
        "abn_lookup_strategy": str(row.get("abn_lookup_strategy") or "").strip() or None,
        "group_notes": _normalise_optional(row.get("group_notes")),
    }


def csv_rows_to_seed_venues(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    """CSV venue rows -> JSON-like venue dicts for ``find_venue_match``."""
    out: list[dict[str, Any]] = []
    for row in rows:
        venue_name = str(row.get("venue_name") or "").strip()
        if not venue_name:
            continue
        out.append(
            {
                "name": venue_name,
                "suburb": _normalise_optional(row.get("suburb_hint")),
                "state": str(row.get("state") or "").strip(),
                "address_hint": _normalise_optional(row.get("address_hint")),
                "evidence_url": _normalise_optional(row.get("evidence_url")),
                "notes": _normalise_optional(row.get("notes")),
            }
        )
    return out


def iter_chain_seeds_from_csv(
    groups_csv: Path,
    venues_csv: Path,
) -> Iterator[dict[str, Any]]:
    """
    Yield one chain seed per ``group_slug``::

        {
          'group': { ... venue_groups columns ... },
          'venues': [ { 'name', 'suburb', 'state', 'address_hint', ... }, ... ],
        }
    """
    groups_by_slug = load_groups_by_slug(groups_csv)
    venues_by_slug = load_venues_grouped(venues_csv)

    for slug in sorted(groups_by_slug.keys()):
        g_row = groups_by_slug[slug]
        v_rows = venues_by_slug.get(slug, [])
        if not v_rows:
            yield {
                "group": build_group_payload(g_row),
                "venues": [],
            }
            continue
        yield {
            "group": build_group_payload(g_row),
            "venues": csv_rows_to_seed_venues(v_rows),
        }
