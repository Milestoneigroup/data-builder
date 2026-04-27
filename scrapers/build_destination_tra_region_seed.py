"""Build ``data/seed_destination_to_tra_region.json`` from Supabase or a CSV export.

Mappings use **ASGS Tourism Region** names/codes from ``data/ref_tra_regions_asgs2021.json``
(derived from ABS ``TR_2021_AUST.xlsx`` — the same TR geography TRA uses for STAR).

Run (Supabase):

  ``python -m scrapers.build_destination_tra_region_seed``

Run (CSV export with columns ``destination_id,destination_name,state_code``):

  ``python -m scrapers.build_destination_tra_region_seed --csv path/to/ref_destinations.csv``

Requires ``thefuzz`` for fuzzy matching when notes are not exact.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from thefuzz import fuzz, process

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

_CATALOG_PATH = _ROOT / "data" / "ref_tra_regions_asgs2021.json"
_OUT_PATH = _ROOT / "data" / "seed_destination_to_tra_region.json"

_LABEL_ALIASES: dict[tuple[str, str], str] = {
    ("VIC", "Mornington Peninsula"): "Peninsula",
    ("VIC", "Phillip Island"): "Phillip Island",
}


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _load_catalog() -> list[dict[str, str]]:
    data = json.loads(_CATALOG_PATH.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("ref_tra_regions_asgs2021.json must be a list")
    au_states = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}
    return [r for r in data if isinstance(r, dict) and str(r.get("state_code", "")).upper() in au_states]


def _match(
    state_code: str,
    destination_name: str,
    catalog: list[dict[str, str]],
) -> tuple[str, str, str, str]:
    key = (state_code, destination_name)
    target = _LABEL_ALIASES.get(key, destination_name)
    choices = [r for r in catalog if r.get("state_code") == state_code]
    if not choices:
        raise SystemExit(f"No TR regions for state {state_code}")
    names = [c["tra_region_name"] for c in choices]
    best = process.extractOne(target, names, scorer=fuzz.token_sort_ratio)
    if not best:
        raise RuntimeError("thefuzz returned no candidate")
    if len(best) == 3:
        name_hit, score, _ = best
    else:
        name_hit, score = best
    row = next(c for c in choices if c["tra_region_name"] == name_hit)
    if score >= 95:
        conf = "exact"
    elif score >= 82:
        conf = "strong"
    else:
        conf = "approximate"
    notes = f"ASGS TR fuzzy match score={score} from destination_name to TR_NAME_2021"
    return row["tra_region_code"], row["tra_region_name"], conf, notes


def _fetch_destinations_supabase() -> list[dict[str, str]]:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required for Supabase mode.")
    from supabase import create_client

    sb = create_client(url, key)
    rows: list[dict[str, str]] = []
    offset = 0
    page = 1000
    while True:
        res = (
            sb.schema("shared")
            .table("ref_destinations")
            .select("destination_id,destination_name,state_code")
            .eq("is_active", True)
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = res.data or []
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


def _fetch_destinations_csv(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            did = (row.get("destination_id") or "").strip()
            name = (row.get("destination_name") or "").strip()
            st = (row.get("state_code") or "").strip().upper()
            if did and name and st:
                rows.append({"destination_id": did, "destination_name": name, "state_code": st})
    return rows


def main(argv: list[str] | None = None) -> None:
    _load_env()
    p = argparse.ArgumentParser(description="Build destination→TRA region seed JSON.")
    p.add_argument("--csv", type=Path, help="CSV with destination_id,destination_name,state_code")
    p.add_argument("--output", type=Path, default=_OUT_PATH, help="Output JSON path")
    args = p.parse_args(argv)

    catalog = _load_catalog()
    if args.csv:
        destinations = _fetch_destinations_csv(args.csv)
        LOG.info("Loaded %s destinations from %s", len(destinations), args.csv)
    else:
        destinations = _fetch_destinations_supabase()
        LOG.info("Loaded %s destinations from Supabase", len(destinations))

    out: list[dict[str, Any]] = []
    low = 0
    for d in destinations:
        st = str(d["state_code"]).upper()
        name = str(d["destination_name"])
        try:
            code, tr_name, conf, notes = _match(st, name, catalog)
        except Exception as e:  # noqa: BLE001
            LOG.warning("Unmapped %s %s: %s", d["destination_id"], name, e)
            continue
        if conf == "approximate":
            low += 1
        out.append(
            {
                "destination_id": d["destination_id"],
                "tra_region_code": code,
                "tra_region_name": tr_name,
                "mapping_confidence": conf,
                "mapping_notes": notes,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(out, indent=2), encoding="utf-8")
    LOG.info("Wrote %s rows (%s approximate) to %s", len(out), low, args.output)


if __name__ == "__main__":
    main()
