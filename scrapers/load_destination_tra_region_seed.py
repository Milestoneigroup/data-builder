"""Load ``data/seed_destination_to_tra_region.json`` into ``shared.ref_destination_to_tra_region``.

Run: ``python -m scrapers.load_destination_tra_region_seed``

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY``).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

_SEED_PATH = _ROOT / "data" / "seed_destination_to_tra_region.json"


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def main() -> None:
    _load_env()
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) required.")

    if not _SEED_PATH.is_file():
        raise FileNotFoundError(str(_SEED_PATH))
    rows: list[dict[str, Any]] = json.loads(_SEED_PATH.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise SystemExit("Seed file must contain a JSON array")
    if not rows:
        LOG.warning("Seed file is empty — run ``python -m scrapers.build_destination_tra_region_seed`` first.")
        return

    from supabase import create_client

    sb = create_client(url, key)
    sh = sb.schema("shared")
    sh.table("ref_destination_to_tra_region").upsert(rows, on_conflict="destination_id").execute()
    LOG.info("Upserted %s mapping rows at %s", len(rows), datetime.now(timezone.utc).isoformat())


if __name__ == "__main__":
    main()
