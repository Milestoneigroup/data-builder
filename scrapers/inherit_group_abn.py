"""Apply chain-group ABN inheritance onto venues (Phase 2 fields + Phase 3 audit).

Runs **after** ``group_abn_lookup.py``. Single SQL ``UPDATE`` with row-count reporting.
Targets ``data_source = 'chain_seed_csv_v3'`` venues only (chain seed rows).

Run: ``python scrapers/inherit_group_abn.py``

Requires: ``DATABASE_URL`` in ``env.local`` (``load_dotenv(..., override=True)``).
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger("inherit_group_abn")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SQL = """
UPDATE public.venues v
SET abn_phase2_value = vg.group_abn,
    abn_phase2_entity_legal_name = vg.group_abn_entity_name,
    abn_phase2_entity_type = vg.group_abn_entity_type,
    abn_phase2_match_confidence = CASE
        WHEN vg.group_abn_status = 'verified' THEN 'strong'
        WHEN vg.group_abn_status = 'probable' THEN 'fuzzy'
    END,
    abn_phase2_query_strategy = 'manual',
    abn_phase2_query_used = 'INHERIT FROM ' || vg.group_slug,
    abn_phase3_method = CASE
        WHEN vg.group_abn_status = 'verified' THEN 'group_inherited_verified'
        WHEN vg.group_abn_status = 'probable' THEN 'group_inherited_probable'
    END,
    abn_phase3_attempted_at = now(),
    abn_phase3_notes = 'Inherited via chain group ' || vg.group_slug
        || ' (group ABN status: ' || vg.group_abn_status || ')'
FROM public.venue_group_membership vgm
JOIN public.venue_groups vg ON vg.group_id = vgm.group_id
WHERE v.id = vgm.venue_id
  AND v.abn_phase2_value IS NULL
  AND v.data_source = 'chain_seed_csv_v3'
  AND vg.group_abn IS NOT NULL
  AND vg.group_abn_status IN ('verified', 'probable')
  AND vg.abn_lookup_strategy = 'operator_group_lookup'
"""


def main() -> None:
    db = os.getenv("DATABASE_URL", "").strip()
    if not db:
        raise SystemExit("DATABASE_URL is required in env.local")
    with psycopg.connect(db, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            n = cur.rowcount
        conn.commit()
    LOG.info("W2 chain inheritance: rows updated=%s", n)
    print(f"W2 rows updated: {n}")


if __name__ == "__main__":
    main()
