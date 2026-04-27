"""Railway job: reuse committed EW CSV; refresh Wedlockers, TWS, MCA; skip AFCC (WAF)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.celebrant_active_enrichment import (  # noqa: E402
    print_multi_directory_railway_footer,
    run_step1,
    run_step2,
)

if __name__ == "__main__":
    run_step1(
        ew_pages=15,
        skip_ew=True,
        skip_afcc=True,
    )
    stats, master = run_step2(output_file="data/celebrants_master_v3.csv")
    print_multi_directory_railway_footer(stats, master)
