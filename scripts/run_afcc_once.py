"""One-off entrypoint for Railway: run AFCC profile scraper (not the scheduler)."""

import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from scrapers.afcc_profile_scraper import main  # noqa: E402

if __name__ == "__main__":
    main()
