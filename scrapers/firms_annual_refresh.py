"""Placeholder for annual NASA FIRMS refresh (not scheduled in v1).

Run: ``python -m scrapers.firms_annual_refresh``
"""

from __future__ import annotations


def main() -> None:
    print(
        "Annual refresh placeholder. Re-run firms_hotspot_download.py with start_date "
        "set to last year's data_period_end, then re-run firms_aggregate_by_destination.py."
    )


if __name__ == "__main__":
    main()
