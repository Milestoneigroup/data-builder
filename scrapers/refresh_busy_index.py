"""Annual Busy Index refresh — **STUB ONLY** (v1).

Intended to be wired into the Railway scheduler in a future release. For v1 this
module only prints a placeholder message.

TODO (v2): automated scrape / ingest of state education calendars and Fair Work
holiday pages; extend ``ref_major_events`` from official event feeds.
"""

from __future__ import annotations


def main() -> None:
    print("Annual refresh placeholder — manual seed update required for next year.")
    print("TODO v2: scrape state education sites + Fair Work; refresh major events from official APIs.")


if __name__ == "__main__":
    main()
