"""Backfill ``public.venues.suburb`` using Australian address parsing plus Places Details.

Primary source: parse the free-text ``address`` column.
Secondary source: Google Places Details (new) when parsing yields ``low`` confidence.

**Augmentation only:** updates suburb enrichment columns only - never ``name``, ``address``,
``postcode``, coordinates, ``google_name``, ``place_id``, or any other legacy column.

Run: ``python -m scrapers.suburb_backfill``

Requires ``env.local`` at the repo root with ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY``,
and ``GOOGLE_MAPS_API_KEY``.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import quote

import httpx
from dotenv import load_dotenv

from ._utils.address_parser import parse_au_address

_ROOT = Path(__file__).resolve().parents[1]

LOG = logging.getLogger("suburb_backfill")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

REQUEST_DELAY_S = 0.3
CHECKPOINT_EVERY = 50
WRITE_BATCH_SIZE = 50

PLACES_FIELD_MASK = "addressComponents"


class VenueRow(TypedDict, total=False):
    id: str
    name: str | None
    address: str | None
    postcode: str | None
    place_id: str | None


class SuburbUpdate(TypedDict):
    id: str
    suburb: str
    suburb_source: str
    suburb_confidence: str


def load_env() -> None:
    load_dotenv(_ROOT / "env.local", override=True)


def check_preflight(sb: Any) -> None:
    """Ensure Step 0 enrichment columns exist; abort loudly otherwise."""
    try:
        sb.table("venues").select(
            "id,suburb,suburb_source,suburb_confidence,suburb_backfilled_at"
        ).limit(1).execute()
    except Exception as exc:  # noqa: BLE001
        LOG.error("Preflight failed — Step 0 columns missing or inaccessible: %s", exc)
        raise SystemExit(1) from exc


def fetch_venues_needing_suburb(sb: Any) -> list[VenueRow]:
    """Return venue rows where ``suburb`` is still NULL."""
    page_size = 1_000
    offset = 0
    rows: list[VenueRow] = []
    while True:
        resp = (
            sb.table("venues")
            .select("id,name,address,postcode,place_id")
            .is_("suburb", "null")
            .order("id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


def parse_phase(
    venues: list[VenueRow],
) -> tuple[list[SuburbUpdate], list[VenueRow], dict[str, int]]:
    parsed_rows: list[SuburbUpdate] = []
    ambiguous_rows: list[VenueRow] = []
    counts = {"high": 0, "medium": 0, "low": 0}

    for i, v in enumerate(venues, start=1):
        addr_raw = v.get("address")
        addr = addr_raw if isinstance(addr_raw, str) else str(addr_raw or "")
        suburb, _state, _pc, conf = parse_au_address(addr)
        counts[conf] = counts.get(conf, 0) + 1

        if conf in ("high", "medium") and suburb:
            parsed_rows.append(
                {
                    "id": v["id"],
                    "suburb": suburb,
                    "suburb_source": "address_parse",
                    "suburb_confidence": conf,
                }
            )
        else:
            ambiguous_rows.append(v)

        if i % CHECKPOINT_EVERY == 0:
            LOG.info(
                "Checkpoint (parse): processed %s venues - high=%s medium=%s low=%s",
                i,
                counts["high"],
                counts["medium"],
                counts["low"],
            )

    return parsed_rows, ambiguous_rows, counts


def suburb_from_address_components(payload: dict[str, Any]) -> str | None:
    comps = payload.get("addressComponents")
    if not isinstance(comps, list):
        return None

    locality: str | None = None
    sublocals: list[str] = []

    for comp in comps:
        if not isinstance(comp, dict):
            continue
        types_raw = comp.get("types") or []
        if not isinstance(types_raw, list):
            continue
        types_set = {str(t) for t in types_raw}
        text = (comp.get("longText") or "").strip()
        if not text:
            continue

        if "locality" in types_set:
            locality = text
        elif any(str(t).startswith("sublocality") for t in types_raw):
            sublocals.append(text)

    return locality or (sublocals[0] if sublocals else None)


def fetch_suburb_places_details(client: httpx.Client, place_id: str, api_key: str) -> str | None:
    pid = quote(place_id, safe="")
    url = f"https://places.googleapis.com/v1/places/{pid}"
    r = client.get(
        url,
        headers={
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": PLACES_FIELD_MASK,
        },
        timeout=60.0,
    )
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError:
        LOG.warning("Places Details HTTP %s for place_id=%s", r.status_code, place_id[:24])
        return None

    data = r.json()
    if isinstance(data, dict) and "error" in data:
        err = data.get("error") or {}
        LOG.warning("Places Details API error for place_id=%s: %s", place_id[:24], err)
        return None

    return suburb_from_address_components(data) if isinstance(data, dict) else None


def places_phase(
    ambiguous_rows: list[VenueRow],
    api_key: str,
) -> tuple[list[SuburbUpdate], int, int, int]:
    """Resolve suburbs via Places Details for rows still missing after parsing."""
    staged: list[SuburbUpdate] = []
    queried = 0
    succeeded = 0
    failed = 0

    with httpx.Client() as client:
        eligible = [r for r in ambiguous_rows if r.get("place_id")]
        for i, v in enumerate(eligible, start=1):
            queried += 1
            pid = str(v.get("place_id") or "")
            suburb = fetch_suburb_places_details(client, pid, api_key)
            time.sleep(REQUEST_DELAY_S)

            if suburb:
                succeeded += 1
                staged.append(
                    {
                        "id": v["id"],
                        "suburb": suburb,
                        "suburb_source": "places_api",
                        "suburb_confidence": "high",
                    }
                )
            else:
                failed += 1

            if i % CHECKPOINT_EVERY == 0:
                LOG.info(
                    "Checkpoint (Places API): %s calls - succeeded=%s failed=%s",
                    i,
                    succeeded,
                    failed,
                )

    return staged, queried, succeeded, failed


def upsert_to_supabase(sb: Any, rows: list[SuburbUpdate]) -> None:
    """Apply UPDATE batches touching enrichment columns only."""
    if not rows:
        return

    ts = datetime.now(timezone.utc).isoformat()

    for batch_start in range(0, len(rows), WRITE_BATCH_SIZE):
        chunk = rows[batch_start : batch_start + WRITE_BATCH_SIZE]
        for row in chunk:
            sb.table("venues").update(
                {
                    "suburb": row["suburb"],
                    "suburb_source": row["suburb_source"],
                    "suburb_confidence": row["suburb_confidence"],
                    "suburb_backfilled_at": ts,
                }
            ).eq("id", row["id"]).execute()

        LOG.info(
            "Wrote Supabase batch ending %s (%s rows this batch)",
            batch_start + len(chunk),
            len(chunk),
        )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    t0 = time.monotonic()

    load_env()

    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    gmaps_key = (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()

    if not url or not key:
        LOG.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env.local.")
        raise SystemExit(1)
    if not gmaps_key:
        LOG.error("Missing GOOGLE_MAPS_API_KEY in env.local.")
        raise SystemExit(1)

    from supabase import create_client

    sb = create_client(url, key)

    check_preflight(sb)

    venues = fetch_venues_needing_suburb(sb)
    total = len(venues)
    LOG.info("Fetched %s venues needing suburb backfill", total)

    parsed_rows, ambiguous_rows, parse_counts = parse_phase(venues)

    no_place_id = sum(1 for r in ambiguous_rows if not r.get("place_id"))
    LOG.info(
        "Phase A (address_parse): high=%s medium=%s low=%s - staged %s updates",
        parse_counts["high"],
        parse_counts["medium"],
        parse_counts["low"],
        len(parsed_rows),
    )
    LOG.info(
        "Ambiguous rows after parsing: %s (%s without place_id - manual review queue)",
        len(ambiguous_rows),
        no_place_id,
    )

    places_rows: list[SuburbUpdate] = []
    queried = succeeded = failed = 0
    if ambiguous_rows:
        places_rows, queried, succeeded, failed = places_phase(ambiguous_rows, gmaps_key)
        LOG.info(
            "Phase B (places_api): queried=%s succeeded=%s failed=%s - staged %s updates",
            queried,
            succeeded,
            failed,
            len(places_rows),
        )

    combined = parsed_rows + places_rows
    upsert_to_supabase(sb, combined)

    elapsed = time.monotonic() - t0
    google_calls = queried
    est_cost = google_calls * 0.017

    LOG.info("Finished - total venues processed=%s", total)
    LOG.info(
        "Google Places Details calls=%s (~USD %.4f est.) runtime=%.1fs",
        google_calls,
        est_cost,
        elapsed,
    )

    print("")
    print("Summary")
    print("-------")
    print(f"Total venues processed: {total}")
    print(
        "Phase A (address_parse) counts - "
        f"high: {parse_counts['high']}, medium: {parse_counts['medium']}, low: {parse_counts['low']}"
    )
    print(
        "Phase B (places_api) - "
        f"queried: {queried}, succeeded: {succeeded}, failed: {failed}"
    )
    print(f"Google Places API calls: {google_calls} (~USD {est_cost:.4f} est.)")
    print(f"Runtime: {elapsed:.1f}s")
    print(
        "Anomalies - ambiguous parse without place_id (manual review): "
        f"{no_place_id}"
    )


if __name__ == "__main__":
    main()
