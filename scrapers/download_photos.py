"""Download venue facade images via Google Places API (New), upload to Supabase Storage.

Reads ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY``, and ``GOOGLE_MAPS_API_KEY`` from ``env.local``
(loaded with ``override=True`` per data-builder convention).

**Important (pre-flight, May 2026):**
``public.venues`` retains photo identifiers in ``photo_ref_1`` … ``photo_ref_4`` (legacy layout).
Those values are Places API ``photos[].name`` strings. Older names return ``400 INVALID_ARGUMENT``
from ``Place Photo`` media downloads; reliable downloads require calling ``Place Details (New)``
for the current ``photos[]`` list, then retrieving each binary via the media endpoint.

This script touches **only**: ``photo_storage_urls``, ``photos_downloaded_at``,
``photos_download_error`` on ``public.venues``. It never updates ``photo_ref_*`` or
``google_photos`` (the latter column does not exist in the current warehouse schema).

Run: ``python -m scrapers.download_photos``
"""

from __future__ import annotations

import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from supabase import Client, create_client

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / "env.local", override=True)

VENUE_BUCKET = "venue_photos"
BATCH_VENUES = 25
THROTTLE_MEDIA_SEC = 0.3
THROTTLE_BATCH_SEC = 1.0
MAX_PHOTOS_PER_VENUE = 10
MAX_MEDIA_CALLS_HARD = 4000
MAX_ESTIMATE_USD = 50.0
CONSECUTIVE_FAILURE_CAP = 5
PAUSE_ON_FAILURE_CLUSTER_SEC = 60

COST_MEDIA_USD = 0.007
COST_PLACE_DETAILS_ESSENTIALS_USD = 0.017

PHOTO_FIELD_MASK = "photos"


def _has_photo_enrichment(row: dict[str, Any]) -> bool:
    for k in ("photo_ref_1", "photo_ref_2", "photo_ref_3", "photo_ref_4"):
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            return True
    tc = row.get("total_photo_count")
    try:
        if tc is not None and int(tc) > 0:
            return True
    except (TypeError, ValueError):
        pass
    return False


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val or not val.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val.strip()


def _estimate_cost_usd(place_details_calls: int, media_calls: int) -> float:
    return place_details_calls * COST_PLACE_DETAILS_ESSENTIALS_USD + media_calls * COST_MEDIA_USD


def _media_extension(content_type: str | None) -> str:
    if not content_type:
        return "bin"
    ct = content_type.split(";")[0].strip().lower()
    if ct in ("image/jpeg", "image/jpg"):
        return "jpg"
    if ct == "image/png":
        return "png"
    if ct == "image/webp":
        return "webp"
    return "bin"


def _supabase_client() -> Client:
    return create_client(_require_env("SUPABASE_URL"), _require_env("SUPABASE_SERVICE_ROLE_KEY"))


def _load_pending_queue(client: Client) -> list[dict[str, Any]]:
    """Venues with a Place ID and primary photo ref but no Storage URLs yet (true gaps only)."""
    offset = 0
    chunk = 1000
    out: list[dict[str, Any]] = []
    while True:
        q = (
            client.table("venues")
            .select(
                "id,name,place_id,photo_ref_1,photo_ref_2,photo_ref_3,photo_ref_4,"
                "total_photo_count,data_source"
            )
            .is_("photo_storage_urls", "null")
            .not_.is_("place_id", "null")
            .not_.is_("photo_ref_1", "null")
            .order("id")
            .range(offset, offset + chunk - 1)
        )
        rows = q.execute().data or []
        if not rows:
            break
        out.extend(rows)
        offset += chunk
        if len(rows) < chunk:
            break
    out.sort(
        key=lambda r: (
            0 if r.get("data_source") == "chain_seed_csv_v3" else 1,
            str(r.get("id") or ""),
        )
    )
    return out


def _stamp_photos_downloaded_at_where_stored(sb: Client) -> None:
    """Backfill timestamps where photos already exist in Storage (keeps audit queries clean)."""
    now_iso = datetime.now(UTC).isoformat()
    sb.table("venues").update({"photos_downloaded_at": now_iso}).not_.is_(
        "photo_storage_urls", "null"
    ).is_("photos_downloaded_at", "null").execute()
    print(
        "Housekeeping: set photos_downloaded_at where photo_storage_urls is present "
        "but photos_downloaded_at was null."
    )


def _place_details(session: httpx.Client, google_key: str, place_resource: str) -> dict[str, Any]:
    headers = {"X-Goog-Api-Key": google_key, "X-Goog-FieldMask": PHOTO_FIELD_MASK}
    url = f"https://places.googleapis.com/v1/places/{place_resource}"
    resp = session.get(url, headers=headers, timeout=120.0)
    resp.raise_for_status()
    return resp.json()


def _download_photo_media(
    session: httpx.Client, google_key: str, photo_name: str
) -> tuple[bytes, str]:
    url = f"https://places.googleapis.com/v1/{photo_name}/media"
    resp = session.get(
        url, params={"maxWidthPx": 1600, "key": google_key}, timeout=120.0, follow_redirects=True
    )
    resp.raise_for_status()
    ct = resp.headers.get("content-type") or ""
    return resp.content, ct


def _upload_and_public_url(sb: Client, venue_id: str, index: int, body: bytes, content_type: str) -> str:
    ext = _media_extension(content_type)
    object_path = f"venues/{venue_id}/{index}.{ext}"
    bucket = sb.storage.from_(VENUE_BUCKET)
    bucket.upload(
        object_path,
        body,
        file_options={
            "content-type": content_type.split(";")[0].strip(),
            "upsert": "true",
        },
    )
    return bucket.get_public_url(object_path)


def _update_venues_failure(sb: Client, venue_id: str, msg: str) -> None:
    payload = {"photos_download_error": msg[:8000]}
    sb.table("venues").update(payload).eq("id", venue_id).execute()


def _count_no_ref_venues(sb: Client) -> int:
    """How many venues have no stored photo enrichment (refs + total count)."""
    offset = 0
    chunk = 1000
    n = 0
    while True:
        r2 = (
            sb.table("venues")
            .select("photo_ref_1,photo_ref_2,photo_ref_3,photo_ref_4,total_photo_count")
            .order("id")
            .range(offset, offset + chunk - 1)
            .execute()
        )
        block = r2.data or []
        if not block:
            break
        for row in block:
            if not _has_photo_enrichment(row):
                n += 1
        offset += chunk
        if len(block) < chunk:
            break
    return n


def ensure_bucket(sb: Client) -> None:
    names = [b.name for b in sb.storage.list_buckets()]
    if VENUE_BUCKET not in names:
        sb.storage.create_bucket(VENUE_BUCKET, options={"public": True})
        print(f"Created Storage bucket '{VENUE_BUCKET}' as public.")
    else:
        print(f"Bucket '{VENUE_BUCKET}' exists; leaving configuration unchanged.")

    objs = sb.storage.from_(VENUE_BUCKET).list("venues", {"limit": 5})
    existing = objs if isinstance(objs, list) else []
    if existing:
        print(
            "[Notice] Bucket already contains venue objects "
            "(first page non-empty); continuing with upsert uploads.",
            file=sys.stderr,
        )


def run() -> None:
    google_key = _require_env("GOOGLE_MAPS_API_KEY")
    sb = _supabase_client()
    ensure_bucket(sb)
    _stamp_photos_downloaded_at_where_stored(sb)

    pre_no_enrichment = _count_no_ref_venues(sb)

    processed = succeeded = failed = 0
    total_urls = 0
    place_calls = media_calls = 0
    consecutive_place_failures = 0

    summary_every = BATCH_VENUES
    pending_accum = _load_pending_queue(sb)

    stop_everything = False

    session = httpx.Client()

    session_started = datetime.now(UTC)

    print(
        f"Photo download sweep started UTC {session_started.isoformat()} | "
        f"batch_size={BATCH_VENUES} | max_media_calls={MAX_MEDIA_CALLS_HARD} | "
        f"max_budget_estimate_usd={MAX_ESTIMATE_USD:.2f} "
        f"(pricing uses published Place Details Essentials + Photo SKUs).\n"
        f"Barker count of venues lacking any photo enrichment (pre-scan): {pre_no_enrichment}"
    )

    while True:
        spend = _estimate_cost_usd(place_calls, media_calls)
        if spend >= MAX_ESTIMATE_USD:
            print(
                f"Hard stop: estimated spend ${spend:.2f} exceeds cap "
                f"${MAX_ESTIMATE_USD:.2f} USD."
            )
            break
        if media_calls >= MAX_MEDIA_CALLS_HARD:
            print("Hard stop: 4,000 media calls reached.")
            break

        if not pending_accum:
            print("No more pending venues.")
            break

        batch = pending_accum[:BATCH_VENUES]
        pending_accum = pending_accum[BATCH_VENUES:]

        for row in batch:
            if stop_everything:
                break
            venue_id = str(row["id"])
            place_id = (row.get("place_id") or "").strip()
            if not place_id:
                continue

            processed += 1

            try:
                if _estimate_cost_usd(place_calls + 1, media_calls) > MAX_ESTIMATE_USD:
                    print("Budget would be exceeded before next venue; stopping.")
                    stop_everything = True
                    break

                details: dict[str, Any]
                try:
                    details = _place_details(session, google_key, place_id)
                except httpx.HTTPError as exc:
                    consecutive_place_failures += 1
                    if consecutive_place_failures >= CONSECUTIVE_FAILURE_CAP:
                        print(
                            f"{CONSECUTIVE_FAILURE_CAP} consecutive Places API failures "
                            f"— pausing {PAUSE_ON_FAILURE_CLUSTER_SEC}s, then retrying Details."
                        )
                        time.sleep(PAUSE_ON_FAILURE_CLUSTER_SEC)
                        consecutive_place_failures = 0
                        try:
                            details = _place_details(session, google_key, place_id)
                        except httpx.HTTPError as exc2:
                            consecutive_place_failures += 1
                            msg = (
                                "Place Details still failing after cooldown; exiting clean. "
                                f"Last error: {exc2!r}"
                            )
                            print(msg)
                            _update_venues_failure(sb, venue_id, msg[:8000])
                            failed += 1
                            stop_everything = True
                            break
                    else:
                        _update_venues_failure(sb, venue_id, f"Place Details error: {exc!r}")
                        failed += 1
                        continue

                place_calls += 1
                consecutive_place_failures = 0

                photos = details.get("photos") if isinstance(details.get("photos"), list) else []
                names = [
                    str(p["name"]) for p in photos if isinstance(p, dict) and p.get("name")
                ]
                names = names[:MAX_PHOTOS_PER_VENUE]

                urls: list[str] = []
                budget_mid_venue = False
                stopped_media_cap = False
                venue_failed_here = False

                for idx, photo_name in enumerate(names):
                    if media_calls >= MAX_MEDIA_CALLS_HARD:
                        stopped_media_cap = True
                        break
                    if _estimate_cost_usd(place_calls, media_calls + 1) > MAX_ESTIMATE_USD:
                        print(
                            "Budget cap hit mid-venue; recording error "
                            "without timestamps so the venue retries."
                        )
                        budget_mid_venue = True
                        break
                    time.sleep(THROTTLE_MEDIA_SEC)

                    body: bytes | None = None
                    ct = ""
                    try:
                        body, ct = _download_photo_media(session, google_key, photo_name)
                    except httpx.HTTPError as exc:
                        consecutive_place_failures += 1
                        if consecutive_place_failures >= CONSECUTIVE_FAILURE_CAP:
                            print(
                                f"{CONSECUTIVE_FAILURE_CAP} consecutive Places API failures "
                                "— pausing prior to retrying the same photograph."
                            )
                            time.sleep(PAUSE_ON_FAILURE_CLUSTER_SEC)
                            consecutive_place_failures = 0
                            try:
                                body, ct = _download_photo_media(
                                    session, google_key, photo_name
                                )
                            except httpx.HTTPError as exc2:
                                consecutive_place_failures += 1
                                _update_venues_failure(
                                    sb,
                                    venue_id,
                                    f"Persistent Places media error after cooldown: {exc2!r}",
                                )
                                venue_failed_here = True
                                failed += 1
                                stop_everything = True
                                break
                        else:
                            _update_venues_failure(sb, venue_id, f"Place media error: {exc!r}")
                            venue_failed_here = True
                            failed += 1
                            break

                    if venue_failed_here or body is None:
                        break

                    consecutive_place_failures = 0
                    media_calls += 1
                    urls.append(_upload_and_public_url(sb, venue_id, idx, body, ct))

                if venue_failed_here:
                    continue

                if budget_mid_venue or stopped_media_cap:
                    parts: list[str] = []
                    if budget_mid_venue:
                        parts.append("stopped on budget ceiling mid-venue")
                    if stopped_media_cap:
                        parts.append("stopped on media-call hard cap")
                    _update_venues_failure(sb, venue_id, "; ".join(parts))
                    failed += 1
                    if stopped_media_cap:
                        stop_everything = True
                    continue

                total_urls += len(urls)

                payload = {
                    "photo_storage_urls": urls,
                    "photos_downloaded_at": datetime.now(UTC).isoformat(),
                    "photos_download_error": None,
                }
                sb.table("venues").update(payload).eq("id", venue_id).execute()

                succeeded += 1

            except BaseException as exc:  # noqa: BLE001
                failed += 1
                _update_venues_failure(sb, venue_id, f"{type(exc).__name__}: {exc!r}")

            spend_live = _estimate_cost_usd(place_calls, media_calls)
            if processed % summary_every == 0:
                print(
                    f"Summary checkpoint | processed={processed} succeeded={succeeded} "
                    f"failed={failed} total_photos_uploaded={total_urls} "
                    f"estimated_cost_usd={spend_live:.2f} "
                    f"(details_calls={place_calls} @ ${COST_PLACE_DETAILS_ESSENTIALS_USD} USD, "
                    f"media_calls={media_calls} @ ${COST_MEDIA_USD} USD)"
                )

        time.sleep(THROTTLE_BATCH_SEC)

    post_no_enrichment = _count_no_ref_venues(sb)

    print(
        f"\nSweep finished UTC {datetime.now(UTC).isoformat()} | processed={processed} "
        f"succeeded={succeeded} failed={failed} total_photos_uploaded={total_urls} "
        f"place_detail_calls={place_calls} media_calls={media_calls} "
        f"estimated_cost_usd={_estimate_cost_usd(place_calls, media_calls):.2f}\n"
        f"Venues with no enrichment count pre={pre_no_enrichment} post={post_no_enrichment} "
        "(expect identical if original columns untouched)"
    )


if __name__ == "__main__":
    run()
