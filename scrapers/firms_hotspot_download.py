"""NASA FIRMS Area API: VIIRS S-NPP Standard (archive) hotspots for Australia.

Run: ``python -m scrapers.firms_hotspot_download``

Requires ``NASA_FIRMS_MAP_KEY`` (see https://firms.modaps.eosdis.nasa.gov/api/map_key/).

API reference (verified 2026-04-27): https://firms.modaps.eosdis.nasa.gov/api/area/
- Path: ``/api/area/csv/{MAP_KEY}/{SOURCE}/{west,south,east,north}/{DAY_RANGE}/{DATE}``
- ``DAY_RANGE`` is 1..5 days per request (not 10).
- ``DATE`` optional: when set, returns [DATE .. DATE+DAY_RANGE-1].
- Archive source: ``VIIRS_SNPP_SP`` (Standard Processing).

Fair use: 2s pause between requests; retries with backoff on transient errors.
Resumable: skips chunks whose output CSV already exists.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterator

import httpx
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("firms_download")

_ROOT = Path(__file__).resolve().parents[1]
_FIRMS_BASE = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
_SOURCE = "VIIRS_SNPP_SP"
# west,south,east,north per FIRMS Area API (not north/south/east/west keywords).
_AU_BBOX = "112,-44,154,-10"
_MAX_DAY_RANGE = 5
_START_DATE = date(2012, 1, 20)
_REQUEST_PAUSE_S = 2.0
_MAX_ATTEMPTS = 5
_BACKOFF_S = (2, 4, 8, 16, 32)
_TIMEOUT_S = 120.0
_MAX_FAILURE_RATE = 0.05


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _yesterday_utc() -> date:
    return (datetime.utcnow() - timedelta(days=1)).date()


def _chunk_ranges(
    start: date, end: date, chunk_days: int
) -> Iterator[tuple[date, date]]:
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def _chunk_path(year: int, d0: date, d1: date) -> Path:
    out_dir = _ROOT / "data" / "firms_archive" / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{d0.isoformat()}_{d1.isoformat()}.csv"


def _fetch_chunk(client: httpx.Client, map_key: str, d0: date, d1: date) -> str:
    span = (d1 - d0).days + 1
    if span > _MAX_DAY_RANGE:
        raise ValueError(f"chunk span {span} exceeds API max {_MAX_DAY_RANGE}")
    url = f"{_FIRMS_BASE}/{map_key}/{_SOURCE}/{_AU_BBOX}/{span}/{d0.isoformat()}"
    last_err: Exception | None = None
    for attempt in range(_MAX_ATTEMPTS):
        try:
            r = client.get(url, timeout=_TIMEOUT_S)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code} retry")
            r.raise_for_status()
            text = r.text
            if text.lstrip().startswith("<"):
                raise RuntimeError("unexpected HTML response")
            return text
        except (httpx.HTTPError, OSError, RuntimeError) as e:
            last_err = e
            if attempt < _MAX_ATTEMPTS - 1:
                time.sleep(_BACKOFF_S[min(attempt, len(_BACKOFF_S) - 1)])
    assert last_err is not None
    raise last_err


def main() -> int:
    _load_env()
    map_key = os.environ.get("NASA_FIRMS_MAP_KEY", "").strip()
    if not map_key:
        log.error("Missing NASA_FIRMS_MAP_KEY in environment (.env / env.local).")
        return 1

    end = _yesterday_utc()
    start = _START_DATE
    if end < start:
        log.error("End date before start.")
        return 1

    chunks = list(_chunk_ranges(start, end, _MAX_DAY_RANGE))
    total_chunks = len(chunks)
    failed = 0
    total_rows = 0
    skipped = 0

    with httpx.Client(follow_redirects=True) as client:
        for i, (d0, d1) in enumerate(chunks, start=1):
            path = _chunk_path(d0.year, d0, d1)
            if path.is_file() and path.stat().st_size > 0:
                skipped += 1
                body = path.read_text(encoding="utf-8", errors="replace")
                lines = [ln for ln in body.splitlines() if ln.strip()]
                nrows = max(0, len(lines) - 1)
                total_rows += nrows
                log.info(
                    "skip %s/%s %s..%s (%s rows, existing file)",
                    i,
                    total_chunks,
                    d0,
                    d1,
                    nrows,
                )
                time.sleep(_REQUEST_PAUSE_S)
                continue
            try:
                body = _fetch_chunk(client, map_key, d0, d1)
            except Exception as e:
                failed += 1
                log.error("chunk %s..%s failed: %s", d0, d1, e)
                time.sleep(_REQUEST_PAUSE_S)
                continue
            path.write_text(body, encoding="utf-8")
            lines = [ln for ln in body.splitlines() if ln.strip()]
            if not lines:
                log.warning("empty response %s..%s", d0, d1)
            nrows = max(0, len(lines) - 1)
            total_rows += nrows
            log.info(
                "ok %s/%s %s..%s (%s rows)",
                i,
                total_chunks,
                d0,
                d1,
                nrows,
            )
            time.sleep(_REQUEST_PAUSE_S)

    failure_rate = failed / total_chunks if total_chunks else 0.0
    log.info(
        "summary: chunks=%s failed=%s skipped_existing=%s failure_rate=%.4f total_rows=%s",
        total_chunks,
        failed,
        skipped,
        failure_rate,
        total_rows,
    )
    if failure_rate > _MAX_FAILURE_RATE:
        log.error(
            "Failure rate %.2f%% exceeds %.2f%% — aborting concatenation.",
            failure_rate * 100,
            _MAX_FAILURE_RATE * 100,
        )
        return 2

    missing: list[str] = []
    for d0, d1 in chunks:
        p = _chunk_path(d0.year, d0, d1)
        if not p.is_file() or p.stat().st_size == 0:
            missing.append(f"{d0}_{d1}")
    if missing:
        log.error(
            "Missing %s chunk file(s); will not build combined CSV.",
            len(missing),
        )
        return 4

    # Concatenate all chunk CSVs in chronological order (one header).
    archive_root = _ROOT / "data" / "firms_archive"
    combined = _ROOT / "data" / "firms_archive" / "AU_VIIRS_2012_to_present.csv"
    combined.parent.mkdir(parents=True, exist_ok=True)
    all_files: list[Path] = []
    for year_dir in sorted(archive_root.glob("*")):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        all_files.extend(sorted(year_dir.glob("*.csv")))
    # Exclude the combined artifact if re-run
    all_files = [p for p in all_files if p.name != "AU_VIIRS_2012_to_present.csv"]
    all_files.sort(key=lambda p: p.name)

    if not all_files:
        log.error("No per-chunk CSV files found under data/firms_archive/")
        return 3

    first = True
    with combined.open("w", encoding="utf-8", newline="") as out:
        for fp in all_files:
            text = fp.read_text(encoding="utf-8", errors="replace")
            lines = text.splitlines()
            if not lines:
                continue
            if first:
                out.write("\n".join(lines))
                if lines:
                    out.write("\n")
                first = False
            else:
                # drop header row on subsequent files
                rest = lines[1:] if len(lines) > 1 else []
                if rest:
                    out.write("\n".join(rest))
                    out.write("\n")

    log.info("wrote combined CSV: %s", combined)
    return 0


if __name__ == "__main__":
    sys.exit(main())
