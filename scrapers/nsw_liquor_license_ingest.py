"""One-off ingest of NSW liquor licence premises from data.nsw.gov.au into Supabase.

Run: ``python -m scrapers.nsw_liquor_license_ingest``

Discovers the latest monthly CSV via CKAN ``package_show``, downloads it (comma- or tab-separated —
sniffed from the header), and upserts into ``shared.ref_liquor_licenses`` on
``(state_code, license_number)``.
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]

LOG = logging.getLogger("nsw_liquor_license_ingest")

PACKAGE_SHOW_URL = "https://data.nsw.gov.au/data/api/3/action/package_show?id=liquor-licence-premises-list"
USER_AGENT = "Milestone-Innovations-Group/1.0 (data-builder; richard@milestoneigroup.com)"
MAX_HTTP_REQUESTS = 5
# Initial attempt plus up to three retries (four GETs max per phase if needed).
HTTP_RETRIES = 4
UPLOAD_BATCH = 500
PROGRESS_EVERY = 1000

SOURCE_DATASET = "data.nsw.gov.au_liquor_premises"

_PREMISES_LIST_NAME = re.compile(r"premises.list", re.IGNORECASE)


@dataclass(frozen=True)
class ChosenResource:
    """CKAN resource metadata for the snapshot we ingest."""

    resource_id: str
    resource_name: str
    download_url: str
    last_modified_raw: str | None
    created_raw: str | None


def load_env() -> None:
    """Load ``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY`` from repo-root ``env.local``."""
    load_dotenv(_ROOT / "env.local", override=True)


class RequestBudget:
    """Track logical GET calls (each client ``get`` / ``stream`` counts once; redirect hops do not)."""

    def __init__(self, cap: int) -> None:
        self._cap = cap
        self.total = 0
        self._last_end: float | None = None

    def _throttle(self) -> None:
        """At most one request per second to data.nsw.gov.au."""
        if self._last_end is None:
            return
        wait = 1.0 - (time.perf_counter() - self._last_end)
        if wait > 0:
            time.sleep(wait)

    def record_top_level_request(self) -> None:
        """Count one logical GET (initial URL only; redirects are not double-counted)."""
        self.total += 1
        if self.total > self._cap:
            raise RuntimeError("HTTP request counter overshot cap (logic error).")

    def mark_request_end(self) -> None:
        self._last_end = time.perf_counter()

    def before_request(self) -> None:
        if self.total >= self._cap:
            raise RuntimeError(
                f"HTTP request budget exhausted (cap={self._cap}, used={self.total})."
            )
        self._throttle()


def _parse_http_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _year_month_from_name(name: str) -> tuple[int, int]:
    """Best-effort (year, month) from resource title for tie-breaking."""
    try:
        from dateutil import parser as dateutil_parser

        dt = dateutil_parser.parse(name, fuzzy=True, default=datetime(1900, 1, 1, tzinfo=UTC))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        if dt.year >= 1990:
            return (dt.year, dt.month)
    except Exception:
        # dateutil fuzzy parse raises assorted parser errors across versions.
        pass
    years = [int(m) for m in re.findall(r"\b(20\d{2})\b", name)]
    if years:
        return (max(years), 0)
    return (0, 0)


def _resource_sort_key(res: dict[str, Any]) -> tuple:
    ts = _parse_http_datetime(res.get("last_modified")) or _parse_http_datetime(res.get("created"))
    if ts is None:
        ts = datetime.min.replace(tzinfo=UTC)
    ym = _year_month_from_name(str(res.get("name") or ""))
    latest_boost = 1 if "latest" in str(res.get("name") or "").lower() else 0
    return (ts, ym[0], ym[1], latest_boost)


def find_latest_resource(
    client: httpx.Client,
    budget: RequestBudget,
) -> ChosenResource:
    """Pick the newest eligible premises-list CSV resource from CKAN (no hard-coded IDs)."""
    last_err: Exception | None = None
    data: dict[str, Any] | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        budget.before_request()
        try:
            r = client.get(PACKAGE_SHOW_URL)
            budget.record_top_level_request()
            budget.mark_request_end()
            r.raise_for_status()
            data = r.json()
            break
        except (httpx.HTTPError, json.JSONDecodeError) as e:
            last_err = e
            budget.mark_request_end()
            LOG.warning("package_show attempt %s/%s failed: %s", attempt, HTTP_RETRIES, e)
            time.sleep(min(2**attempt, 10))
    if data is None:
        raise RuntimeError(f"package_show failed after {HTTP_RETRIES} attempts") from last_err

    if not data.get("success"):
        raise RuntimeError(f"CKAN package_show unsuccessful: {data!s}")

    pkg = data.get("result") or {}
    resources: list[dict[str, Any]] = list(pkg.get("resources") or [])

    candidates: list[dict[str, Any]] = []
    for res in resources:
        fmt = str(res.get("format") or "")
        name = str(res.get("name") or "")
        url = str(res.get("url") or "").strip()
        if not fmt or fmt.upper() != "CSV":
            continue
        if not _PREMISES_LIST_NAME.search(name):
            continue
        if not url.lower().endswith(".csv"):
            continue
        candidates.append(res)

    if not candidates:
        raise RuntimeError("No CSV resources matched premises list filters.")

    candidates.sort(key=_resource_sort_key, reverse=True)
    top_ts = _resource_sort_key(candidates[0])[0]
    tied = [c for c in candidates if _resource_sort_key(c)[0] == top_ts]
    tied.sort(
        key=lambda r: (
            _year_month_from_name(str(r.get("name") or "")),
            1 if "latest" in str(r.get("name") or "").lower() else 0,
        ),
        reverse=True,
    )
    chosen = tied[0]

    rid = str(chosen.get("id") or "")
    if not rid:
        raise RuntimeError("Chosen CKAN resource has no id.")
    url_out = str(chosen.get("url") or "").strip()
    name_out = str(chosen.get("name") or "")

    return ChosenResource(
        resource_id=rid,
        resource_name=name_out,
        download_url=url_out,
        last_modified_raw=chosen.get("last_modified"),
        created_raw=chosen.get("created"),
    )


def _snapshot_date_for_resource(res: dict[str, Any]) -> str:
    """``YYYY-MM-DD`` from CKAN timestamps, falling back to the resource name."""
    for key in ("last_modified", "created"):
        raw = res.get(key)
        if not raw:
            continue
        dt = _parse_http_datetime(str(raw))
        if dt:
            return dt.date().isoformat()
    ym = _year_month_from_name(str(res.get("name") or ""))
    if ym[0] >= 1990 and ym[1] > 0:
        return f"{ym[0]:04d}-{ym[1]:02d}-01"
    raise RuntimeError("Could not derive source_snapshot_date from CKAN resource.")


def download_csv(client: httpx.Client, budget: RequestBudget, url: str, resource_id: str) -> Path:
    """Stream the snapshot to ``.tmp_nsw_liquor/<resource_id>.tsv``."""
    tmp = _ROOT / ".tmp_nsw_liquor"
    tmp.mkdir(parents=True, exist_ok=True)
    out = tmp / f"{resource_id}.tsv"

    last_err: Exception | None = None
    for attempt in range(1, HTTP_RETRIES + 1):
        budget.before_request()
        try:
            with client.stream("GET", url) as r:
                budget.record_top_level_request()
                r.raise_for_status()
                with out.open("wb") as fh:
                    for chunk in r.iter_bytes(chunk_size=1024 * 256):
                        fh.write(chunk)
            budget.mark_request_end()
            last_err = None
            break
        except httpx.HTTPError as e:
            last_err = e
            budget.mark_request_end()
            LOG.warning("download attempt %s/%s failed: %s", attempt, HTTP_RETRIES, e)
            time.sleep(min(2**attempt, 10))
        if out.exists():
            try:
                out.unlink()
            except OSError:
                pass

    if last_err is not None:
        raise RuntimeError(f"Download failed after {HTTP_RETRIES} attempts") from last_err

    return out


def _nullable_text(cell: str) -> str | None:
    s = (cell or "").strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None
    return s


def _nullable_float(cell: str) -> float | None:
    s = (cell or "").strip()
    if s == "" or s.lower() in {"nan", "-nan", "none", "null"}:
        return None
    try:
        v = float(s)
    except ValueError:
        return None

    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _detect_sep(path: Path) -> str:
    """NSW publishes both tab- and comma-separated exports; sniff the first line."""
    with path.open(encoding="utf-8", errors="replace") as fh:
        first = fh.readline()
    tabs = first.count("\t")
    commas = first.count(",")
    if tabs > commas:
        return "\t"
    if commas > tabs:
        return ","
    return "\t"


def parse_rows(
    path: Path,
    meta: ChosenResource,
    source_snapshot_date: str,
) -> Iterator[dict[str, Any]]:
    """Parse rows and yield Supabase-shaped dicts (TSV or CSV, depending on the snapshot)."""
    sep = _detect_sep(path)
    LOG.info("Detected delimiter %r for snapshot file", sep)
    kwargs: dict[str, Any] = {
        "sep": sep,
        "dtype": str,
        "na_values": [""],
        "keep_default_na": False,
        "chunksize": 10_000,
        "encoding": "utf-8",
        "encoding_errors": "replace",
    }

    for chunk in pd.read_csv(path, **kwargs):
        chunk.columns = [str(c).strip() for c in chunk.columns]
        chunk = chunk.apply(lambda s: s.str.strip() if s.dtype == "object" else s)

        for raw in chunk.to_dict(orient="records"):
            norm: dict[str, str] = {}
            for k, v in raw.items():
                if k is None:
                    continue
                norm[str(k).strip()] = v if isinstance(v, str) else str(v)

            licensee = (norm.get("Licensee") or "").strip()
            lic_no = (norm.get("Licence number") or "").strip()
            if not lic_no:
                continue
            if not licensee:
                continue

            row: dict[str, Any] = {
                "state_code": "NSW",
                "license_number": lic_no,
                "license_type": _nullable_text(norm.get("Licence type") or ""),
                "license_status": _nullable_text(norm.get("Status") or ""),
                "trading_status": _nullable_text(norm.get("Trading Status") or ""),
                "trading_name": _nullable_text(norm.get("Licence name") or ""),
                "licensee_legal_name": licensee,
                "licensee_type": _nullable_text(norm.get("Licensee Type") or ""),
                "abn_from_register": _nullable_text(norm.get("Licensee ABN") or ""),
                "acn_from_register": _nullable_text(norm.get("Licensee ACN") or ""),
                "premises_address": _nullable_text(norm.get("Address") or ""),
                "suburb": _nullable_text(norm.get("Suburb") or ""),
                "postcode": _nullable_text(norm.get("Postcode") or ""),
                "premises_state": "NSW",
                "lat": _nullable_float(norm.get("Latitude") or ""),
                "lng": _nullable_float(norm.get("Longitude") or ""),
                "lga": _nullable_text(norm.get("LGA") or ""),
                "region": _nullable_text(norm.get("Region") or ""),
                "authorisations": _nullable_text(norm.get("Authorisation restriction name") or ""),
                "source_dataset": SOURCE_DATASET,
                "source_resource_id": meta.resource_id,
                "source_resource_name": meta.resource_name,
                "source_snapshot_date": source_snapshot_date,
                "raw_data": norm,
            }
            yield row


def _dedupe_conflict_keys(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Last row wins when the CSV repeats the same (state_code, licence number) in one batch."""
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (str(r["state_code"]), str(r["license_number"]))
        merged[key] = r
    return list(merged.values())


def upsert_batch(client: Any, rows: list[dict[str, Any]]) -> None:
    """Upsert one batch (caller ensures no duplicate ``(state_code, license_number)`` within ``rows``)."""
    if not rows:
        return
    (
        client.schema("shared")
        .table("ref_liquor_licenses")
        .upsert(rows, on_conflict="state_code,license_number")
        .execute()
    )


def main() -> None:
    import os

    load_env()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    t0 = time.perf_counter()
    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env.local")

    from supabase import create_client

    sb = create_client(url, key)

    headers = {"User-Agent": USER_AGENT}
    budget = RequestBudget(MAX_HTTP_REQUESTS)

    with httpx.Client(headers=headers, timeout=httpx.Timeout(120.0, connect=30.0), follow_redirects=True) as http:
        chosen = find_latest_resource(http, budget)
        source_snapshot_date = _snapshot_date_for_resource(
            {
                "name": chosen.resource_name,
                "last_modified": chosen.last_modified_raw,
                "created": chosen.created_raw,
            }
        )
        print(
            json.dumps(
                {
                    "resource_id": chosen.resource_id,
                    "resource_name": chosen.resource_name,
                    "last_modified": chosen.last_modified_raw,
                    "download_url": chosen.download_url,
                    "source_snapshot_date": source_snapshot_date,
                },
                indent=2,
                ensure_ascii=False,
            )
        )

        path = download_csv(http, budget, chosen.download_url, chosen.resource_id)

    unique_keys: set[tuple[str, str]] = set()
    source_rows = 0
    batch: list[dict[str, Any]] = []
    for row in parse_rows(path, chosen, source_snapshot_date):
        source_rows += 1
        batch.append(row)
        if len(batch) >= UPLOAD_BATCH:
            deduped = _dedupe_conflict_keys(batch)
            upsert_batch(sb, deduped)
            for r in deduped:
                unique_keys.add((str(r["state_code"]), str(r["license_number"])))
            if source_rows % PROGRESS_EVERY == 0:
                LOG.info(
                    "Parsed %s source rows (%s distinct NSW licence keys so far)",
                    source_rows,
                    len(unique_keys),
                )
            batch.clear()

    if batch:
        deduped = _dedupe_conflict_keys(batch)
        upsert_batch(sb, deduped)
        for r in deduped:
            unique_keys.add((str(r["state_code"]), str(r["license_number"])))

    n_unique = len(unique_keys)
    elapsed = time.perf_counter() - t0
    LOG.info(
        "Done. source_rows=%s distinct_nsw_licence_keys=%s http_requests_counted=%s elapsed_s=%.2f",
        source_rows,
        n_unique,
        budget.total,
        elapsed,
    )
    print(
        f"source_rows={source_rows} distinct_nsw_licence_keys={n_unique} "
        f"http_requests_counted={budget.total} elapsed_s={elapsed:.2f}"
    )


if __name__ == "__main__":
    main()
