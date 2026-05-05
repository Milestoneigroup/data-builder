"""Ingest Victorian liquor licences from discover.data.vic.gov.au into Supabase.

Discovers the latest monthly XLSX snapshots (ideally three CKAN resources: metropolitan,
regional, and no-address; if the catalogue only lists one consolidated file per month,
that file is fetched once and edition is derived from sheet columns).

Requires ``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY`` with
service role) in ``env.local``, ``.env.local``, or ``.env``.

Run: ``python -m scrapers.vic_liquor_license_ingest``
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from dotenv import load_dotenv

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

PACKAGE_SHOW_URL = (
    "https://discover.data.vic.gov.au/api/3/action/package_show?"
    "id=victorian-liquor-licences-by-location"
)
USER_AGENT = (
    "Milestone-Innovations-Group/1.0 (data-builder; richard@milestoneigroup.com)"
)
SOURCE_DATASET = "discover.data.vic.gov.au_liquor_licences"
HTTP_DELAY_S = 1.0
MAX_HTTP_REQUESTS = 8
TMP_DIR = _ROOT / ".tmp_vic_liquor"

LOG = logging.getLogger(__name__)
_http_requests_made = 0
_last_http_at: float | None = None

MONTHS = (
    ("january", 1),
    ("february", 2),
    ("march", 3),
    ("april", 4),
    ("may", 5),
    ("june", 6),
    ("july", 7),
    ("august", 8),
    ("september", 9),
    ("october", 10),
    ("november", 11),
    ("december", 12),
    ("jan", 1),
    ("feb", 2),
    ("mar", 3),
    ("apr", 4),
    ("jun", 6),
    ("jul", 7),
    ("aug", 8),
    ("sep", 9),
    ("oct", 10),
    ("nov", 11),
    ("dec", 12),
)


def load_env() -> None:
    """Load environment variables from standard data-builder env files."""
    for p in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if p.is_file():
            load_dotenv(p, override=True, encoding="utf-8")


def _rate_limited_http() -> None:
    """Respect a maximum of one HTTP request per second to discover.data.vic.gov.au."""
    global _last_http_at
    if _last_http_at is None:
        return
    elapsed = time.monotonic() - _last_http_at
    if elapsed < HTTP_DELAY_S:
        time.sleep(HTTP_DELAY_S - elapsed)


def _bump_http_count() -> None:
    global _http_requests_made, _last_http_at
    if _http_requests_made >= MAX_HTTP_REQUESTS:
        raise RuntimeError(
            f"HTTP request budget exceeded (cap {MAX_HTTP_REQUESTS}); aborting to stay polite."
        )
    _http_requests_made += 1
    _last_http_at = time.monotonic()


def _http_get_json(client: httpx.Client, url: str) -> dict[str, Any]:
    _rate_limited_http()
    _bump_http_count()
    r = client.get(url, follow_redirects=True, timeout=120.0)
    r.raise_for_status()
    return r.json()


def _http_download(client: httpx.Client, url: str, dest: Path) -> None:
    _rate_limited_http()
    _bump_http_count()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with client.stream("GET", url, follow_redirects=True, timeout=300.0) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_bytes():
                fh.write(chunk)


def _normalise_month_token(token: str) -> str:
    return token.strip().lower().rstrip(".")


def _parse_month_year_from_text(text: str) -> tuple[int, int] | None:
    t = text.strip()
    for word, month_num in MONTHS:
        m = re.search(rf"{re.escape(word)}\s+(\d{{4}})\b", t, flags=re.I)
        if m:
            return int(m.group(1)), month_num
        m = re.search(rf"\b(\d{{4}})\s+{re.escape(word)}\b", t, flags=re.I)
        if m:
            return int(m.group(1)), month_num
    return None


def _parse_resource_month_year(resource: dict[str, Any]) -> tuple[int, int] | None:
    name = str(resource.get("name") or "")
    ym = _parse_month_year_from_text(name)
    if ym:
        return ym
    lm = resource.get("last_modified") or resource.get("metadata_modified")
    if isinstance(lm, str):
        return _parse_month_year_from_text(lm)
    return None


def _edition_from_resource_name(name: str) -> str | None:
    n = name.lower()
    if "no address" in n or "no-address" in n:
        return "no_address"
    if "metropolitan" in n or "metro melbourne" in n or "melbourne metro" in n:
        return "metro"
    if "regional" in n and "metro" not in n:
        return "regional"
    if re.search(r"\bmetro\b", n) and "regional" not in n:
        return "metro"
    return None


def _snapshot_date(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}-01"


def find_latest_resources() -> list[dict[str, Any] | None]:
    """Return ``[metro, regional, no_address]`` CKAN resource dicts (see :func:`_select_ckan_resources`)."""
    chosen, _, _ = _select_ckan_resources()
    return chosen


def _select_ckan_resources() -> tuple[list[dict[str, Any] | None], str | None, bool]:
    """Return resource choices, snapshot_date (YYYY-MM-01), and whether CKAN uses one consolidated file per month.

    Each slot is a CKAN ``resource`` object (subset of fields preserved) or ``None`` if
    the three-file layout is incomplete *and* no consolidated fallback applies.

    When the catalogue only publishes one spreadsheet per month, the same resource dict
    is returned in all three slots and ``combined_mode`` is True (caller downloads once).
    """
    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(headers=headers) as client:
        data = _http_get_json(client, PACKAGE_SHOW_URL)
    if not data.get("success"):
        raise RuntimeError(f"CKAN package_show failed: {data}")
    resources: list[dict[str, Any]] = data["result"].get("resources") or []
    filtered: list[dict[str, Any]] = []
    for r in resources:
        fmt = str(r.get("format") or "").upper()
        if fmt not in {"XLSX", "XLS"}:
            continue
        ym = _parse_resource_month_year(r)
        if ym is None:
            LOG.warning("Skipping resource with unparseable month: %s", r.get("name"))
            continue
        year, month = ym
        edition = _edition_from_resource_name(str(r.get("name") or ""))
        filtered.append(
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "url": r.get("url"),
                "format": r.get("format"),
                "_year": year,
                "_month": month,
                "_edition": edition,
            }
        )

    if not filtered:
        raise RuntimeError("No XLSX/XLS resources with a parseable month were found.")

    by_month: dict[tuple[int, int], dict[str, dict[str, Any]]] = defaultdict(dict)
    singles: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for r in filtered:
        key = (r["_year"], r["_month"])
        ed = r["_edition"]
        if ed is not None:
            by_month[key][ed] = r
        else:
            singles[key].append(r)

    months_sorted = sorted({k for k in by_month} | {k for k in singles}, reverse=True)
    for y, m in months_sorted:
        editions = by_month.get((y, m), {})
        if len(editions) == 3 and all(k in editions for k in ("metro", "regional", "no_address")):
            snap = _snapshot_date(y, m)
            LOG.info("Using three CKAN files for %s-%02d", y, m)
            return [editions["metro"], editions["regional"], editions["no_address"]], snap, False

    for y, m in months_sorted:
        editions = by_month.get((y, m), {})
        if len(editions) >= 1 and len(editions) < 3:
            LOG.warning(
                "Month %04d-%02d has incomplete CKAN edition set (%s); trying older months.",
                y,
                m,
                list(editions),
            )

    latest = max(filtered, key=lambda r: (r["_year"], r["_month"], str(r.get("name") or "")))
    y, m = latest["_year"], latest["_month"]
    cand = singles.get((y, m), [])
    if len(cand) != 1:
        cand = [r for r in filtered if r["_year"] == y and r["_month"] == m]
        cand.sort(key=lambda r: str(r.get("name") or ""))
    if not cand:
        raise RuntimeError(f"No consolidated snapshot found for month {y}-{m:02d}.")
    pick = max(cand, key=lambda r: str(r.get("url") or ""))
    snap = _snapshot_date(y, m)
    LOG.warning(
        "CKAN lists one consolidated XLSX per month; using %s (%s) and deriving edition "
        "from sheet columns (Metro/Regional / Region).",
        pick.get("name"),
        pick.get("id"),
    )
    return [pick, pick, pick], snap, True


def download_xlsx(client: httpx.Client, url: str, resource_id: str) -> Path:
    """Stream an XLSX to ``.tmp_vic_liquor/<resource_id>.xlsx`` and return the path."""
    safe_id = re.sub(r"[^\w.-]+", "_", resource_id)[:120]
    dest = TMP_DIR / f"{safe_id}.xlsx"
    _http_download(client, url, dest)
    return dest


def _strip(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _find_header_row(path: Path) -> int:
    preview = pd.read_excel(
        path, sheet_name=0, header=None, dtype=str, keep_default_na=False, nrows=40
    )
    for i in range(len(preview)):
        cells = [_strip(x).lower() for x in preview.iloc[i].tolist()]
        if any(
            c in {"licence num", "licence no", "licence number", "license no"} for c in cells if c
        ):
            return i
    raise ValueError(f"Could not locate a header row with a licence number column in {path}")


def _get_col(row: dict[str, str], *aliases: str) -> str:
    for a in aliases:
        if a in row:
            return _strip(row[a])
    lowered = {k.lower(): k for k in row}
    for a in aliases:
        k = lowered.get(a.lower())
        if k is not None:
            return _strip(row[k])
    return ""


def _derive_display_region(sheet_row: dict[str, str], edition_label: str | None) -> str:
    if edition_label == "metropolitan":
        return "Metropolitan"
    if edition_label == "regional":
        return "Regional"
    if edition_label == "no_address":
        return "No address"
    mr = _get_col(sheet_row, "Metro/Regional", "Metro Regional").lower()
    region_val = _get_col(sheet_row, "Region")
    if mr == "metro":
        return "Metropolitan"
    if mr == "regional" and not region_val:
        return "No address"
    if mr == "regional":
        return "Regional"
    if not mr and not region_val:
        return "No address"
    return "Metropolitan"


def parse_rows(
    path: Path,
    edition_label: str | None,
    source_resource_id: str | None,
    source_resource_name: str | None,
    source_snapshot_date: str,
) -> Iterator[dict[str, Any]]:
    """Yield database-ready dicts for each licence row."""
    header_idx = _find_header_row(path)
    df = pd.read_excel(
        path,
        sheet_name=0,
        header=header_idx,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl",
    )
    df.columns = [_strip(c) for c in df.columns]

    for _, row in df.iterrows():
        sheet_row: dict[str, str] = {str(k): _strip(v) for k, v in row.items()}
        lic = _get_col(sheet_row, "Licence Num", "Licence No", "Licence Number")
        if not lic or not re.search(r"\d", lic):
            continue
        licensee = _get_col(sheet_row, "Licensee", "Licensee ")
        if not licensee:
            continue

        display_region = _derive_display_region(sheet_row, edition_label)
        premises_state = None if display_region == "No address" else "VIC"

        raw_payload = dict(sheet_row)
        raw_payload["_vic_edition"] = edition_label or "derived_from_columns"
        raw_payload["_display_region"] = display_region

        rec: dict[str, Any] = {
            "state_code": "VIC",
            "license_number": lic,
            "license_type": _get_col(sheet_row, "Licence Category", "Category")
            or None,
            "license_class": None,
            "license_status": "Current",
            "trading_status": None,
            "trading_name": _get_col(sheet_row, "Trading As") or None,
            "licensee_legal_name": licensee,
            "licensee_type": None,
            "abn_from_register": None,
            "acn_from_register": None,
            "premises_address": _get_col(sheet_row, "Address") or None,
            "suburb": _get_col(sheet_row, "Suburb") or None,
            "postcode": _get_col(sheet_row, "Postcode") or None,
            "premises_state": premises_state,
            "lat": None,
            "lng": None,
            "lga": _get_col(sheet_row, "Council Name", "Council") or None,
            "region": display_region,
            "authorisations": None,
            "source_dataset": SOURCE_DATASET,
            "source_resource_id": source_resource_id,
            "source_resource_name": source_resource_name,
            "source_snapshot_date": source_snapshot_date,
            "raw_data": raw_payload,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        yield rec


def upsert_batch(rows: list[dict[str, Any]], url: str, key: str, batch_size: int = 500) -> None:
    """Upsert licence rows on ``(state_code, license_number)``."""
    from supabase import create_client

    client = create_client(url, key)
    tbl = client.schema("shared").table("ref_liquor_licenses")
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        for rec in chunk:
            rd = rec.get("raw_data")
            if isinstance(rd, dict):
                rec["raw_data"] = json.loads(json.dumps(rd, default=str))
        tbl.upsert(chunk, on_conflict="state_code,license_number").execute()


def main() -> None:
    global _http_requests_made
    _http_requests_made = 0
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout
    )
    load_env()
    from data_builder.config import get_settings

    settings = get_settings()
    url = (settings.supabase_url or "").strip()
    key = (settings.supabase_service_role_key or "").strip()
    if not key:
        key = (getattr(settings, "supabase_key", None) or "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    chosen, snapshot_date, combined = _select_ckan_resources()

    labels_slot = ("metropolitan", "regional", "no_address")
    LOG.info("Chosen resources (print before download):")
    for slot, label in zip(chosen, labels_slot, strict=True):
        if slot is None:
            print(f"  [{label}] MISSING")
            continue
        print(
            f"  [{label}] id={slot.get('id')} name={slot.get('name')!r} "
            f"url={slot.get('url')} snapshot_date={snapshot_date}"
        )

    id_to_path: dict[str, Path] = {}
    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(headers=headers) as client:
        for slot in chosen:
            if slot is None:
                continue
            rid = str(slot.get("id") or "unknown")
            if rid in id_to_path:
                continue
            surl = str(slot.get("url") or "")
            if not surl:
                raise RuntimeError(f"Resource {rid} has no download URL.")
            id_to_path[rid] = download_xlsx(client, surl, rid)

    counts: dict[str, int] = defaultdict(int)
    total_rows = 0
    batch: list[dict[str, Any]] = []
    batch_size = 500

    seen_paths_handled: set[Path] = set()
    for slot, label in zip(chosen, labels_slot, strict=True):
        if slot is None:
            continue
        rid = str(slot.get("id") or "")
        path = id_to_path[rid]
        if combined and path in seen_paths_handled:
            continue
        seen_paths_handled.add(path)
        eff_label = None if combined else label
        edition_name = str(slot.get("name") or "")
        for rec in parse_rows(path, eff_label, rid, edition_name, snapshot_date):
            dr = str(rec.get("region") or "")
            counts[dr] += 1
            total_rows += 1
            batch.append(rec)
            if len(batch) >= batch_size:
                upsert_batch(batch, url, key, batch_size=batch_size)
                LOG.info("Upserted %s rows (running total %s)", len(batch), total_rows)
                batch.clear()

    if batch:
        upsert_batch(batch, url, key, batch_size=batch_size)
        LOG.info("Upserted final batch (%s rows)", len(batch))

    elapsed = time.perf_counter() - t0
    LOG.info("Done. Rows processed: %s | HTTP requests: %s | elapsed_s: %.1f", total_rows, _http_requests_made, elapsed)
    print("")
    print("--- vic_liquor_license_ingest summary ---")
    print(f"Snapshot date:           {snapshot_date}")
    print(f"Combined CKAN mode:      {combined}")
    print(f"Total rows upserted:     {total_rows}")
    for k in sorted(counts, key=lambda x: -counts[x]):
        print(f"  {k}: {counts[k]}")
    print(f"HTTP requests made:      {_http_requests_made}")
    print(f"Runtime (seconds):       {elapsed:.1f}")


if __name__ == "__main__":
    main()
