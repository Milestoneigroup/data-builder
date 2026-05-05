"""One-off ingest of Tasmanian liquor licences from treasury.tas.gov.au into Supabase.

Run: ``python -m scrapers.tas_liquor_license_ingest``

Discovers the latest "Tasmanian Liquor Licences as at …" XLSX from the Treasury landing page,
downloads it (≤1 req/s to treasury.tas.gov.au, ≤4 logical GETs including retries),
and upserts into ``shared.ref_liquor_licenses`` on ``(state_code, license_number)``.
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
TMP_DIR = _ROOT / ".tmp_tas_liquor"

LOG = logging.getLogger("tas_liquor_license_ingest")

LANDING_PAGE_URL = (
    "https://www.treasury.tas.gov.au/"
    "liquor-and-gaming/liquor/community-information/liquor-licence-data"
)
USER_AGENT = "Milestone-Innovations-Group/1.0 (data-builder; richard@milestoneigroup.com)"
MAX_HTTP_REQUESTS = 4
PAGE_ATTEMPTS = 2
DOWNLOAD_ATTEMPTS = 2
HTTP_DELAY_S = 1.0
UPLOAD_BATCH = 500

SOURCE_DATASET = "treasury.tas.gov.au_liquor_licences"

_ANCHOR_PHRASE = re.compile(r"Tasmanian\s+Liquor\s+Licences\s+as\s+at", re.I)
_FILENAME_DATE = re.compile(
    r"Tasmanian\s+Liquor\s+Licences\s+as\s+at\s+(.+?)\.xlsx", re.I
)

_LICENCE_NUM_PATTERNS = (
    re.compile(r"^licen[cs]e?\s*number$", re.I),
    re.compile(r"^licen[cs]e?\s*no\.?$", re.I),
    re.compile(r"^licencenumber$", re.I),
    re.compile(r"^license\s*number$", re.I),
)


def load_env() -> None:
    """Load ``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY`` from repo-root ``env.local``."""
    load_dotenv(_ROOT / "env.local", override=True)


class RequestBudget:
    """At most ``cap`` logical GETs; at least ``HTTP_DELAY_S`` between request end and next start."""

    def __init__(self, cap: int) -> None:
        self._cap = cap
        self.total = 0
        self._last_end: float | None = None

    def _throttle(self) -> None:
        if self._last_end is None:
            return
        wait = HTTP_DELAY_S - (time.perf_counter() - self._last_end)
        if wait > 0:
            time.sleep(wait)

    def before_request(self) -> None:
        if self.total >= self._cap:
            raise RuntimeError(
                f"HTTP request budget exhausted (cap={self._cap}, used={self.total})."
            )
        self._throttle()

    def record_request_started(self) -> None:
        self.total += 1
        if self.total > self._cap:
            raise RuntimeError("HTTP request counter overshot cap (logic error).")

    def mark_request_end(self) -> None:
        self._last_end = time.perf_counter()


def _normalise_header(cell: Any) -> str:
    return str(cell).strip()


def _headers_list_match_licence_number(headers: list[str]) -> bool:
    lowered = [_normalise_header(h).lower() for h in headers]
    for h in lowered:
        for pat in _LICENCE_NUM_PATTERNS:
            if pat.match(h):
                return True
        if "licence" in h and "number" in h:
            return True
        if "license" in h and "number" in h:
            return True
    return False


def _detect_header_row(path: Path, sheet_name: str) -> int:
    preview = pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=None,
        dtype=str,
        keep_default_na=False,
        engine="openpyxl",
        nrows=40,
    )
    for i in range(len(preview)):
        headers = [_normalise_header(x) for x in preview.iloc[i].tolist()]
        if _headers_list_match_licence_number(headers):
            return i
    raise ValueError(
        f"Could not find a header row containing a licence number column in "
        f"sheet {sheet_name!r} of {path.name}."
    )


def _snapshot_date_from_filename(filename: str) -> date:
    m = _FILENAME_DATE.search(filename)
    if not m:
        raise ValueError(f"Could not parse snapshot date from filename: {filename!r}")
    try:
        from dateutil import parser as dateutil_parser

        dt = dateutil_parser.parse(m.group(1), dayfirst=True, fuzzy=False)
        return dt.date()
    except (ValueError, TypeError) as e:
        raise ValueError(f"Unparseable date in filename {filename!r}") from e


def find_latest_xlsx_url(
    client: httpx.Client, budget: RequestBudget
) -> tuple[str, date, str]:
    """Return absolute download URL, snapshot date, and filename (with ``.xlsx``)."""
    last_err: BaseException | None = None
    html_text = ""
    for attempt in range(1, PAGE_ATTEMPTS + 1):
        budget.before_request()
        try:
            r = client.get(LANDING_PAGE_URL)
            budget.record_request_started()
            r.raise_for_status()
            html_text = r.text
            budget.mark_request_end()
            last_err = None
            break
        except (httpx.HTTPError, OSError) as e:
            last_err = e
            budget.mark_request_end()
            LOG.warning("Landing page fetch attempt %s/%s failed: %s", attempt, PAGE_ATTEMPTS, e)
    if last_err is not None:
        raise RuntimeError("Landing page fetch failed") from last_err

    soup = BeautifulSoup(html_text, "lxml")
    candidates: list[tuple[str, str, str]] = []
    for a in soup.find_all("a", href=True):
        href_raw = str(a.get("href") or "").strip()
        if not href_raw.lower().endswith(".xlsx"):
            continue
        text = re.sub(r"\s+", " ", (a.get_text() or "").strip())
        slug = href_raw.split("/")[-1]
        slug_decoded = re.sub(r"%20", " ", slug)
        if not (
            _ANCHOR_PHRASE.search(text)
            or _ANCHOR_PHRASE.search(slug_decoded)
            or _ANCHOR_PHRASE.search(href_raw)
        ):
            continue
        abs_url = urljoin(LANDING_PAGE_URL, href_raw)
        filename = slug_decoded if slug_decoded.lower().endswith(".xlsx") else f"{slug_decoded}.xlsx"
        if not filename.lower().endswith(".xlsx"):
            filename = f"{filename}.xlsx"
        candidates.append((abs_url, filename, text or slug_decoded))

    if not candidates:
        raise RuntimeError(
            "No XLSX link matched 'Tasmanian Liquor Licences as at …' on the landing page."
        )

    best: tuple[str, date, str] | None = None
    best_key: tuple | None = None
    for url, fname, _label in candidates:
        snap = _snapshot_date_from_filename(fname)
        key = (snap, fname)
        if best_key is None or key > best_key:
            best_key = key
            best = (url, snap, fname)
    assert best is not None
    return best


def download_xlsx(client: httpx.Client, budget: RequestBudget, url: str, filename: str) -> Path:
    """Stream workbook into ``.tmp_tas_liquor/`` and return path."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w.\-]+", "_", filename)[:180]
    out = TMP_DIR / safe_name

    last_err: BaseException | None = None
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        budget.before_request()
        try:
            with client.stream("GET", url, follow_redirects=True) as r:
                budget.record_request_started()
                r.raise_for_status()
                cl = r.headers.get("content-length")
                print(
                    json.dumps(
                        {
                            "download_started": {
                                "content_length_bytes": int(cl) if cl and str(cl).isdigit() else None,
                                "content_length_header": cl,
                                "status_code": r.status_code,
                            }
                        },
                        ensure_ascii=False,
                    )
                )
                with out.open("wb") as fh:
                    for chunk in r.iter_bytes(chunk_size=256 * 1024):
                        fh.write(chunk)
            budget.mark_request_end()
            last_err = None
            break
        except httpx.HTTPError as e:
            last_err = e
            budget.mark_request_end()
            LOG.warning("Download attempt %s/%s failed: %s", attempt, DOWNLOAD_ATTEMPTS, e)
            if out.exists():
                try:
                    out.unlink()
                except OSError:
                    pass
    if last_err is not None:
        raise RuntimeError("XLSX download failed") from last_err
    return out


def _first_matching_column(
    norm_to_original: dict[str, str], aliases: tuple[str, ...]
) -> str | None:
    for alias in aliases:
        key = alias.strip().lower().replace(" ", "_")
        if key in norm_to_original:
            return norm_to_original[key]
    return None


def _normalise_key(h: str) -> str:
    s = str(h).strip().lower()
    s = re.sub(r"\s+", "_", s)
    return s


def discover_columns(path: Path) -> dict[str, Any]:
    """Print workbook structure to stdout and return mapping metadata for ``parse_rows``."""
    xf = pd.ExcelFile(path, engine="openpyxl")
    sheet_names = xf.sheet_names
    print("Sheet names:", sheet_names)

    per_sheet: list[dict[str, Any]] = []
    for sn in sheet_names:
        hdr_row = _detect_header_row(path, sn)
        df = pd.read_excel(
            path,
            sheet_name=sn,
            header=hdr_row,
            dtype=str,
            keep_default_na=False,
            engine="openpyxl",
        )
        df.columns = [_normalise_header(c) for c in df.columns]
        norm_to_original = {_normalise_key(c): c for c in df.columns}

        row_count = len(df)
        print(f"\n--- Sheet {sn!r} ---")
        print(f"Header row index (0-based): {hdr_row}")
        print(f"Headers: {list(df.columns)}")
        print(f"Row count (data rows): {row_count}")
        sample = df.head(3)
        print("First 3 sample rows:")
        print(sample.to_string())

        if row_count == 0:
            raise RuntimeError(f"Sheet {sn!r} has zero data rows; aborting.")

        if not _headers_list_match_licence_number(list(df.columns)):
            raise RuntimeError(
                f"Sheet {sn!r} has no recognisable licence number column after header detection."
            )

        lic_num_col = _first_matching_column(
            norm_to_original,
            (
                "licencenumber",
                "licence number",
                "license number",
                "licence no",
                "licence no.",
            ),
        )
        if not lic_num_col:
            raise RuntimeError(f"Could not map licence number column on sheet {sn!r}.")

        licensee_single = _first_matching_column(
            norm_to_original,
            (
                "licensee",
                "licence holder",
                "license holder",
                "licensee legal name",
            ),
        )

        type_col = _first_matching_column(
            norm_to_original,
            (
                "licencetypecategory",
                "licence type",
                "license type",
                "licence type category",
                "type",
                "category",
            ),
        )
        subcat_col = _first_matching_column(
            norm_to_original,
            (
                "licencetypesubcategory",
                "subcategory",
                "licence subcategory",
            ),
        )
        trading_col = _first_matching_column(
            norm_to_original,
            (
                "premisesname",
                "premises name",
                "trading name",
                "trading as",
                "name",
            ),
        )
        line1 = _first_matching_column(
            norm_to_original, ("premiseaddress_line1", "address line 1", "address")
        )
        line2 = _first_matching_column(norm_to_original, ("premiseaddress_line2", "address line 2"))
        line3 = _first_matching_column(norm_to_original, ("premiseaddress_line3", "address line 3"))
        suburb_col = _first_matching_column(
            norm_to_original,
            ("premiseaddress_suburb", "suburb", "town", "locality"),
        )
        post_col = _first_matching_column(
            norm_to_original,
            ("premiseaddress_postcode", "postcode", "post code"),
        )
        lga_col = _first_matching_column(
            norm_to_original,
            (
                "premise_municipality",
                "municipality",
                "council",
                "lga",
            ),
        )

        title_c = norm_to_original.get("title")
        initials_c = norm_to_original.get("initials")
        first_c = norm_to_original.get("firstname")
        sur_c = norm_to_original.get("surname")

        per_sheet.append(
            {
                "sheet_name": sn,
                "header_row": hdr_row,
                "license_number_column": lic_num_col,
                "licensee_single_column": licensee_single,
                "licensee_title_column": title_c,
                "licensee_initials_column": initials_c,
                "licensee_firstname_column": first_c,
                "licensee_surname_column": sur_c,
                "license_type_column": type_col,
                "license_subcategory_column": subcat_col,
                "trading_name_column": trading_col,
                "address_line1_column": line1,
                "address_line2_column": line2,
                "address_line3_column": line3,
                "suburb_column": suburb_col,
                "postcode_column": post_col,
                "lga_column": lga_col,
            }
        )

    mapping = {"workbook_path": str(path), "sheets": per_sheet}
    print("\n--- Column mapping (source header -> usage) ---")
    for spec in per_sheet:
        print(f"  [{spec['sheet_name']}]")
        print(f"    license_number <- {spec['license_number_column']}")
        if spec["licensee_single_column"]:
            print(f"    licensee_legal_name <- {spec['licensee_single_column']}")
        else:
            print(
                "    licensee_legal_name <- composite: "
                f"{spec['licensee_title_column']}, {spec['licensee_initials_column']}, "
                f"{spec['licensee_firstname_column']}, {spec['licensee_surname_column']}"
            )
        print(f"    license_type <- {spec['license_type_column']}")
        print(f"    authorisations (subcategory) <- {spec['license_subcategory_column']}")
        print(f"    trading_name <- {spec['trading_name_column']}")
        print(
            "    premises_address <- join: "
            f"{spec['address_line1_column']}, {spec['address_line2_column']}, "
            f"{spec['address_line3_column']}"
        )
        print(f"    suburb <- {spec['suburb_column']}")
        print(f"    postcode <- {spec['postcode_column']}")
        print(f"    lga <- {spec['lga_column']}")
    return mapping


def _strip_cell(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip()


def _nullable_text(s: str) -> str | None:
    s = s.strip()
    return None if s == "" else s


def _build_licensee_legal_name(row: dict[str, str], spec: dict[str, Any]) -> str:
    single = spec.get("licensee_single_column")
    if single:
        return _strip_cell(row.get(single, ""))

    parts: list[str] = []
    for key in (
        "licensee_title_column",
        "licensee_initials_column",
        "licensee_firstname_column",
        "licensee_surname_column",
    ):
        col = spec.get(key)
        if col:
            p = _strip_cell(row.get(col, ""))
            if p:
                parts.append(p)
    return " ".join(parts)


def _build_premises_address(row: dict[str, str], spec: dict[str, Any]) -> str:
    lines: list[str] = []
    for key in ("address_line1_column", "address_line2_column", "address_line3_column"):
        col = spec.get(key)
        if not col:
            continue
        s = _strip_cell(row.get(col, ""))
        if s:
            lines.append(s)
    return ", ".join(lines)


def _license_category_only(row: dict[str, str], spec: dict[str, Any]) -> str | None:
    """Treasury category (e.g. General, Special) for ``license_type``; subcategory goes to ``authorisations``."""
    cat_col = spec.get("license_type_column")
    if not cat_col:
        return None
    cat = _strip_cell(row.get(cat_col, ""))
    return cat or None


def parse_rows(
    path: Path,
    mapping: dict[str, Any],
    source_resource_name: str,
    source_snapshot_date: date,
) -> Iterator[dict[str, Any]]:
    """Yield Supabase-shaped dicts for each spreadsheet row."""
    for spec in mapping["sheets"]:
        sn = spec["sheet_name"]
        hdr_row = spec["header_row"]
        df = pd.read_excel(
            path,
            sheet_name=sn,
            header=hdr_row,
            dtype=str,
            keep_default_na=False,
            engine="openpyxl",
        )
        df.columns = [_normalise_header(c) for c in df.columns]

        lic_col = spec["license_number_column"]
        snap_str = source_snapshot_date.isoformat()

        for _, ser in df.iterrows():
            raw_cells: dict[str, str] = {str(k): _strip_cell(v) for k, v in ser.items()}
            raw_cells["sheet_name"] = sn

            lic_no = _strip_cell(raw_cells.get(lic_col, ""))
            if not lic_no:
                continue

            licensee = _build_licensee_legal_name(raw_cells, spec)
            if not licensee:
                continue

            premises_address = _build_premises_address(raw_cells, spec)
            trading_col = spec.get("trading_name_column")
            trading = _strip_cell(raw_cells.get(trading_col or "", "")) if trading_col else ""

            sub_col = spec.get("license_subcategory_column")
            sub_val = _strip_cell(raw_cells.get(sub_col or "", "")) if sub_col else ""

            row: dict[str, Any] = {
                "state_code": "TAS",
                "license_number": lic_no,
                "license_type": _license_category_only(raw_cells, spec),
                "license_class": None,
                "license_status": "Current",
                "trading_status": None,
                "trading_name": _nullable_text(trading),
                "licensee_legal_name": licensee,
                "licensee_type": None,
                "abn_from_register": None,
                "acn_from_register": None,
                "premises_address": _nullable_text(premises_address),
                "suburb": _nullable_text(
                    _strip_cell(raw_cells.get(spec.get("suburb_column") or "", ""))
                ),
                "postcode": _nullable_text(
                    _strip_cell(raw_cells.get(spec.get("postcode_column") or "", ""))
                ),
                "premises_state": "TAS",
                "lat": None,
                "lng": None,
                "lga": _nullable_text(
                    _strip_cell(raw_cells.get(spec.get("lga_column") or "", ""))
                ),
                "region": None,
                "authorisations": _nullable_text(sub_val),
                "source_dataset": SOURCE_DATASET,
                "source_resource_id": None,
                "source_resource_name": source_resource_name,
                "source_snapshot_date": snap_str,
                "raw_data": raw_cells,
            }
            yield row


def _dedupe_conflict_keys(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        key = (str(r["state_code"]), str(r["license_number"]))
        merged[key] = r
    return list(merged.values())


def upsert_batch(client: Any, rows: list[dict[str, Any]], batch_size: int = UPLOAD_BATCH) -> None:
    if not rows:
        return
    rows = _dedupe_conflict_keys(rows)
    for start in range(0, len(rows), batch_size):
        chunk = rows[start : start + batch_size]
        for rec in chunk:
            rd = rec.get("raw_data")
            if isinstance(rd, dict):
                rec["raw_data"] = json.loads(json.dumps(rd, default=str))
        (
            client.schema("shared")
            .table("ref_liquor_licenses")
            .upsert(chunk, on_conflict="state_code,license_number")
            .execute()
        )


def main() -> None:
    import os

    load_env()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    t0 = time.perf_counter()
    url_env = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url_env or not key:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env.local")

    from supabase import create_client

    sb = create_client(url_env, key)

    budget = RequestBudget(MAX_HTTP_REQUESTS)
    headers = {"User-Agent": USER_AGENT}

    with httpx.Client(
        headers=headers,
        timeout=httpx.Timeout(180.0, connect=30.0),
        follow_redirects=True,
    ) as http:
        xlsx_url, snapshot_date, fname = find_latest_xlsx_url(http, budget)

        print(
            json.dumps(
                {
                    "chosen_xlsx_url": xlsx_url,
                    "snapshot_date": snapshot_date.isoformat(),
                    "filename": fname,
                    "message": (
                        "Response Content-Length (if present) is printed when the download starts, "
                        "before the response body is written to disk."
                    ),
                },
                indent=2,
                ensure_ascii=False,
            )
        )

        local_path = download_xlsx(http, budget, xlsx_url, fname)

    disc = discover_columns(local_path)
    _ = disc

    source_resource_name = fname[: -len(".xlsx")] if fname.lower().endswith(".xlsx") else fname

    total = 0
    batch: list[dict[str, Any]] = []
    for row in parse_rows(local_path, disc, source_resource_name, snapshot_date):
        batch.append(row)
        if len(batch) >= UPLOAD_BATCH:
            upsert_batch(sb, batch)
            total += len(batch)
            batch.clear()
    if batch:
        upsert_batch(sb, batch)
        total += len(batch)

    elapsed = time.perf_counter() - t0
    LOG.info(
        "Done. rows_upserted=%s http_requests_counted=%s elapsed_s=%.2f",
        total,
        budget.total,
        elapsed,
    )
    print(f"rows_upserted={total} http_requests_counted={budget.total} elapsed_s={elapsed:.2f}")


if __name__ == "__main__":
    main()
