"""One-off ingest of South Australian liquor and gaming licences from data.sa.gov.au into Supabase.

Run: ``python -m scrapers.sa_liquor_license_ingest``

Fetches the latest eligible bulk snapshot (CKAN ``package_show``) plus the ``Licence Types``
reference workbook (same dataset) when the HTTP budget allows, then upserts into
``shared.ref_liquor_licenses`` on ``(state_code, license_number)``.

**Known limitation:** the CKAN bulk snapshot was last published in August 2019; live registers
sit behind a search portal and are out of scope here. ``source_snapshot_date`` reflects the
chosen CKAN resource so downstream jobs can treat rows as potentially stale.
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
from urllib.parse import unquote

import httpx
import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
TMP_DIR = _ROOT / ".tmp_sa_liquor"

LOG = logging.getLogger("sa_liquor_license_ingest")

PACKAGE_SHOW_URL = (
    "https://data.sa.gov.au/data/api/3/action/package_show?id=liquor-gaming-licences"
)
USER_AGENT = "Milestone-Innovations-Group/1.0 (data-builder; richard@milestoneigroup.com)"
HTTP_DELAY_S = 1.0
MAX_HTTP_REQUESTS = 3
UPLOAD_BATCH = 500

SOURCE_DATASET = "data.sa.gov.au_liquor_gaming_licences"

_LICENCE_NUM_PATTERNS = (
    re.compile(r"^licen[cs]e?\s*number$", re.I),
    re.compile(r"^licen[cs]e?\s*no\.?$", re.I),
    re.compile(r"^ln_licencenumber$", re.I),
    re.compile(r"^licencenumber$", re.I),
)


def load_env() -> None:
    """Load ``SUPABASE_URL`` and ``SUPABASE_SERVICE_ROLE_KEY`` from repo-root ``env.local``."""
    load_dotenv(_ROOT / "env.local", override=True)


class RequestBudget:
    """At most ``cap`` logical GETs to data.sa.gov.au; ≥ ``HTTP_DELAY_S`` between request end and next start."""

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


def _normalise_key(h: str) -> str:
    s = str(h).strip().lower()
    s = re.sub(r"\s+", "_", s)
    return s


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


def _parse_iso_date(s: str | None) -> date | None:
    if not s or not str(s).strip():
        return None
    raw = str(s).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw).date()
    except ValueError:
        return None


def _snapshot_date_from_resource(res: dict[str, Any]) -> date:
    url = str(res.get("url") or "")
    m = re.search(r"lglicences(\d{4})(\d{2})(\d{2})", url, re.I)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    lm = _parse_iso_date(res.get("last_modified"))
    if lm:
        return lm
    cr = _parse_iso_date(res.get("created"))
    if cr:
        return cr
    from dateutil import parser as dateutil_parser

    name = str(res.get("name") or "")
    dt = dateutil_parser.parse(name, dayfirst=False, fuzzy=True)
    return dt.date()


def _resource_datetime_key(res: dict[str, Any]) -> datetime:
    for key in ("last_modified", "created"):
        d = _parse_iso_date(res.get(key))
        if d:
            return datetime.combine(d, datetime.min.time())
    return datetime.min


def _eligible_bulk_resource(res: dict[str, Any]) -> bool:
    fmt = (res.get("format") or "").strip().lower()
    if fmt not in ("xlsx", "csv"):
        return False
    name = str(res.get("name") or "")
    nl = name.lower()
    if "licences" not in nl and "licenses" not in nl:
        return False
    if "licence types" in nl or "license types" in nl:
        return False
    if "meta data" in nl:
        return False
    url = (res.get("url") or "").strip()
    return bool(url)


def _licence_types_resource(resources: list[dict[str, Any]]) -> dict[str, Any] | None:
    for r in resources:
        name = (r.get("name") or "").strip().lower()
        if name == "licence types" or name == "license types":
            url = (r.get("url") or "").strip()
            if url:
                return r
    return None


def find_latest_resource(client: httpx.Client, budget: RequestBudget) -> dict[str, Any]:
    """Call CKAN ``package_show`` and return metadata for the main snapshot and types reference."""
    budget.before_request()
    r = client.get(PACKAGE_SHOW_URL)
    budget.record_request_started()
    r.raise_for_status()
    payload = r.json()
    budget.mark_request_end()

    if not payload.get("success"):
        raise RuntimeError(f"CKAN package_show failed: {payload!r}")

    result = payload["result"]
    resources: list[dict[str, Any]] = list(result.get("resources") or [])
    eligible = [x for x in resources if _eligible_bulk_resource(x)]
    if not eligible:
        raise RuntimeError("No eligible bulk licence resources found on CKAN.")

    eligible.sort(key=_resource_datetime_key, reverse=True)
    main = eligible[0]
    types_res = _licence_types_resource(resources)

    snap = _snapshot_date_from_resource(main)
    out = {
        "main": main,
        "types": types_res,
        "source_snapshot_date": snap,
    }
    print(
        json.dumps(
            {
                "chosen_main_resource": {
                    "resource_id": main.get("id"),
                    "name": main.get("name"),
                    "last_modified": main.get("last_modified"),
                    "created": main.get("created"),
                    "format": main.get("format"),
                    "url": main.get("url"),
                    "size_bytes_ckan": main.get("size"),
                    "source_snapshot_date": snap.isoformat(),
                },
                "types_resource": (
                    {
                        "resource_id": types_res.get("id"),
                        "name": types_res.get("name"),
                        "url": types_res.get("url"),
                    }
                    if types_res
                    else None
                ),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return out


def download_file(client: httpx.Client, budget: RequestBudget, url: str, filename: str) -> Path:
    """Stream a file into ``.tmp_sa_liquor/`` and return the saved path."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w.\-]+", "_", unquote(filename))[:180]
    out = TMP_DIR / safe_name

    budget.before_request()
    with client.stream("GET", url, follow_redirects=True) as resp:
        budget.record_request_started()
        resp.raise_for_status()
        cl = resp.headers.get("content-length")
        print(
            json.dumps(
                {
                    "download_started": {
                        "url": url,
                        "path": str(out),
                        "content_length_bytes": int(cl) if cl and str(cl).isdigit() else None,
                        "status_code": resp.status_code,
                    }
                },
                ensure_ascii=False,
            )
        )
        with out.open("wb") as fh:
            for chunk in resp.iter_bytes(chunk_size=256 * 1024):
                fh.write(chunk)
    budget.mark_request_end()
    print(
        json.dumps(
            {"download_finished": {"path": str(out), "bytes_on_disk": out.stat().st_size}},
            ensure_ascii=False,
        )
    )
    return out


def _first_matching_column(
    norm_to_original: dict[str, str], aliases: tuple[str, ...]
) -> str | None:
    for alias in aliases:
        key = _normalise_key(alias)
        if key in norm_to_original:
            return norm_to_original[key]
    return None


def load_prefix_type_labels(types_path: Path | None) -> dict[str, str]:
    """Map three-digit licence prefixes to the published licence type label."""
    if types_path is None or not types_path.exists():
        return {}
    df = pd.read_excel(types_path, dtype=str, keep_default_na=False, engine="openpyxl")
    df.columns = [_normalise_header(c) for c in df.columns]
    nmap = {_normalise_key(c): c for c in df.columns}
    type_col = _first_matching_column(
        nmap,
        ("licence type", "license type", "licence_type"),
    )
    prefix_col = _first_matching_column(
        nmap,
        ("licence prefix", "license prefix", "licence_prefix"),
    )
    if not type_col or not prefix_col:
        LOG.warning("Licence types workbook missing expected columns; prefix map empty.")
        return {}
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        pfx = _strip_cell(row.get(prefix_col, ""))
        label = _strip_cell(row.get(type_col, ""))
        if len(pfx) == 3 and label and pfx.isdigit():
            out[pfx] = label
    return out


def _strip_cell(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if s.upper() == "NULL":
        return ""
    return s


def _nullable_text(s: str) -> str | None:
    s = s.strip()
    return None if s == "" else s


def discover_columns(path: Path) -> dict[str, Any]:
    """Print workbook structure to stdout and return mapping metadata for ``parse_rows``."""
    if path.suffix.lower() == ".csv":
        df0 = pd.read_csv(path, dtype=str, keep_default_na=False, nrows=5)
        sheet_names = ["__csv__"]
        xf = None
    else:
        xf = pd.ExcelFile(path, engine="openpyxl")
        sheet_names = xf.sheet_names
    print("Sheet names:", sheet_names)

    per_sheet: list[dict[str, Any]] = []
    for sn in sheet_names:
        if path.suffix.lower() == ".csv":
            hdr_row = 0
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
        else:
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
                "licence number",
                "license number",
                "licence no",
                "licence no.",
                "lic no",
                "ln_licencenumber",
            ),
        )
        if not lic_num_col:
            raise RuntimeError(f"Could not map licence number column on sheet {sn!r}.")

        licensee_col = _first_matching_column(
            norm_to_original,
            (
                "licensee",
                "licence holder",
                "license holder",
                "holder name",
                "licensee legal name",
            ),
        )
        sa_pn_fallback = _first_matching_column(norm_to_original, ("pn_name",))
        if not licensee_col:
            if sa_pn_fallback:
                licensee_col = sa_pn_fallback
                print(
                    f"\n[Note] Sheet {sn!r}: no dedicated licensee column; using {licensee_col!r} "
                    "(SA CBS bulk extract does not publish a separate legal holder field)."
                )
            else:
                raise RuntimeError(
                    f"Sheet {sn!r}: no recognisable licensee / licence holder column "
                    f"(and no SA ``PN_Name`` fallback)."
                )

        type_col = _first_matching_column(
            norm_to_original,
            ("licence type", "license type", "type", "class", "liquor class"),
        )
        status_col = _first_matching_column(
            norm_to_original,
            ("status", "licence status", "license status", "liquor_status"),
        )
        trading_col = _first_matching_column(
            norm_to_original,
            (
                "trading name",
                "premises name",
                "trading as",
                "pn_name",
            ),
        )
        addr1 = _first_matching_column(
            norm_to_original,
            ("premises address", "premises address 1", "lic_premisesaddress1", "address", "street address"),
        )
        addr2 = _first_matching_column(
            norm_to_original,
            ("premises address 2", "lic_premisesaddress2", "address line 2"),
        )
        suburb_col = _first_matching_column(
            norm_to_original,
            ("suburb", "town", "locality", "lic_premisestown"),
        )
        post_col = _first_matching_column(
            norm_to_original,
            ("postcode", "post code", "lic_premisespostcode"),
        )
        lga_col = _first_matching_column(
            norm_to_original,
            ("lga", "council", "local government area"),
        )
        premises_state_col = _first_matching_column(
            norm_to_original,
            ("premises state", "lic_premisesstate", "state"),
        )

        per_sheet.append(
            {
                "sheet_name": sn,
                "header_row": hdr_row,
                "license_number_column": lic_num_col,
                "licensee_column": licensee_col,
                "license_type_column": type_col,
                "license_status_column": status_col,
                "trading_name_column": trading_col,
                "address_line1_column": addr1,
                "address_line2_column": addr2,
                "suburb_column": suburb_col,
                "postcode_column": post_col,
                "lga_column": lga_col,
                "premises_state_column": premises_state_col,
            }
        )

    mapping: dict[str, Any] = {"workbook_path": str(path), "sheets": per_sheet}
    print("\n--- Column mapping (source header -> usage) ---")
    for spec in per_sheet:
        print(f"  [{spec['sheet_name']}]")
        print(f"    licence_number <- {spec['license_number_column']}")
        print(f"    licensee_legal_name <- {spec['licensee_column']}")
        print(f"    licence_type (from sheet if present) <- {spec['license_type_column']}")
        print(f"    licence_status <- {spec['license_status_column']}")
        print(f"    trading_name <- {spec['trading_name_column']}")
        print(
            "    premises_address <- join: "
            f"{spec['address_line1_column']}, {spec['address_line2_column']}"
        )
        print(f"    suburb <- {spec['suburb_column']}")
        print(f"    postcode <- {spec['postcode_column']}")
        print(f"    lga <- {spec['lga_column']}")
        print(f"    premises_state (optional column) <- {spec['premises_state_column']}")
    print(
        "\nLicence type labels for rows without a type column are derived from the first three "
        "digits of the licence number using the ``Licence Types`` CKAN resource when downloaded."
    )
    return mapping


def _build_premises_address(row: dict[str, str], spec: dict[str, Any]) -> str:
    lines: list[str] = []
    for key in ("address_line1_column", "address_line2_column"):
        col = spec.get(key)
        if not col:
            continue
        s = _strip_cell(row.get(col, ""))
        if s:
            lines.append(s)
    return ", ".join(lines)


def _normalise_status_display(raw: str) -> str | None:
    s = _strip_cell(raw)
    if not s:
        return None
    parts = s.split(maxsplit=1)
    if len(parts) == 2 and len(parts[0]) <= 4 and parts[0].isalpha():
        return parts[1].strip() or None
    return s


def _licence_type_for_row(
    lic_no: str,
    row: dict[str, str],
    spec: dict[str, Any],
    prefix_labels: dict[str, str],
) -> str | None:
    col = spec.get("license_type_column")
    if col:
        v = _strip_cell(row.get(col, ""))
        if v:
            return v
    if len(lic_no) >= 3 and lic_no[:3].isdigit():
        pfx = lic_no[:3]
        if pfx in prefix_labels:
            raw_label = prefix_labels[pfx].strip()
            return raw_label.title() if raw_label else None
    return None


def parse_rows(
    path: Path,
    mapping: dict[str, Any],
    prefix_labels: dict[str, str],
    source_resource_id: str,
    source_resource_name: str,
    source_snapshot_date: date,
) -> Iterator[dict[str, Any]]:
    """Yield Supabase-shaped dicts for each spreadsheet row."""
    for spec in mapping["sheets"]:
        sn = spec["sheet_name"]
        hdr_row = spec["header_row"]
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
        else:
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
        lic_ee = spec["licensee_column"]
        snap_str = source_snapshot_date.isoformat()

        for _, ser in df.iterrows():
            raw_cells: dict[str, str] = {str(k): _strip_cell(v) for k, v in ser.items()}
            raw_cells["sheet_name"] = sn

            lic_no = _strip_cell(raw_cells.get(lic_col, ""))
            if not lic_no:
                continue

            licensee = _strip_cell(raw_cells.get(lic_ee, ""))
            if not licensee:
                continue

            trading_col = spec.get("trading_name_column")
            if trading_col and trading_col != lic_ee:
                trading = _strip_cell(raw_cells.get(trading_col, ""))
            else:
                trading = licensee

            st_col = spec.get("license_status_column")
            status_raw = _strip_cell(raw_cells.get(st_col, "")) if st_col else ""
            lic_status = _normalise_status_display(status_raw)

            premises_address = _build_premises_address(raw_cells, spec)
            lic_type = _licence_type_for_row(lic_no, raw_cells, spec, prefix_labels)

            row: dict[str, Any] = {
                "state_code": "SA",
                "license_number": lic_no,
                "license_type": _nullable_text(lic_type) if lic_type else None,
                "license_class": None,
                "license_status": lic_status,
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
                "premises_state": "SA",
                "lat": None,
                "lng": None,
                "lga": _nullable_text(
                    _strip_cell(raw_cells.get(spec.get("lga_column") or "", ""))
                ),
                "region": None,
                "authorisations": None,
                "source_dataset": SOURCE_DATASET,
                "source_resource_id": source_resource_id,
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

    meta = None
    main_path: Path | None = None
    types_path: Path | None = None

    with httpx.Client(
        headers=headers,
        timeout=httpx.Timeout(180.0, connect=30.0),
        follow_redirects=True,
    ) as http:
        meta = find_latest_resource(http, budget)
        main = meta["main"]
        main_url = str(main["url"])
        main_fname = unquote(main_url.rstrip("/").split("/")[-1].split("?")[0] or "snapshot.xlsx")
        main_path = download_file(http, budget, main_url, main_fname)

        types_res = meta.get("types")
        if types_res and budget.total < MAX_HTTP_REQUESTS:
            t_url = str(types_res["url"])
            t_fname = unquote(t_url.rstrip("/").split("/")[-1].split("?")[0] or "licence_types.xlsx")
            types_path = download_file(http, budget, t_url, t_fname)
        elif types_res:
            LOG.warning("HTTP budget did not allow downloading the licence types reference.")

    assert main_path is not None and meta is not None
    prefix_labels = load_prefix_type_labels(types_path)
    print(f"Loaded {len(prefix_labels)} licence prefix labels from reference workbook.")

    disc = discover_columns(main_path)
    main = meta["main"]
    source_resource_id = str(main.get("id") or "")
    source_resource_name = str(main.get("name") or "")
    snapshot_date: date = meta["source_snapshot_date"]

    total = 0
    batch: list[dict[str, Any]] = []
    for row in parse_rows(
        main_path,
        disc,
        prefix_labels,
        source_resource_id,
        source_resource_name,
        snapshot_date,
    ):
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
