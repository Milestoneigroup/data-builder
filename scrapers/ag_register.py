"""Scrape the Commonwealth Register of marriage celebrants (AG Deregistration S115).

``https://marriage.ag.gov.au/commonwealthcelebrants/all`` — Telerik RadGrid with
ASP.NET ``__doPostBack`` pagination (~215 pages, 50 rows).

Usage
-----
    python -m scrapers.ag_register
    python -m scrapers.ag_register --max-pages 5
    python -m scrapers.ag_register --max-pages 215
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import expect, sync_playwright

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from scrapers.celebrants_schema import VERIFY_REQUIRED, load_53_column_names  # noqa: E402

URL = "https://marriage.ag.gov.au/commonwealthcelebrants/all"
OUT_PATH = _ROOT / "data" / "ag_register_raw.csv"
LOG_PATH = _ROOT / "logs" / "ag_register.log"
PROGRESS_EVERY = 10
PAGE_DELAY_S = 2.0
TOTAL_PAGES = 215

# --- parsing helpers ---

RE_PHONE = re.compile(
    r"(?:(?:^|\b)(?:m|p(?:\([Hh]\))?):)\s*([0-9+()\s]{6,20})",
    re.MULTILINE,
)
RE_STATE = re.compile(r"\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b", re.I)
RE_POSTCODE = re.compile(r"\b(\d{4})\b")


def _setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.handlers.clear()
    root.addHandler(fh)
    root.addHandler(ch)


def _parse_name_cell(html_section: str) -> tuple[str, str, str, str, str, str]:
    """Return full_name, register_class, reg_date, status, unavailability, raw."""
    soup = BeautifulSoup(html_section, "lxml")
    name_el = soup.find("span", id=lambda x: x and "lblCelebrant" in (x or ""))
    full_name = (name_el.get_text(" ", strip=True) if name_el else soup.get_text(" ", strip=True) or "")

    reg_class = "VERIFY_REQUIRED"
    st = soup.find("span", class_="gridview_status")
    if st and st.get_text(strip=True):
        reg_class = st.get_text(" ", strip=True)

    reg_date = "VERIFY_REQUIRED"
    for sp in soup.find_all("span", class_="gridview_smalltext"):
        b = sp.find("b")
        if b and b.string and re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", b.string.strip()):
            reg_date = b.string.strip()
            break

    sub_st = soup.find("span", id=lambda x: x and x.endswith("spnCelebrantSubStatus"))
    status = "VERIFY_REQUIRED"
    if sub_st and sub_st.get_text(strip=True):
        raw_s = sub_st.get_text(" ", strip=True)
        status = _normalize_status(raw_s)

    unavail = ""
    dv = soup.find("span", id=lambda x: x and "dvUnavailable" in (x or ""))
    if dv and dv.get_text(strip=True):
        unavail = dv.get_text(" ", strip=True)

    return (full_name.strip(), reg_class, reg_date, status, unavail, html_section)


def _normalize_status(raw: str) -> str:
    u = raw.upper()
    if "INACTIVE" in u:
        return "Inactive"
    if "UNAVAILABLE" in u:
        return "Unavailable"
    if "ACTIVE" in u:
        return "Active"
    return raw.strip() or "VERIFY_REQUIRED"


def _state_from_address_text(addr: str) -> str:
    m = RE_STATE.search(addr or "")
    return m.group(1).upper() if m else "VERIFY_REQUIRED"


def _postcode_from_text(addr: str) -> str:
    m = RE_POSTCODE.search(addr or "")
    return m.group(1) if m else "VERIFY_REQUIRED"


def _parse_address_cell(html_section: str) -> tuple[str, str, str, str, str]:
    """Return raw_text, email, phone, address_line, ceremony_type."""
    soup = BeautifulSoup(html_section, "lxml")
    email = "VERIFY_REQUIRED"
    phone = "VERIFY_REQUIRED"
    a = soup.find("a", href=True)
    if a and str(a.get("href", "")).lower().startswith("mailto:"):
        email = a.get("href").split(":", 1)[-1].strip() or "VERIFY_REQUIRED"

    raw_text = soup.get_text("\n", strip=True)
    for m in RE_PHONE.finditer(raw_text):
        p = m.group(1)
        p = re.sub(r"\s+", " ", p).strip()
        if p and phone == "VERIFY_REQUIRED":
            phone = p

    ceremony = "VERIFY_REQUIRED"
    bolds = [b.get_text(" ", strip=True) for b in soup.find_all("b")]
    for b in bolds:
        if "cerem" in b.lower() or b.lower() in ("civil ceremonies", "religious ceremonies"):
            ceremony = b
            break
    if ceremony == "VERIFY_REQUIRED":
        for sp in soup.find_all("span", class_="gridview_smalltext"):
            t = sp.get_text(" ", strip=True)
            if t and ("Civil" in t or "Religious" in t):
                ceremony = t
                break

    addr_lines: list[str] = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or "cerem" in line.lower():
            continue
        if re.match(r"^m:", line, re.I) or re.match(r"^p\(", line, re.I) or "@" in line:
            continue
        if line and line not in addr_lines and not line.lower().startswith("civil"):
            addr_lines.append(line)
    address_line = " ".join(addr_lines).strip() if addr_lines else ""
    if not address_line:
        # strip known bits from raw
        tmp = raw_text
        for _ in range(2):
            tmp = re.sub(r"(?i)^\s*m:\s*[\d+()\s]+\s*", "", tmp)
        tmp = re.sub(r"[\w.+-]+@[\w.-]+\.?\w*", "", tmp)
        lines = [ln.strip() for ln in tmp.splitlines() if ln.strip() and "cerem" not in ln.lower()]
        address_line = " ".join(lines[:3]) if lines else "VERIFY_REQUIRED"
    if address_line in (phone, email) and phone != "VERIFY_REQUIRED":
        address_line = "VERIFY_REQUIRED"

    return (raw_text, email, phone, address_line, ceremony)


def _assign_celebrant_id(state: str, per_state: defaultdict[str, int]) -> str:
    st = (state or "UNK").strip().upper()[:10] or "UNK"
    if st not in ("NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT", "UNK"):
        st = re.sub(r"[^A-Z0-9]", "", st) or "UNK"
    per_state[st] += 1
    n = per_state[st]
    return f"CEL-{st}-{n:06d}"


def _row_to_record(
    cols: list[str],
    group_state: str,
    page: int,
    index_on_page: int,
) -> tuple[dict[str, str], str]:
    h1, h2 = cols[0], cols[1]
    full_name, reg_class, reg_date, status, unavail, _ = _parse_name_cell(h1)
    raw_text, email, phone, addr_line, ceremony = _parse_address_cell(h2)
    st_from_adr = _state_from_address_text(raw_text)
    st = st_from_adr
    if st == "VERIFY_REQUIRED" and group_state != "UNK":
        st = group_state
    elif st == "VERIFY_REQUIRED":
        st = "UNK"
    st_for_id = st if st != "VERIFY_REQUIRED" else "UNK"
    pc = _postcode_from_text(raw_text)
    if pc == "VERIFY_REQUIRED" and addr_line and addr_line != "VERIFY_REQUIRED":
        pc = _postcode_from_text(addr_line)

    mapping = {
        "brand_id": VERIFY_REQUIRED,
        "full_name": full_name or VERIFY_REQUIRED,
        "ag_display_name": full_name or VERIFY_REQUIRED,
        "title": VERIFY_REQUIRED,
        "email": email,
        "phone": phone,
        "state": st,
        "address_text": addr_line or VERIFY_REQUIRED,
        "suburb": VERIFY_REQUIRED,
        "postcode": pc,
        "website": VERIFY_REQUIRED,
        "registration_date": reg_date,
        "register_class": reg_class,
        "status": status,
        "unavailability_text": unavail or VERIFY_REQUIRED,
        "ceremony_type": ceremony,
        "data_source": "AG_REGISTER",
        "ag_scrape_page": str(page),
        "ag_scrape_index": str(index_on_page),
        "raw_address_cell": raw_text or VERIFY_REQUIRED,
    }
    return mapping, st_for_id


def scrape_pages(max_pages: int) -> list[dict[str, str]]:
    per_state: defaultdict[str, int] = defaultdict(int)
    rows: list[dict[str, str]] = []
    names = load_53_column_names()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(120_000)
        logging.info("Navigating to %s", URL)
        page.goto(URL, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")

        grid = page.locator("#ctl00_MainContent_gridCelebrants")
        pager_nums = grid.locator(".rgNumPart").last
        page_next = grid.locator("input.rgPageNext").last

        for pg in range(1, max_pages + 1):
            if pg > 1:
                time.sleep(PAGE_DELAY_S)
                if pg > TOTAL_PAGES + 1:
                    break
                page_next.click()
                # RadAjax UpdatePanel: wait until footer pager shows new page.
                expect(pager_nums.locator("a.rgCurrentPage")).to_have_text(
                    str(pg), timeout=30_000
                )
            time.sleep(0.2)
            inner = page.locator("table.rgMasterTable").first.inner_html()
            soup = BeautifulSoup(f"<table>{inner}</table>", "lxml")
            current_state = "UNK"
            idx_page = 0
            for tr in soup.find_all("tr"):
                raw_c = tr.get("class") or []
                tcls = raw_c if isinstance(raw_c, list) else str(raw_c).split()
                if "rgGroupHeader" in tcls:
                    gtxt = tr.get_text(" ", strip=True).replace("\xa0", " ")
                    m = re.search(
                        r"\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b",
                        gtxt,
                        re.I,
                    )
                    if m:
                        current_state = m.group(1).upper()
                    else:
                        lines = [ln.strip() for ln in gtxt.splitlines() if ln.strip()]
                        nxt = lines[-1].upper() if lines else "UNK"
                        current_state = nxt if nxt in {
                            "NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"
                        } else "UNK"
                    continue
                if "rgRow" not in tcls and "rgAltRow" not in tcls:
                    continue
                tds = tr.find_all("td", recursive=False)
                if len(tds) < 2:
                    continue
                h1 = str(tds[1])
                h2 = str(tds[2]) if len(tds) > 2 else ""
                if "lblCelebrant" not in h1 and not tds[1].get_text(strip=True):
                    continue
                idx_page += 1
                cols = [h1, h2]
                record, st_for_id = _row_to_record(cols, current_state, pg, idx_page)
                cid = _assign_celebrant_id(st_for_id, per_state)
                record["celebrant_id"] = cid
                row_out: dict[str, str] = {k: VERIFY_REQUIRED for k in names}
                for k, v in record.items():
                    if k in row_out:
                        row_out[k] = v
                rows.append(row_out)

            if pg % PROGRESS_EVERY == 0 or pg == max_pages:
                logging.info("Progress: finished page %s of %s (%s rows so far)", pg, max_pages, len(rows))
        browser.close()
    return rows


def _write_csv(records: list[dict[str, str]], out_path: Path) -> None:
    if not records:
        logging.warning("No records to write")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    names = load_53_column_names()
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=names, extrasaction="ignore")
        w.writeheader()
        w.writerows(records)
    logging.info("Wrote %s rows to %s", len(records), out_path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=TOTAL_PAGES, help="Max pages to fetch (default 215)")
    ap.add_argument("--out", type=Path, default=OUT_PATH)
    ap.add_argument("--log", type=Path, default=LOG_PATH)
    args = ap.parse_args()
    _setup_logging(args.log)
    logging.info("Starting AG register scrape, max_pages=%s", args.max_pages)
    try:
        recs = scrape_pages(args.max_pages)
    except Exception:  # noqa: BLE001
        logging.exception("Scrape failed")
        return 1
    _write_csv(recs, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
