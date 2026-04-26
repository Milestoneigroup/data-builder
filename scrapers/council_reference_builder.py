"""Build and load shared.ref_councils from official state directories."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

LOG_PATH = _ROOT / "logs" / "council_reference_builder.log"
OUT_JSON_DEFAULT = _ROOT / "data" / "councils_reference.json"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT_S = 75.0

SOURCES: dict[str, dict[str, Any]] = {
    "NSW": {
        "url": "https://lgnsw.org.au/Public/public/NSW-Councils/NSW-Council-Links.aspx",
        "source_directory": "LGNSW NSW Council Links",
        "expected": 128,
    },
    "QLD": {
        "url": "https://www.dlgwv.qld.gov.au/local-government/for-the-community/local-government-directory",
        "source_directory": "QLD DLGWV Local Government Directory",
        "expected": 77,
    },
    "VIC": {
        "url": "https://www.viccouncils.asn.au/find-your-council/council-contacts-list",
        "source_directory": "VIC Councils Contacts List",
        "expected": 79,
    },
    "TAS": {
        "url": "https://www.lgat.tas.gov.au/tasmanian-councils/find-your-local-council",
        "source_directory": "LGAT Tasmanian Councils",
        "expected": 29,
    },
    "SA": {
        "url": "https://www.lga.sa.gov.au/sa-councils/councils-listing",
        "source_directory": "LGA SA Councils Listing",
        "expected": 68,
    },
    "WA": {
        "url": "https://portal.walga.asn.au/your-local-government/online-local-government-directory",
        "source_directory": "WALGA Online Local Government Directory",
        "expected": 137,
    },
}


def _setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("council_reference_builder")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    return log


def _load_env() -> None:
    for p in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if p.is_file():
            load_dotenv(p, override=True, encoding="utf-8")


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _clean_name(name: str) -> str:
    return _norm_space(re.sub(r"\s*\(.*?\)\s*$", "", name))


def _is_real_website(url: str) -> bool:
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.netloc:
        return False
    host = p.netloc.lower()
    blocked = ("facebook.com", "instagram.com", "linkedin.com", "youtube.com", "x.com", "twitter.com")
    return not any(b in host for b in blocked)


def _fetch_html(url: str, log: logging.Logger) -> str | None:
    try:
        with httpx.Client(timeout=TIMEOUT_S, follow_redirects=True) as client:
            r = client.get(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-AU,en;q=0.9",
                },
            )
            r.raise_for_status()
            return r.text
    except Exception as e:  # noqa: BLE001
        log.warning("httpx failed url=%s err=%s", url, e)
        return None


def _fetch_html_playwright(url: str, log: logging.Logger) -> str | None:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        log.warning("Playwright unavailable err=%s", e)
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            page.goto(url, wait_until="networkidle", timeout=95_000)
            html = page.content()
            browser.close()
            return html
    except Exception as e:  # noqa: BLE001
        log.warning("Playwright failed url=%s err=%s", url, e)
        return None


def _extract_links_generic(soup: BeautifulSoup) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for a in soup.select("a[href]"):
        name = _clean_name(a.get_text(" ", strip=True))
        href = str(a.get("href") or "").strip()
        if not name or not href or not _is_real_website(href):
            continue
        low = name.lower()
        if not any(tok in low for tok in ("council", "shire", "city", "regional", "municipal", "district")):
            continue
        out.append((name, href))
    return out


def _extract_source_rows(state_code: str, html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    pairs = _extract_links_generic(soup)
    src = SOURCES[state_code]
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for name, website in pairs:
        key = (name.lower(), website.lower().rstrip("/"))
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "council_name": name,
                "state_code": state_code,
                "website": website,
                "url_pattern": "official directory homepage",
                "source_directory": src["source_directory"],
                "scraped_date": date.today().isoformat(),
                "is_active": True,
            }
        )
    rows.sort(key=lambda r: (r["state_code"], r["council_name"]))
    for i, row in enumerate(rows, start=1):
        row["council_id"] = f"CNCL-{state_code}-{i:03d}"
    return rows


def _scrape_state(state_code: str, log: logging.Logger) -> list[dict[str, Any]]:
    src = SOURCES[state_code]
    html = _fetch_html(src["url"], log)
    if not html:
        html = _fetch_html_playwright(src["url"], log)
    if not html:
        log.error("No HTML for %s", state_code)
        return []
    rows = _extract_source_rows(state_code, html)
    log.info("state=%s rows=%s expected~%s", state_code, len(rows), src["expected"])
    return rows


def _load_to_supabase(rows: list[dict[str, Any]], log: logging.Logger) -> int:
    from data_builder.config import get_settings
    from supabase import create_client

    settings = get_settings()
    sb = create_client((settings.supabase_url or "").strip(), (settings.supabase_service_role_key or "").strip())
    count = 0
    for row in rows:
        payload = dict(row)
        payload.setdefault("aligned_destination_ids", [])
        sb.schema("shared").table("ref_councils").upsert(payload, on_conflict="council_id").execute()
        count += 1
    try:
        sb.rpc("refresh_ref_councils_alignment").execute()
    except Exception as e:  # noqa: BLE001
        log.warning("refresh_ref_councils_alignment failed err=%s", e)
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Council reference builder")
    parser.add_argument("--states", default="NSW,QLD,VIC,TAS,SA,WA")
    parser.add_argument("--preview", type=int, default=10)
    parser.add_argument("--full", action="store_true", help="Shortcut for all 6 states with default output")
    parser.add_argument("--json-out", default=str(OUT_JSON_DEFAULT))
    parser.add_argument("--load-supabase", action="store_true")
    args = parser.parse_args()

    _load_env()
    log = _setup_logging()
    states = ["NSW", "QLD", "VIC", "TAS", "SA", "WA"] if args.full else [s.strip().upper() for s in args.states.split(",")]
    rows: list[dict[str, Any]] = []
    for st in states:
        if st in SOURCES:
            rows.extend(_scrape_state(st, log))

    print(f"Total extracted rows: {len(rows)}")
    print(f"Preview first {min(args.preview, len(rows))}:")
    for row in rows[: args.preview]:
        print(json.dumps(row, ensure_ascii=False))

    out_path = Path(args.json_out)
    if not out_path.is_absolute():
        out_path = (_ROOT / out_path).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote JSON: {out_path}")

    if args.load_supabase:
        loaded = _load_to_supabase(rows, log)
        print(f"Loaded to Supabase rows={loaded}")
    print(f"Log: {LOG_PATH}")


if __name__ == "__main__":
    main()
