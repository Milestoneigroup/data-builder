"""Build and load shared.ref_councils from official state directories."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

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
# Default httpx read/connect timeouts (s)
HTTPX_TIMEOUT_S = 75.0
# SA: shorter per-attempt client timeout, more retries
SA_HTTPX_TIMEOUT_S = 30.0
SA_MAX_ATTEMPTS = 3
WA_503_RETRIES = 3
WA_503_DELAY_S = 5.0

# Reject list-scrape noise: councillor pages, news, org events, etc. (name + full URL, lowercased)
_EXCLUDE_SUBSTR = (
    "councillor",
    "councilor",
    "mayor",
    "election",
    "ward",
    "meeting",
    "minutes",
    "agenda",
    "news",
    "event",  # directory "events" / news, not permit pages (this script only lists councils)
)

SOURCES: dict[str, dict[str, Any]] = {
    "NSW": {
        "url": "https://www.lgnsw.org.au/about/nsw-councils",
        "alt_urls": (
            "https://lgnsw.org.au/Public/public/NSW-Councils/NSW-Council-Links.aspx",
        ),
        "source_directory": "LGNSW — NSW Councils",
        "expected": 128,
    },
    "QLD": {
        "url": "https://www.lgaq.asn.au/about/member-councils",
        "alt_urls": (
            "https://www.dlgwv.qld.gov.au/local-government/for-the-community/local-government-directory",
        ),
        "source_directory": "LGAQ — Member Councils",
        "expected": 77,
    },
    "VIC": {
        "url": "https://www.mav.asn.au/what-we-do/local-government/councils",
        "alt_urls": (
            "https://www.viccouncils.asn.au/find-your-council/council-contacts-list",
        ),
        "source_directory": "MAV — Councils",
        "expected": 79,
    },
    "TAS": {
        "url": "https://www.lgat.tas.gov.au/tasmanian-councils",
        "alt_urls": (
            "https://www.lgat.tas.gov.au/tasmanian-councils/find-your-local-council",
        ),
        "source_directory": "LGAT — Tasmanian Councils",
        "expected": 29,
    },
    "SA": {
        "url": "https://www.lga.sa.gov.au/councils",
        "alt_urls": (
            "https://www.lga.sa.gov.au/sa-councils/councils-listing",
        ),
        "source_directory": "LGA SA — Councils",
        "expected": 68,
    },
    "WA": {
        "url": "https://walga.asn.au/about-walga/member-councils",
        "alt_urls": (
            "https://www.walga.asn.au/about-walga/member-councils",
            "https://www.dlgsc.wa.gov.au/",
            "https://www.wa.gov.au/government/local-government",
        ),
        "source_directory": "WALGA — Member Councils",
        "expected": 137,
    },
}

# Council website hostnames we never store (TAS peak body; real LGAs are *.tas.gov.au elsewhere)
_REJECT_LGA_TAS_DIR_HOSTS: frozenset[str] = frozenset({"lgat.tas.gov.au"})


def _host_key(url: str) -> str:
    p = urlparse(url)
    h = (p.netloc or "").lower()
    if h.startswith("www."):
        h = h[4:]
    return h


def _is_rejected_council_host(url: str) -> bool:
    return _host_key(url) in _REJECT_LGA_TAS_DIR_HOSTS


def _is_junk_label_council_name(name: str) -> bool:
    low = name.lower().strip()
    if low.startswith("for council"):
        return True
    if "council cost index" in low:
        return True
    if low in {"tasmanian councils", "tasmanian councils \u2014 overview", "tasmanian councils - overview"}:
        return True
    if re.match(r"^find your local( council)?\.?$", low):
        return True
    if low.startswith("council jobs") or "council careers" in low:
        return True
    return False


def _resolve_lga_sa_profile_listing(website: str, log: logging.Logger) -> str | None:
    """Resolve LGA SA 'councils-listing' profile to the LGA's own *.sa.gov.au (not elections/state hubs)."""
    if "lga.sa.gov.au" not in website.lower() or "councils-listing" not in website.lower():
        return None
    bad_sa = frozenset(
        {
            "lga.sa.gov.au",
            "www.lga.sa.gov.au",
            "councilelections.sa.gov.au",
            "www.councilelections.sa.gov.au",
            "localcouncils.sa.gov.au",
            "www.localcouncils.sa.gov.au",
            "environment.sa.gov.au",
            "www.environment.sa.gov.au",
        }
    )
    html = _httpx_then_playwright(website, log, 25.0)
    if not html or len(html.strip()) < 200:
        return None
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        if ".sa.gov.au" not in href.lower():
            continue
        if href.startswith(("#", "javascript:", "mailto:")):
            continue
        full = urljoin(website, href)
        h = _host_key(full)
        if not h.endswith("sa.gov.au") or h in bad_sa or h == "sa.gov.au":
            continue
        if _is_gov_au_host(full):
            return full
    return None


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


def _load_optional_seed(state_code: str) -> list[tuple[str, str]]:
    """Optional `data/seed_councils_{state}.json`: list of {council_name, website} for hard-to-scrape states."""
    p = _ROOT / "data" / f"seed_councils_{state_code}.json"
    if not p.is_file():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(data, list):
        return []
    out: list[tuple[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        n = str(item.get("council_name") or "").strip()
        w = str(item.get("website") or "").strip()
        if n and w:
            out.append((n, w))
    return out


def _load_env() -> None:
    for p in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if p.is_file():
            load_dotenv(p, override=True, encoding="utf-8")


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _clean_name(name: str) -> str:
    return _norm_space(re.sub(r"\s*\(.*?\)\s*$", "", name))


def _name_looks_like_council(name: str) -> bool:
    low = name.lower()
    return (
        "council" in low
        or "shire" in low
        or re.search(r"\bcity\b", low) is not None
        or "municipality" in low
    )


def _is_excluded_name_or_url(name: str, url: str) -> bool:
    blob = f"{name} {url}".lower()
    return any(sub in blob for sub in _EXCLUDE_SUBSTR)


def _is_gov_au_host(url: str) -> bool:
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.netloc:
        return False
    host = p.netloc.lower()
    # Australian public-sector council sites (broad: *.gov.au)
    if host.endswith(".gov.au") or host == "gov.au":
        return True
    return False


def _is_valid_council_website(name: str, state_code: str, website: str) -> bool:
    """Require *.gov.au for almost all; allow Hobart (TAS) on hobartcity.com.au per LGAT directory."""
    w = str(website).strip()
    if _is_gov_au_host(w) and not _is_rejected_council_host(w):
        return True
    if state_code == "TAS" and w.lower().endswith("hobartcity.com.au") and "hobart" in name.lower():
        return True
    return False


def _passes_quality_gate(name: str, state_code: str, website: str) -> bool:
    if _is_junk_label_council_name(name):
        return False
    if not _name_looks_like_council(name):
        return False
    if not _is_valid_council_website(name, state_code, website):
        return False
    if _is_rejected_council_host(website):
        return False
    return not _is_excluded_name_or_url(name, website)


def _is_social_url(url: str) -> bool:
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.netloc:
        return True
    host = p.netloc.lower()
    return any(
        b in host
        for b in (
            "facebook.com",
            "instagram.com",
            "linkedin.com",
            "youtube.com",
            "x.com",
            "twitter.com",
        )
    )


def _httpx_get(
    url: str,
    log: logging.Logger,
    *,
    timeout_s: float = HTTPX_TIMEOUT_S,
) -> tuple[str | None, int | None, str | None]:
    try:
        with httpx.Client(
            timeout=httpx.Timeout(timeout_s, connect=min(30.0, timeout_s)),
            follow_redirects=True,
        ) as client:
            r = client.get(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-AU,en;q=0.9",
                },
            )
            code = r.status_code
            if code in (403, 404, 429, 500, 502, 503, 504):
                return None, code, f"http_{code}"
            r.raise_for_status()
            return r.text, code, None
    except Exception as e:  # noqa: BLE001
        err = str(e)
        if "ReadTimeout" in type(e).__name__ or "timeout" in err.lower():
            return None, None, f"httpx_error:{err[:120]}"
        if hasattr(e, "response") and getattr(e.response, "status_code", None):
            sc = e.response.status_code
            if sc in (403, 503, 404):
                return None, sc, f"http_{sc}"
        log.warning("httpx failed url=%s err=%s", url, e)
        return None, None, f"httpx_error:{err[:120]}"


def _fetch_html_playwright(url: str, log: logging.Logger, *, scroll: bool = False) -> str | None:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        log.warning("Playwright unavailable err=%s", e)
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(user_agent=USER_AGENT)
            except TypeError:
                page = browser.new_page()
            page.set_default_timeout(120_000)
            page.goto(url, wait_until="load", timeout=120_000)
            page.wait_for_timeout(2_500)
            if scroll:
                for _ in range(4):
                    try:
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    except Exception:  # noqa: BLE001
                        break
                    page.wait_for_timeout(1_200)
            html = page.content()
            browser.close()
            return html
    except Exception as e:  # noqa: BLE001
        log.warning("Playwright failed url=%s err=%s", url, e)
        return None


def _httpx_then_playwright(url: str, log: logging.Logger, timeout_s: float) -> str | None:
    html, code, _ = _httpx_get(url, log, timeout_s=timeout_s)
    if html and len(html.strip()) > 200:
        return html
    log.info("Falling back to Playwright (httpx code=%s, short_or_empty=%s) url=%s", code, not bool(html), url)
    return _fetch_html_playwright(url, log)


def _fetch_wa_with_retries(state_code: str, url: str, log: logging.Logger) -> str | None:
    for attempt in range(1, WA_503_RETRIES + 1):
        html, code, _ = _httpx_get(url, log, timeout_s=HTTPX_TIMEOUT_S)
        if html and len(html.strip()) > 200:
            return html
        if code == 503 and attempt < WA_503_RETRIES:
            log.info("WA 503, sleeping %ss before retry %s/%s", WA_503_DELAY_S, attempt, WA_503_RETRIES)
            time.sleep(WA_503_DELAY_S)
            continue
        log.info("WA: trying Playwright after httpx (code=%s) attempt %s", code, attempt)
        pw = _fetch_html_playwright(url, log)
        if pw and len(pw.strip()) > 200:
            return pw
        if attempt < WA_503_RETRIES and code is None:
            time.sleep(WA_503_DELAY_S)
    return _fetch_html_playwright(url, log)


def _fetch_sa_with_retries(url: str, log: logging.Logger) -> str | None:
    for attempt in range(1, SA_MAX_ATTEMPTS + 1):
        log.info("SA fetch attempt %s/%s (httpx timeout=%ss)", attempt, SA_MAX_ATTEMPTS, SA_HTTPX_TIMEOUT_S)
        html, code, _ = _httpx_get(url, log, timeout_s=SA_HTTPX_TIMEOUT_S)
        if html and len(html.strip()) > 200:
            return html
        log.info("SA: Playwright fallback (httpx code=%s) attempt=%s", code, attempt)
        pw = _fetch_html_playwright(url, log)
        if pw and len(pw.strip()) > 200:
            return pw
    return _fetch_html_playwright(url, log)


def _fetch_state_page(state_code: str, url: str, log: logging.Logger) -> str | None:
    if state_code == "WA":
        return _fetch_wa_with_retries(state_code, url, log)
    if state_code == "SA":
        return _fetch_sa_with_retries(url, log)
    return _httpx_then_playwright(url, log, HTTPX_TIMEOUT_S)


def _extract_raw_pairs(soup: BeautifulSoup, page_url: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for a in soup.select("a[href]"):
        name = _clean_name(a.get_text(" ", strip=True))
        if _is_junk_label_council_name(name):
            continue
        raw_href = str(a.get("href") or "").strip()
        if not name or not raw_href or raw_href.startswith(("#", "javascript:", "mailto:")):
            continue
        href = urljoin(page_url, raw_href)
        if _is_social_url(href):
            continue
        if not _is_real_href(href):
            continue
        if _is_excluded_name_or_url(name, href):
            continue
        if not _name_looks_like_council(name):
            continue
        out.append((name, href))
    return out


def _extract_gov_au_member_pairs(soup: BeautifulSoup, page_url: str) -> list[tuple[str, str]]:
    """Second pass: any anchor clearly pointing at an Australian government site."""
    out: list[tuple[str, str]] = []
    for a in soup.select("a[href]"):
        name = _clean_name(a.get_text(" ", strip=True))
        if _is_junk_label_council_name(name):
            continue
        raw_href = str(a.get("href") or "").strip()
        if not name or not raw_href or raw_href.startswith(("#", "javascript:", "mailto:")):
            continue
        if ".gov.au" not in raw_href.lower():
            continue
        href = urljoin(page_url, raw_href)
        if not _is_gov_au_host(href) or _is_social_url(href):
            continue
        if not _is_real_href(href):
            continue
        if _is_excluded_name_or_url(name, href):
            continue
        if not _name_looks_like_council(name):
            continue
        out.append((name, href))
    return out


_STATE_TLD: dict[str, str] = {
    "NSW": ".nsw.gov.au",
    "QLD": ".qld.gov.au",
    "VIC": ".vic.gov.au",
    "TAS": ".tas.gov.au",
    "SA": ".sa.gov.au",
    "WA": ".wa.gov.au",
}


def _extract_state_tld_pairs(
    soup: BeautifulSoup, page_url: str, state_code: str
) -> list[tuple[str, str]]:
    """Third pass: state TLD (e.g. .qld.gov.au) for member-council homepages."""
    needle = _STATE_TLD.get(state_code)
    if not needle:
        return []
    out: list[tuple[str, str]] = []
    for a in soup.select("a[href]"):
        name = _clean_name(a.get_text(" ", strip=True))
        if _is_junk_label_council_name(name):
            continue
        raw_href = str(a.get("href") or "").strip()
        if not name or not raw_href or raw_href.startswith(("#", "javascript:", "mailto:")):
            continue
        if needle not in raw_href.lower():
            continue
        href = urljoin(page_url, raw_href)
        if not _is_gov_au_host(href) or _is_social_url(href):
            continue
        if not _is_real_href(href):
            continue
        if _is_excluded_name_or_url(name, href):
            continue
        if not _name_looks_like_council(name):
            continue
        out.append((name, href))
    return out


def _is_real_href(href: str) -> bool:
    p = urlparse(href)
    return p.scheme in ("http", "https") and bool(p.netloc)


def _dedupe(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for name, w in pairs:
        key = (name.lower(), w.lower().rstrip("/"))
        if key in seen:
            continue
        seen.add(key)
        out.append((name, w))
    return out


def _build_rows_for_state(
    state_code: str, pairs: list[tuple[str, str]], log: logging.Logger, skipped: list[str]
) -> list[dict[str, Any]]:
    src = SOURCES[state_code]
    good: list[tuple[str, str]] = []
    for name, website in pairs:
        w = website
        if "lga.sa.gov.au" in w.lower() and "councils-listing" in w.lower():
            resolved = _resolve_lga_sa_profile_listing(w, log)
            if resolved:
                log.info("Resolved LGA SA profile to council site: %s -> %s", w, resolved)
                w = resolved
        if not _passes_quality_gate(name, state_code, w):
            skipped.append(
                json.dumps(
                    {
                        "reason": "quality_gate",
                        "state_code": state_code,
                        "council_name": name,
                        "website": w,
                    },
                    ensure_ascii=False,
                )
            )
            log.info("SKIPPED (quality): %s | %s", name, w)
            continue
        good.append((name, w))

    rows: list[dict[str, Any]] = []
    for name, website in good:
        rows.append(
            {
                "council_name": name,
                "state_code": state_code,
                "website": website,
                "url_pattern": "official directory (gov.au)",
                "source_directory": src["source_directory"],
                "scraped_date": date.today().isoformat(),
                "is_active": True,
            }
        )
    rows.sort(key=lambda r: (r["state_code"], r["council_name"]))
    for i, row in enumerate(rows, start=1):
        row["council_id"] = f"CNCL-{state_code}-{i:03d}"
    return rows


def _playwright_harvest_council_anchors(
    page_url: str, domain_needle: str, state_code: str, log: logging.Logger
) -> list[tuple[str, str]]:
    """Collect (name, href) from the live DOM (handles JS-rendered link lists)."""
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # noqa: BLE001
        log.debug("playwright missing: %s", e)
        return []
    out: list[tuple[str, str]] = []
    try:
        with sync_playwright() as p:
            br = p.chromium.launch(headless=True)
            try:
                page = br.new_page(user_agent=USER_AGENT)
            except TypeError:
                page = br.new_page()
            page.set_default_timeout(120_000)
            page.goto(page_url, wait_until="load", timeout=120_000)
            page.wait_for_timeout(3_000)
            for _ in range(3):
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:  # noqa: BLE001
                    break
                page.wait_for_timeout(1_200)
            rows = page.eval_on_selector_all(
                "a[href]",
                """
                (els) => els.map((e) => [
                    (e.textContent || '').replace(/\\s+/g, ' ').trim(),
                    e.getAttribute('href') || ''
                ])
                """,
            )
            br.close()
    except Exception as e:  # noqa: BLE001
        log.warning("playwright_harvest failed url=%s err=%s", page_url, e)
        return []
    for name, href in rows or []:
        name = _clean_name(name)
        if not name or not href:
            continue
        full = urljoin(page_url, href.strip())
        if domain_needle not in full.lower():
            continue
        if _is_excluded_name_or_url(name, full):
            continue
        if not _name_looks_like_council(name):
            continue
        if not _is_gov_au_host(full):
            continue
        out.append((name, full))
    log.info("playwright_harvest %s n=%s url=%s", state_code, len(out), page_url)
    return out


def _scrape_state(state_code: str, log: logging.Logger, skipped: list[str]) -> list[dict[str, Any]]:
    src = SOURCES[state_code]
    page_urls: list[str] = [src["url"]] + list(src.get("alt_urls") or ())
    blobs: dict[str, str] = {}
    for u in page_urls:
        try:
            html = _fetch_state_page(state_code, u, log)
        except Exception as e:  # noqa: BLE001
            log.error("Fetch crashed state=%s url=%s err=%s", state_code, u, e)
            continue
        if html and len(html.strip()) > 200:
            blobs[u] = html
    if not blobs:
        log.error("No HTML for %s (tried %s URLs)", state_code, page_urls)
        return []
    merged: list[tuple[str, str]] = []
    for u, html in blobs.items():
        soup = BeautifulSoup(html, "html.parser")
        merged.extend(_extract_raw_pairs(soup, u))
        merged.extend(_extract_gov_au_member_pairs(soup, u))
        merged.extend(_extract_state_tld_pairs(soup, u, state_code))
    raw = _dedupe(merged)
    if state_code in ("QLD", "WA") and len(raw) < 40:
        for u in page_urls:
            hs = _fetch_html_playwright(u, log, scroll=True)
            if not hs or len(hs.strip()) < 200:
                continue
            sp = BeautifulSoup(hs, "html.parser")
            merged.extend(_extract_raw_pairs(sp, u))
            merged.extend(_extract_gov_au_member_pairs(sp, u))
            merged.extend(_extract_state_tld_pairs(sp, u, state_code))
        raw = _dedupe(merged)
    if state_code == "QLD" and len(raw) < 40:
        for u in page_urls:
            merged.extend(_playwright_harvest_council_anchors(u, "qld.gov.au", "QLD", log))
        raw = _dedupe(merged)
    if state_code == "WA" and len(raw) < 40:
        for u in page_urls:
            merged.extend(_playwright_harvest_council_anchors(u, "wa.gov.au", "WA", log))
        raw = _dedupe(merged)
    seed = _load_optional_seed(state_code)
    if seed and len(raw) < 40:
        log.info("Merging %s optional seed rows for %s from data/seed_councils_%s.json", len(seed), state_code, state_code)
        merged.extend(seed)
        raw = _dedupe(merged)
    rows = _build_rows_for_state(state_code, raw, log, skipped)
    log.info("state=%s rows(clean)=%s raw_links=%s expected~%s", state_code, len(rows), len(raw), src["expected"])
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
    parser.add_argument("--full", action="store_true", help="All 6 states, default JSON output")
    parser.add_argument("--json-out", default=str(OUT_JSON_DEFAULT))
    parser.add_argument(
        "--load",
        action="store_true",
        help="Upsert all scraped rows to shared.ref_councils (same as legacy --load-supabase)",
    )
    parser.add_argument(
        "--load-supabase",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()
    do_load = args.load or args.load_supabase

    _load_env()
    log = _setup_logging()
    out_path = Path(args.json_out)
    if not out_path.is_absolute():
        out_path = (_ROOT / out_path).resolve()

    # Step 6 style: --load without --full → upsert from existing JSON only (no re-scrape).
    if do_load and not args.full:
        if not out_path.is_file():
            raise SystemExit(f"No council JSON at {out_path}. Run: python -m scrapers.council_reference_builder --full")
        raw = json.loads(out_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise SystemExit("Council JSON must be a list of objects")
        rows = [dict(x) for x in raw if isinstance(x, dict)]
        print(f"Read {len(rows)} councils from {out_path}")
        loaded = _load_to_supabase(rows, log)
        print(f"Loaded to Supabase rows={loaded}")
        print(f"Log: {LOG_PATH}")
        return

    states = (
        ["NSW", "QLD", "VIC", "TAS", "SA", "WA"] if args.full else [s.strip().upper() for s in args.states.split(",")]
    )
    skipped: list[str] = []
    rows: list[dict[str, Any]] = []
    for st in states:
        if st in SOURCES:
            rows.extend(_scrape_state(st, log, skipped))

    counts = Counter(str(r.get("state_code") or "UNK") for r in rows)

    print(f"Total extracted (clean) rows: {len(rows)}")
    print("Per-state counts:")
    for st in sorted(SOURCES.keys()):
        print(f"  {st}: {counts.get(st, 0)}")
    print(f"Preview first {min(args.preview, len(rows))}:")
    for row in rows[: args.preview]:
        print(json.dumps(row, ensure_ascii=False))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote JSON: {out_path}")
    if skipped:
        sk_path = out_path.with_name(out_path.stem + "_skipped.log")
        sk_path.write_text("\n".join(skipped) + "\n", encoding="utf-8")
        print(f"Skipped log: {sk_path} ({len(skipped)} lines)")

    if do_load:
        loaded = _load_to_supabase(rows, log)
        print(f"Loaded to Supabase rows={loaded}")
    print(f"Log: {LOG_PATH}")


if __name__ == "__main__":
    main()
