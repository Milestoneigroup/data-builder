"""Active-market celebrant enrichment: directory scrapes → AG cross-reference → optional Places.

Step 1: Scrape Easy Weddings, AFCC, Wedding Society, Wedlockers, MyCelebrantApp → CSVs under ``data/``.
Step 2: Fuzzy-match to ``celebrants_master_v1.csv`` (token_sort_ratio ≥ 70), write ``celebrants_master_v2.csv``.
Step 3: Google Places (New) for active/featured only — **costs money**; run with ``--step3`` only when approved.

Quick test (EW pages 1–3 only, then Step 2)::

    python -m scrapers.celebrant_active_enrichment --ew-pages 3 --ew-only --step2

Full Step 1 (all sources) + Step 2::

    python -m scrapers.celebrant_active_enrichment --step1 --step2
"""

from __future__ import annotations

import argparse
import html as html_module
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from thefuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

USER_AGENT = "MilestoneDataBuilder/1.0 (+https://milestonei.com.au; celebrant-active-enrichment)"
VERIFY = "VERIFY_REQUIRED"
LOG = _ROOT / "logs" / "celebrant_active_enrichment.log"

# Browser-like headers for directory scrapes (e.g. Railway) to reduce bot friction.
RAILWAY_DIRECTORY_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.google.com.au/",
}

EW_BASE = "https://www.easyweddings.com.au"
TWS_REGIONS = (
    "new-south-wales",
    "queensland",
    "victoria",
    "south-australia",
    "western-australia",
    "tasmania",
    "act",
    "northern-territory",
)
TWS_DIR_TMPL = "https://theweddingsociety.co/directory/?type=marriage-celebrant&tab=search-form&region={region}"
TWS_HOME = "https://theweddingsociety.co/"
WL_URLS = (
    "https://www.wedlockers.com.au/planning/nsw-marriage-celebrant/",
    "https://www.wedlockers.com.au/planning/vic-marriage-celebrant/",
    "https://www.wedlockers.com.au/planning/qld-marriage-celebrant/",
    "https://www.wedlockers.com.au/planning/wa-marriage-celebrant/",
    "https://www.wedlockers.com.au/planning/sa-marriage-celebrant/",
    "https://www.wedlockers.com.au/planning/tas-marriage-celebrant/",
)
WL_STATE = {"nsw": "NSW", "vic": "VIC", "qld": "QLD", "wa": "WA", "sa": "SA", "tas": "TAS"}
MCA_BROWSE = "https://app.mycelebrantapp.com/browse"

MCA_CSV_COLUMNS = (
    "name",
    "state",
    "profile_url",
    "review_count",
    "state_location",
    "speciality",
    "price_range",
    "mycelebrantapp_profile_url",
)


def _wedlockers_urls_for_states(states: list[str] | None) -> tuple[str, ...]:
    """Subset of ``WL_URLS`` by lowercase state token (e.g. ``nsw``). ``None`` = all."""
    if states is None:
        return WL_URLS
    want = {str(s).lower().strip() for s in states if str(s).strip()}
    if not want:
        return WL_URLS
    picked: list[str] = []
    for u in WL_URLS:
        m = re.search(r"/planning/([a-z]+)-marriage", u, re.I)
        if m and m.group(1).lower() in want:
            picked.append(u)
    return tuple(picked)

TWS_REGION_TO_STATE = {
    "new-south-wales": "NSW",
    "queensland": "QLD",
    "victoria": "VIC",
    "south-australia": "SA",
    "western-australia": "WA",
    "tasmania": "TAS",
    "act": "ACT",
    "northern-territory": "NT",
}

DEST_KW = ("travel", "overseas", "destination", "elope", "international", "abroad", "worldwide", "fly to")


def _client(*, directory_browser_headers: bool = False) -> httpx.Client:
    h: dict[str, str] = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"}
    if directory_browser_headers:
        h.update(RAILWAY_DIRECTORY_HEADERS)
    return httpx.Client(headers=h, timeout=60.0, follow_redirects=True)


def _setup_log() -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOG, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )


def scrape_easy_weddings(client: httpx.Client, max_page: int, delay_s: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pg in range(1, max_page + 1):
        url = f"{EW_BASE}/MarriageCelebrant/{pg}/" if pg > 1 else f"{EW_BASE}/MarriageCelebrant/"
        logging.info("Easy Weddings page %s/%s", pg, max_page)
        r = client.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for card in soup.select(".supplier-card"):
            anchor = card.select_one("span.supplierlisting, span.anchor.supplierlisting")
            if not anchor:
                continue
            name = html_module.unescape(anchor.get("data-supplier-name", "") or "").strip()
            slug = anchor.get("data-supplier-slug", "") or ""
            loc_slug = anchor.get("data-location-slug", "") or ""
            state = html_module.unescape(anchor.get("data-supplier-state", "") or "").strip()
            addr = html_module.unescape(anchor.get("data-ew-address", "") or "").strip()
            suburb = addr.split(",")[0].strip() if addr else ""
            raw_json = anchor.get("data-json", "") or "{}"
            try:
                j = json.loads(html_module.unescape(raw_json))
            except json.JSONDecodeError:
                j = {}
            rating = j.get("reviewScore")
            rev_c = j.get("reviewCount")
            price_txt = ""
            m = re.search(r"\$\s*(\d[\d,]*)", card.get_text(" ", strip=True))
            if m:
                price_txt = f"${m.group(1)}"
            elif j.get("averagePrice"):
                price_txt = f"${int(float(j['averagePrice']))}"
            blob = (anchor.get("data-premium-feature-text", "") + card.get_text(" ", strip=True)).lower()
            dest = any(k in blob for k in DEST_KW)
            profile = f"{EW_BASE}/MarriageCelebrant/{loc_slug}/{slug}/" if loc_slug and slug else ""
            rows.append(
                {
                    "brand_name": name,
                    "suburb": suburb,
                    "state": state,
                    "easy_weddings_rating": rating,
                    "easy_weddings_review_count": rev_c,
                    "easy_weddings_price_from": price_txt,
                    "easy_weddings_profile_url": profile,
                    "is_destination_specialist": dest,
                    "ew_page": pg,
                }
            )
        if pg < max_page:
            time.sleep(delay_s)
    return rows


def scrape_afcc(client: httpx.Client) -> list[dict[str, Any]]:
    """Crawl find-a-marriage-celebrant/ (paginated), then each /celebrant/{slug}/ profile.

    WAF: AFCC may return 403 to automated clients; in that case this returns empty rows. See
    ``scrapers/afcc_scrape.py`` and ``python scripts/afcc_extract_test.py --fixtures``.
    """
    from .afcc_scrape import rows_to_step1_list, scrape_afcc as _afcc_crawl

    try:
        rows = rows_to_step1_list(_afcc_crawl(client))
    except Exception:  # noqa: BLE001
        logging.exception("AFCC directory scrape failed")
        return []
    if not rows or not any(
        (str(r.get("full_name", "")) + str(r.get("summary", ""))).strip() for r in rows
    ):
        logging.warning(
            "AFCC: no usable profile data (empty parse or WAF 403 on search/profile). "
            "If your network gets 200 in a browser, try a residential connection or re-run with manual HTML. "
            "``python -m scrapers.afcc_scrape --slugs sonya-nurthen`` for a single profile test."
        )
    return rows


def _tws_state_from_url(profile_url: str, region_fallback: str) -> str:
    m = re.search(r"/marriage-celebrant/([a-z-]+)/", profile_url, re.I)
    if m:
        slug = m.group(1).lower()
        return TWS_REGION_TO_STATE.get(slug, slug.replace("-", " ").title()[:12])
    return TWS_REGION_TO_STATE.get(region_fallback.lower(), region_fallback.upper()[:3])


def _tws_profile_details(
    client: httpx.Client, profile_url: str, delay_s: float, region_label: str
) -> tuple[str, str, str, str, str]:
    """Return (title_name, location_text, style_snippet, review_count, state)."""
    try:
        time.sleep(delay_s)
        r = client.get(profile_url)
        r.raise_for_status()
    except Exception:  # noqa: BLE001
        return "", "", "", "", _tws_state_from_url(profile_url, region_label)
    s = BeautifulSoup(r.text, "lxml")
    title = ""
    if s.title and s.title.string:
        title = s.title.string.split("|")[0].strip()
    og = s.find("meta", property="og:description")
    style = (og.get("content", "") if og else "")[:2000]
    loc = ""
    for sel in (".supplier-location", ".location", "address"):
        el = s.select_one(sel)
        if el and el.get_text(strip=True):
            loc = el.get_text(" ", strip=True)[:500]
            break
    web = ""
    for a in s.find_all("a", href=True):
        if a.get("href", "").startswith("http") and "theweddingsociety" not in a["href"]:
            if any(x in a["href"].lower() for x in ("http://", "https://")):
                t = a.get_text(strip=True).lower()
                if "website" in t or "visit" in t or not t:
                    web = a["href"]
                    break
    txt = s.get_text(" ", strip=True)
    rev = ""
    m = re.search(r"(\d+)\s*(reviews?|review)\b", txt, re.I)
    if m:
        rev = m.group(1)
    st = _tws_state_from_url(profile_url, region_label)
    if loc:
        lm = re.search(r"\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b", loc)
        if lm:
            st = lm.group(1)
    return title, loc or style[:200], style, rev, st


def discover_tws_directory_template(client: httpx.Client, delay_s: float) -> str:
    """Resolve directory URL template from site (fallback: TWS_DIR_TMPL)."""
    try:
        time.sleep(delay_s)
        r = client.get(TWS_HOME)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        logging.info("TWS homepage fetch skipped (%s); using default directory template", e)
        return TWS_DIR_TMPL
    soup = BeautifulSoup(r.text, "lxml")
    for a in soup.find_all("a", href=True):
        u = urljoin("https://theweddingsociety.co/", a["href"])
        if "theweddingsociety.co" not in u:
            continue
        if "directory" in u.lower() and "marriage-celebrant" in u.lower() and "region=" in u.lower():
            prefix = u.split("region=")[0] + "region={region}"
            logging.info("TWS directory template from homepage: %s", prefix)
            return prefix
    logging.info("TWS: no directory link on homepage; using default template")
    return TWS_DIR_TMPL


def scrape_tws(client: httpx.Client, delay_s: float = 3.0) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    tpl = discover_tws_directory_template(client, delay_s)
    for region in TWS_REGIONS:
        url = tpl.format(region=region)
        logging.info("TWS directory %s", region)
        try:
            time.sleep(delay_s)
            r = client.get(url)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            logging.warning("TWS %s: %s", url, e)
            continue
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if "/marriage-celebrant/" not in h:
                continue
            full = urljoin("https://theweddingsociety.co/", h)
            if full in seen:
                continue
            if full.count("/") < 5:
                continue
            seen.add(full)
            name_guess = unquote(full.rstrip("/").split("/")[-1].replace("-", " ").title())
            title, loc, style, rev, st = _tws_profile_details(client, full, delay_s, region)
            profile_url = full.split("?")[0]
            nm = title or name_guess
            rows.append(
                {
                    "name": nm,
                    "state": st,
                    "profile_url": profile_url,
                    "review_count": rev,
                    "location": loc,
                    "website": "",
                    "style_vibe": style[:1500] if style else "",
                    "wedding_society_profile_url": profile_url,
                }
            )
    return rows


def scrape_wedlockers(
    client: httpx.Client,
    delay_s: float = 3.0,
    *,
    states: list[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page_url in _wedlockers_urls_for_states(states):
        mkey = re.search(r"/planning/([a-z]+)-marriage", page_url, re.I)
        key = (mkey.group(1).lower() if mkey else "nsw")
        state = WL_STATE.get(key, key.upper()[:3])
        logging.info("Wedlockers %s", page_url)
        try:
            time.sleep(delay_s)
            r = client.get(page_url)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001
            logging.warning("Wedlockers %s: %s", page_url, e)
            continue
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.find_all("a", href=True):
            h = a.get("href", "")
            if not h.startswith("b/"):
                continue
            full = urljoin("https://www.wedlockers.com.au/", h)
            if full in seen:
                continue
            seen.add(full)
            slug = h.replace("b/", "").split("?")[0].strip("/")
            nm = slug.replace("-", " ").title()
            rev = ""
            profile_url = full.split("?")[0]
            try:
                time.sleep(delay_s)
                pr = client.get(profile_url)
                pr.raise_for_status()
                ps = BeautifulSoup(pr.text, "lxml")
                txt = ps.get_text(" ", strip=True)
                m = re.search(r"(\d+)\s*reviews?", txt, re.I)
                if m:
                    rev = m.group(1)
            except Exception:  # noqa: BLE001
                pass
            rows.append(
                {
                    "name": nm,
                    "state": state,
                    "profile_url": profile_url,
                    "review_count": rev,
                    "wedlockers_profile_url": profile_url,
                }
            )
    return rows


def scrape_mycelebrantapp(delay_s: float = 3.0) -> list[dict[str, Any]]:
    from playwright.sync_api import sync_playwright

    rows: list[dict[str, Any]] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers(RAILWAY_DIRECTORY_HEADERS)
        page.goto(MCA_BROWSE, wait_until="networkidle", timeout=120_000)
        page.wait_for_timeout(2500)
        for link in page.locator("a[href*='/profile']").all():
            try:
                href = link.get_attribute("href") or ""
                if "/profile" not in href:
                    continue
                full = urljoin("https://app.mycelebrantapp.com/", href.lstrip("/"))
                raw = (link.inner_text() or "").strip()
                lines = [x.strip() for x in raw.split("\n") if x.strip()]
                name = lines[0] if lines else ""
                st = ""
                rev = ""
                sm = re.search(r"\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b", raw)
                if sm:
                    st = sm.group(1)
                blob = " ".join(lines).lower()
                rm = re.search(r"(\d+)\s*(reviews?|stars?)\b", blob, re.I)
                if rm:
                    rev = rm.group(1)
                profile_url = full.split("?")[0]
                rows.append(
                    {
                        "name": name,
                        "state": st,
                        "profile_url": profile_url,
                        "review_count": rev,
                        "state_location": st,
                        "speciality": "",
                        "price_range": "",
                        "mycelebrantapp_profile_url": profile_url,
                    }
                )
            except Exception:  # noqa: BLE001
                continue
        browser.close()
    time.sleep(delay_s)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        u = r.get("mycelebrantapp_profile_url", "") or r.get("profile_url", "")
        if u and u not in seen:
            seen.add(u)
            out.append(r)
    logging.info("MyCelebrantApp: %s profile links", len(out))
    return out


def _norm(s: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9\s]", " ", (s or "").lower()).split())


def _best_ag_match(name: str, ag_names: list[tuple[str, str]]) -> tuple[str | None, int]:
    """Return (celebrant_id, score). ag_names: list of (id, full_name)."""
    if not name:
        return None, 0
    best_id: str | None = None
    best = 0
    nn = _norm(name)
    for cid, fn in ag_names:
        sc = fuzz.token_sort_ratio(nn, _norm(fn))
        if sc > best:
            best, best_id = sc, cid
    return best_id, best


@dataclass
class XrefStats:
    ew_ag: int = 0
    afcc_ag: int = 0
    multi_dir: int = 0
    active: int = 0
    new_rows: int = 0
    tws_ag: int = 0
    wl_ag: int = 0
    mca_ag: int = 0


def step2_crossreference(
    master_path: Path,
    out_path: Path,
    ew: pd.DataFrame,
    afcc: pd.DataFrame,
    tws: pd.DataFrame,
    wl: pd.DataFrame,
    mca: pd.DataFrame,
    threshold: int = 70,
) -> XrefStats:
    master = pd.read_csv(master_path, dtype=str, keep_default_na=False)
    ag_names = [(str(r.celebrant_id), str(r.full_name)) for r in master.itertuples(index=False)]

    extra_cols = [
        "easy_weddings_profile_url",
        "easy_weddings_rating",
        "easy_weddings_review_count",
        "easy_weddings_price_from",
        "afcc_member",
        "afcc_profile_url",
        "wedding_society_profile_url",
        "wedlockers_profile_url",
        "mycelebrantapp_profile_url",
        "directory_listing_count",
        "is_active_market",
        "active_signal_sources",
        "is_destination_specialist",
        "ag_registered",
    ]
    for c in extra_cols:
        if c not in master.columns:
            master[c] = "" if c != "directory_listing_count" else "0"
        if c in ("afcc_member", "is_active_market", "is_destination_specialist"):
            master[c] = "false"
    if "is_destination_specialist" not in master.columns:
        master["is_destination_specialist"] = "false"

    # Track matches per AG id: sources set
    sources_by_id: dict[str, set[str]] = defaultdict(set)
    counts: dict[str, int] = defaultdict(int)
    ew_matched = 0
    afcc_matched = 0
    tws_m = wl_m = mca_m = 0

    def apply_row(cid: str, src: str, updates: dict[str, Any]) -> None:
        m = master["celebrant_id"].astype(str) == cid
        if not m.any():
            return
        idx = master.index[m][0]
        for k, v in updates.items():
            if k not in master.columns or v in ("", None, VERIFY):
                continue
            if k in ("easy_weddings_rating", "easy_weddings_review_count", "directory_listing_count"):
                try:
                    if k == "directory_listing_count":
                        master.at[idx, k] = str(int(float(str(v))))
                    elif k == "easy_weddings_review_count":
                        master.at[idx, k] = str(int(float(str(v))))
                    else:
                        master.at[idx, k] = str(float(v))
                except (TypeError, ValueError):
                    master.at[idx, k] = str(v)
            else:
                cur = str(master.at[idx, k])
                if cur in ("", "false", "0", VERIFY) or k in (
                    "easy_weddings_profile_url",
                    "afcc_profile_url",
                    "wedding_society_profile_url",
                    "wedlockers_profile_url",
                    "mycelebrantapp_profile_url",
                ):
                    master.at[idx, k] = str(v)
        sources_by_id[cid].add(src)
        counts[cid] += 1

    for _, row in ew.iterrows():
        nm = str(row.get("brand_name", ""))
        cid, sc = _best_ag_match(nm, ag_names)
        if cid and sc >= threshold:
            ew_matched += 1
            apply_row(
                cid,
                "EW",
                {
                    "easy_weddings_profile_url": row.get("easy_weddings_profile_url", ""),
                    "easy_weddings_rating": row.get("easy_weddings_rating", ""),
                    "easy_weddings_review_count": row.get("easy_weddings_review_count", ""),
                    "easy_weddings_price_from": row.get("easy_weddings_price_from", ""),
                    "is_destination_specialist": "true" if row.get("is_destination_specialist") else "false",
                },
            )
    for _, row in afcc.iterrows():
        nm = str(row.get("full_name", ""))
        cid, sc = _best_ag_match(nm, ag_names)
        has_afcc = str(row.get("afcc_profile_url", "")).strip().lower().startswith("http")
        has_email = bool(str(row.get("email", "")).strip())
        if cid and sc >= threshold and (has_email or has_afcc):
            afcc_matched += 1
            apply_row(
                cid,
                "AFCC",
                {"afcc_member": "true", "afcc_profile_url": row.get("afcc_profile_url", "")},
            )
    for _, row in tws.iterrows():
        nm = str(row.get("name", ""))
        cid, sc = _best_ag_match(nm, ag_names)
        if cid and sc >= threshold:
            tws_m += 1
            tws_u = str(row.get("wedding_society_profile_url", "") or row.get("profile_url", "")).strip()
            apply_row(cid, "TWS", {"wedding_society_profile_url": tws_u})
    for _, row in wl.iterrows():
        nm = str(row.get("name", ""))
        cid, sc = _best_ag_match(nm, ag_names)
        if cid and sc >= threshold:
            wl_m += 1
            wl_u = str(row.get("wedlockers_profile_url", "") or row.get("profile_url", "")).strip()
            apply_row(cid, "WL", {"wedlockers_profile_url": wl_u})
    for _, row in mca.iterrows():
        nm = str(row.get("name", ""))
        cid, sc = _best_ag_match(nm, ag_names)
        if cid and sc >= threshold:
            mca_m += 1
            mca_u = str(row.get("mycelebrantapp_profile_url", "") or row.get("profile_url", "")).strip()
            apply_row(cid, "MCA", {"mycelebrantapp_profile_url": mca_u})

    # Finalise directory_listing_count, active flags
    stats = XrefStats()
    stats.ew_ag = ew_matched
    stats.afcc_ag = afcc_matched
    stats.tws_ag = tws_m
    stats.wl_ag = wl_m
    stats.mca_ag = mca_m

    for i, r in master.iterrows():
        cid = str(r.get("celebrant_id", ""))
        srcs = sources_by_id.get(cid, set())
        n = len(srcs)
        if n:
            master.at[i, "directory_listing_count"] = str(n)
            master.at[i, "active_signal_sources"] = "|".join(sorted(srcs))
            master.at[i, "is_active_market"] = "true"
            stats.active += 1
        if n >= 2:
            stats.multi_dir += 1

    # New rows: scraped EW not matched (sample — only EW brand names for simplicity)
    new_rows: list[dict[str, Any]] = []
    for _, row in ew.iterrows():
        nm = str(row.get("brand_name", ""))
        cid, sc = _best_ag_match(nm, ag_names)
        if not cid or sc < threshold:
            slug = (row.get("easy_weddings_profile_url", "") or "").rstrip("/").split("/")[-1][:24] or "x"
            safe = re.sub(r"[^A-Za-z0-9]+", "-", slug).strip("-") or "x"
            nr = {c: VERIFY for c in master.columns}
            nr["celebrant_id"] = f"CEL-EWDIR-{safe}"[:40]
            nr["full_name"] = nm
            nr["ag_display_name"] = nm
            nr["data_source"] = "EasyWeddings_directory"
            nr["ag_registered"] = VERIFY
            nr["state"] = str(row.get("state", "") or VERIFY)
            nr["easy_weddings_profile_url"] = str(row.get("easy_weddings_profile_url", ""))
            nr["is_active_market"] = "true"
            nr["active_signal_sources"] = "EW"
            nr["directory_listing_count"] = "1"
            if nr["celebrant_id"] not in set(master["celebrant_id"].astype(str)):
                new_rows.append(nr)
                stats.new_rows += 1

    if new_rows:
        master = pd.concat([master, pd.DataFrame(new_rows)], ignore_index=True)

    master.to_csv(out_path, index=False)
    return stats


def print_step2_summary(stats: XrefStats, master: pd.DataFrame) -> None:
    act = (master["is_active_market"].astype(str).str.lower() == "true").sum()
    print("--- Step 2 cross-reference summary ---")
    print(f"AG register celebrants found in Easy Weddings: {stats.ew_ag}")
    print(f"AG register celebrants found in AFCC: {stats.afcc_ag}")
    print(f"AG register celebrants in 2+ directories: {stats.multi_dir}")
    print(f"Total with is_active_market = True: {act}")
    print(f"Not in AG register (new additions): {stats.new_rows}")
    print(f"(TWS matches: {stats.tws_ag}, Wedlockers: {stats.wl_ag}, MyCelebrantApp: {stats.mca_ag})")


def print_output_summary(master: pd.DataFrame) -> None:
    """High-level counts after Step 2 (Places fields usually empty until Step 3)."""
    n = len(master)
    act = master["is_active_market"].astype(str).str.lower() == "true"
    ew = master["easy_weddings_profile_url"].astype(str).str.startswith("http")
    afcc = master["afcc_member"].astype(str).str.lower() == "true"
    multi = pd.to_numeric(master.get("directory_listing_count", 0), errors="coerce").fillna(0) >= 2
    dest = master.get("is_destination_specialist", pd.Series(["false"] * len(master)))
    dest_b = dest.astype(str).str.lower() == "true"
    gpid = master.get("google_place_id", pd.Series([VERIFY] * n)).astype(str)
    places_done = ((gpid != VERIFY) & (gpid.str.len() > 5)).sum()
    gr = pd.to_numeric(master.get("google_rating", ""), errors="coerce")
    gsub = gr[act & gr.notna()]
    avg_g = f"{gsub.mean():.2f}" if len(gsub) else "n/a"
    print("\n--- OUTPUT SUMMARY (post Step 2) ---")
    print(f"Total in master: {n}")
    print(f"Active market (is_active_market=True): {int(act.sum())}")
    print(f"Places enriched (google_place_id set): {int(places_done)}")
    print(f"Average Google rating (active, non-null): {avg_g}")
    print(f"In Easy Weddings (profile URL set): {int(ew.sum())}")
    print(f"AFCC members (flag): {int(afcc.sum())}")
    print(f"In 2+ directories: {int(multi.sum())}")
    print(f"International/destination specialists (EW flag): {int(dest_b.sum())}")


def print_multi_directory_railway_footer(stats: XrefStats, master: pd.DataFrame) -> None:
    """Post–Step 2 counts for Railway multi-directory job (EW loaded from disk)."""
    act = int((master["is_active_market"].astype(str).str.lower() == "true").sum())
    print(f"Easy Weddings matches: {stats.ew_ag} (from v2, already done)")
    print(f"Wedlockers matches: {stats.wl_ag}")
    print(f"Wedding Society matches: {stats.tws_ag}")
    print(f"MyCelebrantApp matches: {stats.mca_ag}")
    print(f"In 2+ directories: {stats.multi_dir}")
    print(f"Total is_active_market = True: {act}")


def run_step1(
    ew_pages: int = 15,
    ew_delay: float = 1.0,
    *,
    skip_ew: bool = False,
    skip_afcc: bool = False,
    skip_mycelebrantapp: bool = False,
    wedlockers_states: list[str] | None = None,
    request_delay_s: float = 3.0,
    directory_browser_headers: bool = True,
) -> None:
    """Scrape directory CSVs under ``data/``. When ``skip_ew``, reuse ``ew_celebrants.csv``."""
    _setup_log()
    data = _ROOT / "data"
    data.mkdir(exist_ok=True)
    ew_path = data / "ew_celebrants.csv"
    afcc_path = data / "afcc_celebrants.csv"
    tws_path = data / "tws_celebrants.csv"
    wl_path = data / "wedlockers_celebrants.csv"
    mca_path = data / "mycelebrantapp_celebrants.csv"

    with _client(directory_browser_headers=directory_browser_headers) as client:
        if not skip_ew:
            ew_rows = scrape_easy_weddings(client, ew_pages, ew_delay)
            pd.DataFrame(ew_rows).to_csv(ew_path, index=False)
            logging.info("Wrote %s (%s rows)", ew_path, len(ew_rows))
        else:
            if not ew_path.is_file():
                raise FileNotFoundError(f"skip_ew=True but missing {ew_path}")
            logging.info("skip_ew=True: reusing existing %s", ew_path)

        if not skip_afcc:
            pd.DataFrame(scrape_afcc(client)).to_csv(afcc_path, index=False)
            logging.info("Wrote AFCC -> %s", afcc_path)
        else:
            logging.info("skip_afcc=True: not updating %s", afcc_path)

        pd.DataFrame(scrape_tws(client, delay_s=request_delay_s)).to_csv(tws_path, index=False)
        logging.info("Wrote TWS -> %s", tws_path)
        pd.DataFrame(
            scrape_wedlockers(client, delay_s=request_delay_s, states=wedlockers_states)
        ).to_csv(wl_path, index=False)
        logging.info("Wrote Wedlockers -> %s", wl_path)
        if not skip_mycelebrantapp:
            pd.DataFrame(scrape_mycelebrantapp(delay_s=request_delay_s)).to_csv(mca_path, index=False)
            logging.info("Wrote MyCelebrantApp -> %s", mca_path)
        else:
            logging.info("skip_mycelebrantapp=True: empty %s (no Playwright/Chromium)", mca_path)
            pd.DataFrame(columns=list(MCA_CSV_COLUMNS)).to_csv(mca_path, index=False)


def run_step2(
    *,
    master_v1: Path | str | None = None,
    output_file: Path | str | None = None,
    ew_path: Path | str | None = None,
    afcc_path: Path | str | None = None,
    tws_path: Path | str | None = None,
    wl_path: Path | str | None = None,
    mca_path: Path | str | None = None,
) -> tuple[XrefStats, pd.DataFrame]:
    """Cross-reference directory CSVs to AG master; write ``celebrants_master_v*.csv``."""
    _setup_log()
    data = _ROOT / "data"
    m1 = Path(master_v1) if master_v1 else data / "celebrants_master_v1.csv"
    if not m1.is_absolute():
        m1 = _ROOT / m1
    out = Path(output_file) if output_file else data / "celebrants_master_v2.csv"
    if not out.is_absolute():
        out = _ROOT / out
    ew_p = Path(ew_path) if ew_path else data / "ew_celebrants.csv"
    afcc_p = Path(afcc_path) if afcc_path else data / "afcc_celebrants.csv"
    tws_p = Path(tws_path) if tws_path else data / "tws_celebrants.csv"
    wl_p = Path(wl_path) if wl_path else data / "wedlockers_celebrants.csv"
    mca_p = Path(mca_path) if mca_path else data / "mycelebrantapp_celebrants.csv"

    def _abs(pth: Path) -> Path:
        return pth if pth.is_absolute() else _ROOT / pth

    ew_p, afcc_p, tws_p, wl_p, mca_p = map(_abs, (ew_p, afcc_p, tws_p, wl_p, mca_p))
    m1 = _abs(m1)
    out = _abs(out)

    if not m1.is_file():
        raise FileNotFoundError(f"missing master v1: {m1}")

    ew = pd.read_csv(ew_p, dtype=str, keep_default_na=False) if ew_p.is_file() else pd.DataFrame()
    afcc = pd.read_csv(afcc_p, dtype=str, keep_default_na=False) if afcc_p.is_file() else pd.DataFrame()
    tws = pd.read_csv(tws_p, dtype=str, keep_default_na=False) if tws_p.is_file() else pd.DataFrame()
    wl = pd.read_csv(wl_p, dtype=str, keep_default_na=False) if wl_p.is_file() else pd.DataFrame()
    mca = pd.read_csv(mca_p, dtype=str, keep_default_na=False) if mca_p.is_file() else pd.DataFrame()
    stats = step2_crossreference(m1, out, ew, afcc, tws, wl, mca)
    m2 = pd.read_csv(out, dtype=str, keep_default_na=False)
    print_step2_summary(stats, m2)
    print_output_summary(m2)
    return stats, m2


def run_step3_places(master_v2: Path, threshold_fuzzy: int = 60) -> None:
    """Google Places enrichment — requires GOOGLE_PLACES_API_KEY; costs money (not run here)."""
    key = (os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not key:
        logging.error("No Google API key; abort Step 3")
        return
    df = pd.read_csv(master_v2, dtype=str, keep_default_na=False)
    mask = (df["is_active_market"].astype(str).str.lower() == "true") | (
        df.get("content_tier", pd.Series([""] * len(df))).astype(str).str.lower() == "featured"
    )
    sub = df.loc[mask].copy()
    logging.info("Step3 Places would run on %s rows (skipped — awaiting approval).", len(sub))
    print(
        "Step 3 (Google Places) is intentionally not executed. "
        "Implement API calls here after cost approval; see celebrant_places_enrichment.py pattern."
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--step1", action="store_true", help="Run directory scrapers")
    ap.add_argument("--step2", action="store_true", help="Cross-reference to master v2")
    ap.add_argument("--step3", action="store_true", help="Google Places (paid) — use after approval")
    ap.add_argument("--ew-pages", type=int, default=15, metavar="N", help="Easy Weddings last page number")
    ap.add_argument("--ew-only", action="store_true", help="Only scrape Easy Weddings in step 1")
    ap.add_argument("--ew-delay", type=float, default=1.0, help="Seconds between EW pages")
    args = ap.parse_args()

    if not args.step1 and not args.step2 and not args.step3:
        ap.print_help()
        print("\nSpecify at least one of: --step1 --step2 --step3", file=sys.stderr)
        return 1

    _setup_log()
    data = _ROOT / "data"
    data.mkdir(exist_ok=True)
    ew_path = data / "ew_celebrants.csv"
    afcc_path = data / "afcc_celebrants.csv"
    tws_path = data / "tws_celebrants.csv"
    wl_path = data / "wedlockers_celebrants.csv"
    mca_path = data / "mycelebrantapp_celebrants.csv"
    master_v1 = data / "celebrants_master_v1.csv"
    master_v2 = data / "celebrants_master_v2.csv"

    if args.step1:
        if args.ew_only:
            with _client(directory_browser_headers=False) as client:
                ew_rows = scrape_easy_weddings(client, args.ew_pages, args.ew_delay)
                pd.DataFrame(ew_rows).to_csv(ew_path, index=False)
                logging.info("Wrote %s (%s rows)", ew_path, len(ew_rows))
            from .afcc_scrape import AFCC_CSV_COLUMNS

            for p, empty in (
                (afcc_path, list(AFCC_CSV_COLUMNS) + ["phone"]),
                (
                    tws_path,
                    [
                        "name",
                        "state",
                        "profile_url",
                        "review_count",
                        "location",
                        "website",
                        "style_vibe",
                        "wedding_society_profile_url",
                    ],
                ),
                (wl_path, ["name", "state", "profile_url", "review_count", "wedlockers_profile_url"]),
                (mca_path, list(MCA_CSV_COLUMNS)),
            ):
                pd.DataFrame(columns=empty).to_csv(p, index=False)
        else:
            run_step1(
                ew_pages=args.ew_pages,
                ew_delay=args.ew_delay,
                skip_ew=False,
                skip_afcc=False,
                request_delay_s=0.5,
                directory_browser_headers=False,
            )
        if args.ew_pages <= 3:
            sample = pd.read_csv(ew_path).head(12)
            print("\n--- Easy Weddings extract (first 12 rows, pages 1–%s) ---\n%s" % (args.ew_pages, sample.to_string()))

    if args.step2:
        if not master_v1.is_file():
            print(f"ERROR: missing {master_v1}", file=sys.stderr)
            return 1
        run_step2(output_file=master_v2)

    if args.step3:
        run_step3_places(master_v2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
