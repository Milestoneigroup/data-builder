"""Scrape venue websites for ABN candidates, validate with ATO mod-89, write to Supabase.

Populates ``abn_from_website``, ``abn_website_source_url``, and ``abn_scrape_attempted_at`` only
(A/B test session). Does **not** modify verified ``abn`` or name-search columns.

Run:

    python -m scrapers.scrape_venue_websites_for_abn

Environment:

- ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY`` with service role)
- ``TEST_VENUE_IDS`` — optional comma-separated venue UUIDs; if set, only those rows are processed.
  If unset, processes venues where ``abn_scrape_attempted_at`` IS NULL.

Politeness: User-Agent identifies MIG; respects robots.txt; 5s between venues, 2s between page fetches;
HTTP timeout 25s per request.

ABN checksum: https://www.abr.business.gov.au/HelpAbnFormat.aspx (modulus 89).
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dotenv import load_dotenv

from scrapers.abn_util import abn_checksum_valid, normalize_abn_digits

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

LOG = logging.getLogger(__name__)

USER_AGENT = "MIG-Data-Builder/1.0 (data@milestoneigroup.com; venue ABN enrichment)"
PAGE_TIMEOUT_S = 25.0
DELAY_BETWEEN_PAGES_S = 2.0
DELAY_BETWEEN_VENUES_S = 5.0

# Label / context windows (characters of surrounding text)
_LABEL_WINDOW = 30
_CONTEXT_WINDOW = 120

_ABN_CHUNK = r"(?:\d{2}\s*\d{3}\s*\d{3}\s*\d{3}|\d{11})"
ABN_NEAR_LABEL_RE = re.compile(
    rf"(?i)(?:ABN|Australian\s+Business\s+Number)[\s:–—-]*({_ABN_CHUNK})",
    re.I,
)
ABN_STANDALONE_RE = re.compile(rf"\b({_ABN_CHUNK})\b")

_PATH_KEYWORDS = (
    "contact",
    "about",
    "terms",
    "privacy",
    "legal",
    "booking",
    "faq",
    "pricing",
    "wedding",
    "footer",
)

_GUESS_PATHS = (
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/terms",
    "/terms-and-conditions",
    "/terms-and-conditions/",
    "/privacy",
    "/privacy-policy",
    "/legal",
    "/booking",
    "/faq",
    "/pricing",
)

_CONTEXT_HIGH_MARKERS = re.compile(
    r"(?i)\b(abn|australian\s+business\s+number|gst|business\s+number|acn)\b",
)

# Prefer operator-operated domains when ``website`` points at a listings marketplace.
_MARKETPLACE_RD_HINTS = frozenset(
    {"wedshed.com.au", "easyweddings.com.au", "hitched.com.au"}
)

# Australian second-level ccTLD bundles (coarse registrable-domain grouping).
_KNOWN_AU_SECOND_LEVEL = frozenset(
    {"com.au", "net.au", "org.au", "edu.au", "gov.au", "asn.au", "id.au"}
)


@dataclass(frozen=True)
class AbnHit:
    """Validated ABN (11 digits), where it was seen, and confidence tier."""

    digits: str
    source_url: str
    confidence: str  # HIGH | MEDIUM


def _norm_netloc(netloc: str) -> str:
    n = netloc.split("@")[-1].lower()
    if n.startswith("www."):
        return n[4:]
    return n


def registrable_domain(netloc: str) -> str:
    """Group subdomains (shop.foo.com.au -> foo.com.au). Excludes cross-site marketplaces."""
    host = _norm_netloc(netloc).split(":")[0]
    parts = host.split(".")
    if len(parts) >= 3 and ".".join(parts[-2:]) in _KNOWN_AU_SECOND_LEVEL:
        return ".".join(parts[-3:])
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def same_registrable_site(url: str, seed_site_url: str) -> bool:
    """Same venue site cluster (scheme http(s), matching registrable domain)."""
    u, seed = urlparse(url), urlparse(seed_site_url)
    if u.scheme not in ("http", "https") or seed.scheme not in ("http", "https"):
        return False
    return registrable_domain(u.netloc) == registrable_domain(seed.netloc)


def venue_seed_website(row: dict[str, Any]) -> str:
    """Prefer operator URL when ``website`` is a known marketplace listing."""
    primary = str(row.get("website") or "").strip()
    google = str(row.get("website_from_google") or "").strip()

    def ok_http(u: str) -> bool:
        return bool(u) and u.lower().startswith("http")

    if ok_http(primary):
        try:
            rd_p = registrable_domain(urlparse(primary).netloc)
        except Exception:
            rd_p = ""
        if rd_p in _MARKETPLACE_RD_HINTS and ok_http(google):
            return google
        return primary
    if ok_http(google):
        return google
    return ""


def _page_text(html: str) -> str:
    """Visible-ish text for regex search; do not retain raw HTML beyond this."""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def _link_candidates(soup: BeautifulSoup, base_url: str, seed_site_url: str) -> list[str]:
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = str(a.get("href", "")).strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        abs_url = urljoin(base_url, href)
        if not same_registrable_site(abs_url, seed_site_url):
            continue
        out.append(abs_url.split("#")[0].rstrip("/") or abs_url)
    return out


def _score_url(url: str) -> int:
    p = urlparse(url)
    path = f"{p.path} {p.query}".lower()
    score = sum(10 for k in _PATH_KEYWORDS if k in path)
    return score


def _ordered_fetch_urls(home_url: str, soup: BeautifulSoup, seed_site_url: str) -> list[str]:
    """Home first, then URLs on the same registrable domain as seed (deduped)."""
    seen: set[str] = set()
    ordered: list[str] = []

    def add(u: str) -> None:
        u = u.split("#")[0]
        if u in seen:
            return
        if not same_registrable_site(u, seed_site_url):
            return
        seen.add(u)
        ordered.append(u)

    add(home_url)
    guessed = [urljoin(home_url, p) for p in _GUESS_PATHS]
    for g in sorted(guessed, key=_score_url, reverse=True):
        add(g)
    discovered = _link_candidates(soup, home_url, seed_site_url)
    ranked = sorted(set(discovered), key=lambda u: (_score_url(u), len(u)), reverse=True)
    for u in ranked[:25]:
        add(u)
    return ordered[:18]


def _confidence_for_match(
    full_text: str, match_start: int, match_end: int, page_url: str
) -> str:
    """HIGH if explicit ABN label nearby; MEDIUM with policy-page context; else MEDIUM."""
    lo = max(0, match_start - _LABEL_WINDOW)
    hi = min(len(full_text), match_end + _LABEL_WINDOW)
    window_label = full_text[lo:hi]
    if re.search(r"(?i)\babn\b|australian\s+business\s+number", window_label):
        return "HIGH"
    lo2 = max(0, match_start - _CONTEXT_WINDOW)
    hi2 = min(len(full_text), match_end + _CONTEXT_WINDOW)
    ctx = full_text[lo2:hi2]
    path_l = urlparse(page_url).path.lower()
    policy_page = any(
        x in path_l for x in ("terms", "privacy", "legal", "about", "contact", "footer")
    )
    if policy_page and _CONTEXT_HIGH_MARKERS.search(ctx):
        return "HIGH"
    if policy_page:
        return "MEDIUM"
    return "MEDIUM"


def _extract_best_abn_from_text(text: str, page_url: str) -> AbnHit | None:
    hits: list[AbnHit] = []

    for m in ABN_NEAR_LABEL_RE.finditer(text):
        nd = normalize_abn_digits(m.group(1))
        if nd and abn_checksum_valid(nd):
            hits.append(
                AbnHit(
                    digits=nd,
                    source_url=page_url,
                    confidence=_confidence_for_match(text, m.start(), m.end(), page_url),
                )
            )

    for m in ABN_STANDALONE_RE.finditer(text):
        nd = normalize_abn_digits(m.group(1))
        if nd and abn_checksum_valid(nd):
            if any(h.digits == nd for h in hits):
                continue
            hits.append(
                AbnHit(
                    digits=nd,
                    source_url=page_url,
                    confidence=_confidence_for_match(text, m.start(), m.end(), page_url),
                )
            )

    if not hits:
        return None
    hits.sort(key=lambda h: (0 if h.confidence == "HIGH" else 1, h.digits))
    return hits[0]


def _is_blocked_response(status_code: int, body: str) -> bool:
    if status_code in (403, 429, 503):
        return True
    sample = body[:12000].lower()
    if "cf-mitigated" in sample or "cf-ray" in sample:
        if "challenge" in sample or "checking your browser" in sample or "just a moment" in sample:
            return True
    if "attention required" in sample and "cloudflare" in sample:
        return True
    if "enable javascript" in sample and "captcha" in sample:
        return True
    return False


def _fetch_robots(
    client: httpx.Client, origin: str
) -> RobotFileParser | None:
    robots_url = urljoin(origin, "/robots.txt")
    try:
        r = client.get(
            robots_url,
            timeout=PAGE_TIMEOUT_S,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        if r.status_code >= 400:
            return None
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.parse(r.text.splitlines())
        return rp
    except Exception:
        LOG.debug("robots.txt fetch failed for %s", robots_url, exc_info=True)
        return None


def _allowed(rp: RobotFileParser | None, url: str) -> bool:
    if rp is None:
        return True
    try:
        return rp.can_fetch(USER_AGENT, url)
    except Exception:
        return True


def scrape_venue_abn(
    client: httpx.Client,
    website: str,
    robots_cache: dict[str, RobotFileParser | None],
) -> tuple[AbnHit | None, str, int]:
    """Returns (hit or None, outcome_code, invalid_checksum_count_on_site).

    outcome_code: found | no_abn | blocked | no_website | fetch_error | off_domain_redirect
    """
    w = (website or "").strip()
    if not w or not w.lower().startswith("http"):
        return None, "no_website", 0

    invalid_near = 0

    try:
        r0 = client.get(
            w,
            timeout=PAGE_TIMEOUT_S,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
    except Exception as e:
        LOG.info("homepage fetch error %s: %s", w, e)
        return None, "fetch_error", 0

    if _is_blocked_response(r0.status_code, r0.text):
        return None, "blocked", 0

    if r0.status_code >= 400:
        LOG.info("homepage HTTP %s for %s", r0.status_code, w)
        return None, "fetch_error", 0

    final_home = str(r0.url)
    if registrable_domain(urlparse(w).netloc) != registrable_domain(urlparse(final_home).netloc):
        LOG.info(
            "homepage registrable-domain mismatch seed=%s final=%s for %s",
            registrable_domain(urlparse(w).netloc),
            registrable_domain(urlparse(final_home).netloc),
            w,
        )
        return None, "off_domain_redirect", 0

    origin = f"{urlparse(final_home).scheme}://{urlparse(final_home).netloc}"

    if origin not in robots_cache:
        time.sleep(DELAY_BETWEEN_PAGES_S)
        robots_cache[origin] = _fetch_robots(client, origin)
    rp = robots_cache[origin]

    soup = BeautifulSoup(r0.text, "lxml")
    urls = _ordered_fetch_urls(final_home, soup, w)

    for i, url in enumerate(urls):
        if i > 0:
            time.sleep(DELAY_BETWEEN_PAGES_S)
        if not _allowed(rp, url):
            LOG.debug("robots disallow %s", url)
            continue
        if i == 0:
            html = r0.text
        else:
            try:
                rr = client.get(
                    url,
                    timeout=PAGE_TIMEOUT_S,
                    follow_redirects=True,
                    headers={"User-Agent": USER_AGENT},
                )
            except Exception as e:
                LOG.info("fetch error %s: %s", url, e)
                continue
            if _is_blocked_response(rr.status_code, rr.text):
                return None, "blocked", invalid_near
            if rr.status_code >= 400:
                continue
            html = rr.text
            url = str(rr.url)

        if registrable_domain(urlparse(url).netloc) != registrable_domain(
            urlparse(w).netloc
        ):
            LOG.debug("skip page: post-redirect host not venue domain %s", url)
            del html
            continue

        text = _page_text(html)
        del html
        invalid_near += _count_invalid_abn_patterns(text)
        hit = _extract_best_abn_from_text(text, url)
        del text
        if hit:
            return hit, "found", invalid_near

    return None, "no_abn", invalid_near


def _count_invalid_abn_patterns(text: str) -> int:
    """Count 11-digit / formatted candidates that fail checksum (rough signal for bad site data)."""
    n = 0
    seen: set[str] = set()
    for m in ABN_STANDALONE_RE.finditer(text):
        nd = normalize_abn_digits(m.group(1))
        if not nd or nd in seen:
            continue
        seen.add(nd)
        if not abn_checksum_valid(nd):
            n += 1
    for m in ABN_NEAR_LABEL_RE.finditer(text):
        nd = normalize_abn_digits(m.group(1))
        if not nd or nd in seen:
            continue
        seen.add(nd)
        if not abn_checksum_valid(nd):
            n += 1
    return n


def _venue_query_ids(supabase: Any, test_ids: list[str] | None) -> list[dict[str, Any]]:
    if test_ids:
        resp = (
            supabase.table("venues")
            .select("id, website, website_from_google, name")
            .in_("id", test_ids)
            .execute()
        )
        rows = list(getattr(resp, "data", None) or [])
        order = {v: i for i, v in enumerate(test_ids)}
        rows.sort(key=lambda r: order.get(str(r.get("id")), 9999))
        return rows

    batch: list[dict[str, Any]] = []
    offset = 0
    page = 500
    while True:
        resp = (
            supabase.table("venues")
            .select("id, website, website_from_google, name")
            .is_("abn_scrape_attempted_at", "null")
            .range(offset, offset + page - 1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if not rows:
            break
        batch.extend(rows)
        if len(rows) < page:
            break
        offset += page
    return batch


def _today_utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def run() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from data_builder.config import get_settings

    settings = get_settings()
    sb_url = (settings.supabase_url or os.getenv("SUPABASE_URL") or "").strip()
    sb_key = (
        settings.supabase_service_role_key
        or settings.supabase_key
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_KEY")
        or ""
    ).strip()
    if not sb_url or not sb_key:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")
        return 1

    raw_test = os.getenv("TEST_VENUE_IDS", "").strip()
    test_ids = [x.strip() for x in raw_test.split(",") if x.strip()] if raw_test else None

    from supabase import create_client

    supabase = create_client(sb_url, sb_key)

    rows = _venue_query_ids(supabase, test_ids)
    LOG.info("Venues to process: %s", len(rows))

    stats = {
        "found": 0,
        "no_abn": 0,
        "blocked": 0,
        "no_website": 0,
        "fetch_error": 0,
        "off_domain_redirect": 0,
        "invalid_checksum_seen": 0,
    }
    abn_locations: dict[str, int] = {}

    robots_cache: dict[str, RobotFileParser | None] = {}

    with httpx.Client() as client:
        for i, row in enumerate(rows):
            if i > 0:
                time.sleep(DELAY_BETWEEN_VENUES_S)
            vid = str(row.get("id", ""))
            name = str(row.get("name", ""))[:80]
            raw_primary = str(row.get("website", "") or "").strip()
            seed = venue_seed_website(row)
            if seed != raw_primary and seed:
                LOG.info(
                    "venue=%s marketplace_primary_override raw=%s seed=%s name=%s",
                    vid,
                    raw_primary[:120],
                    seed[:120],
                    name,
                )

            hit, outcome, invalid_ct = scrape_venue_abn(client, seed, robots_cache)
            stats["invalid_checksum_seen"] += invalid_ct

            update: dict[str, Any] = {"abn_scrape_attempted_at": _today_utc_date()}

            if outcome == "blocked":
                stats["blocked"] += 1
                LOG.warning("venue=%s blocked_or_challenged name=%s", vid, name)
            elif outcome == "no_website":
                stats["no_website"] += 1
                LOG.info("venue=%s no_website name=%s", vid, name)
            elif outcome == "off_domain_redirect":
                stats["off_domain_redirect"] += 1
                LOG.info("venue=%s off_domain_redirect name=%s", vid, name)
            elif outcome == "fetch_error":
                stats["fetch_error"] += 1
                LOG.info("venue=%s fetch_error name=%s", vid, name)
            elif hit:
                stats["found"] += 1
                update["abn_from_website"] = hit.digits
                update["abn_website_source_url"] = hit.source_url
                path = urlparse(hit.source_url).path.lower() or "/"
                loc = (
                    "home"
                    if path in ("", "/")
                    or path.rstrip("/") == urlparse(seed).path.rstrip("/")
                    else (
                        "terms/privacy/legal"
                        if any(x in path for x in ("terms", "privacy", "legal"))
                        else (
                            "contact/about"
                            if any(x in path for x in ("contact", "about"))
                            else "other"
                        )
                    )
                )
                abn_locations[loc] = abn_locations.get(loc, 0) + 1
                LOG.info(
                    "venue=%s FOUND abn=%s conf=%s url=%s name=%s",
                    vid,
                    hit.digits,
                    hit.confidence,
                    hit.source_url,
                    name,
                )
            else:
                stats["no_abn"] += 1
                LOG.info("venue=%s no_valid_abn name=%s", vid, name)

            try:
                supabase.table("venues").update(update).eq("id", vid).execute()
            except Exception as e:
                LOG.exception("Supabase update failed venue=%s: %s", vid, e)
                return 1

    LOG.info(
        "Summary: found=%s no_abn=%s blocked=%s no_website=%s off_domain=%s fetch_error=%s "
        "invalid_checksum_candidates=%s abn_page_locations=%s",
        stats["found"],
        stats["no_abn"],
        stats["blocked"],
        stats["no_website"],
        stats["off_domain_redirect"],
        stats["fetch_error"],
        stats["invalid_checksum_seen"],
        abn_locations,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
