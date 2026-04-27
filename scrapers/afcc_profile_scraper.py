"""AFCC celebrant profile scraper: discover slugs, fetch profiles, upsert to Supabase.

- Discovery: https://www.afcc.com.au/find-a-marriage-celebrant/?paged={N} → data/afcc_slugs.csv
- Profiles: main URL, ``?tab=services``, and ``?tab=leave-a-review`` (testimonials). Two-second
  delay after every HTTP request (incl. before the next celebrant).

Run: ``python -m scrapers.afcc_profile_scraper --run-now`` (or ``--test`` for 10 slugs).
Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY`` env).
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup, Tag
from dotenv import load_dotenv

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger(__name__)

AFCC_ORIGIN = "https://www.afcc.com.au"
SEARCH_TMPL = AFCC_ORIGIN + "/find-a-marriage-celebrant/?paged={n}"
CELEBRANT_PATH_RE = re.compile(
    r"(?:https?://(?:www\.)?afcc\.com\.au)?/celebrant/([^/\"'?\s#]+)/?",
    re.I,
)
HREF_CELEBRANT = re.compile(r"https?://[^\"'\\s]*afcc\.com\.au/celebrant/([^/\"'?\s#]+)/", re.I)
AUS_MOBILE = re.compile(r"04\d{2}[\s.-]?\d{3}[\s.-]?\d{3}", re.I)
AUS_STATE = re.compile(
    r"\b(NSW|VIC|QLD|SA|WA|TAS|ACT|NT|"
    r"New South Wales|Victoria|Queensland|South Australia|Western Australia|"
    r"Tasmania|Australian Capital Territory|Northern Territory)\b",
    re.I,
)
SUMMARY_MAX = 500
REQUEST_TIMEOUT = 60.0
REQUEST_DELAY_S = 2.0
CHECKPOINT_EVERY = 50
TEST_LIMIT = 10
DATA_DIR = _ROOT / "data"
SLUGS_CSV = DATA_DIR / "afcc_slugs.csv"
CHECKPOINT_CSV = DATA_DIR / "afcc_checkpoint.csv"
SYD = ZoneInfo("Australia/Sydney")

# Fallback slugs for ``--test`` if discovery is blocked or the slug file is empty.
FALLBACK_TEST_SLUGS = [
    "sonya-nurthen",
    "isabelle-gagnet",
    "tracey-bernadette-bradbery",
    "catherine-kennedy",
    "lorraine-moir",
    "steven-nagy",
    "dianne-joy-woods",
    "gillian-lloyd",
    "jennifer-cecere",
    "catherine-cecere",
]
_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36 MilestoneDataBuilder/0.1"
)


@dataclass
class ProfileRow:
    afcc_slug: str
    full_name: str | None
    mobile: str | None
    email: str | None
    suburb: str | None
    state: str | None
    afcc_profile_url: str
    services: str | None
    summary: str | None
    testimonial_1: str | None
    testimonial_2: str | None
    testimonial_3: str | None
    website: str | None
    scraped_date: str
    matched_celebrant_id: str | None = None


def _load_settings() -> Any:
    from data_builder.config import get_settings

    return get_settings()


def _default_headers() -> dict[str, str]:
    s = _load_settings()
    ua = (s.scraper_user_agent or _DEFAULT_UA).strip() or _DEFAULT_UA
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
    }


def _sleep() -> None:
    time.sleep(REQUEST_DELAY_S)


def _captcha_or_blocked(html: str) -> bool:
    s = (html or "").lower()
    return "sgcaptcha" in s or "well-known/sgcaptcha" in s or "please verify" in s[:2000]


def _get_html(client: httpx.Client, url: str) -> str | None:
    try:
        r = client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True, headers=_default_headers())
    except httpx.RequestError as e:  # pragma: no cover
        LOG.warning("Request failed %s: %s", url, e)
        return None
    if r.status_code in (202, 403) or (r.status_code == 200 and len(r.text) < 400 and _captcha_or_blocked(r.text)):
        LOG.warning("Blocked or captcha-style response %s (status %s, len %s)", url, r.status_code, len(r.text or ""))
        return None
    if r.status_code != 200:
        LOG.warning("GET %s → %s", url, r.status_code)
        return None
    if _captcha_or_blocked(r.text):
        LOG.warning("Challenge page for %s", url)
        return None
    return r.text


def _extract_slugs_from_html(html: str) -> set[str]:
    found: set[str] = set()
    for m in HREF_CELEBRANT.finditer(html):
        s = m.group(1).strip()
        if s and s.lower() not in ("", "www", "http:", "https:"):
            found.add(s)
    for m in CELEBRANT_PATH_RE.finditer(html):
        s = m.group(1).strip()
        if s and "/" not in s and "celebrant" not in s.lower():
            found.add(s)
    return {x for x in found if not x.lower().startswith("http")}


def _page_empty_search(html: str) -> bool:
    if not html or len(html) < 500:
        return True
    # No celebrant result cards / links
    s = _extract_slugs_from_html(html)
    return len(s) == 0


def discover_all_slugs(client: httpx.Client) -> list[str]:
    all_slugs: set[str] = set()
    n = 1
    while True:
        url = SEARCH_TMPL.format(n=n)
        _sleep()
        html = _get_html(client, url)
        if html is None:
            if n > 1:
                break
            raise RuntimeError(
                "Discovery failed on page 1 (blocked, captcha, or error). "
                "Try again or populate data/afcc_slugs.csv manually on Railway."
            )
        if _page_empty_search(html) and n > 1:
            break
        batch = _extract_slugs_from_html(html)
        if not batch and n > 1:
            break
        before = len(all_slugs)
        all_slugs |= batch
        if not batch and n == 1:
            LOG.error("No celebrant slugs on first search page. Check WAF or HTML layout.")
        if n > 1 and not batch and before == len(all_slugs):
            # Empty repeated page: stop
            if _page_empty_search(html):
                break
        n += 1
        if n > 200:
            LOG.warning("Stopping discover at paged=200 (safety cap)")
            break
    return sorted(all_slugs)


def _write_slugs(path: Path, slugs: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["afcc_slug"])
        for s in slugs:
            w.writerow([s])


def _read_slugs(path: Path) -> list[str]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        out: list[str] = []
        for row in r:
            slug = (row.get("afcc_slug") or row.get("slug") or "").strip()
            if slug:
                out.append(slug)
    return out


def _read_checkpoint(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        s: set[str] = set()
        for row in r:
            a = (row.get("afcc_slug") or "").strip()
            if a:
                s.add(a)
    return s


def _write_checkpoint(path: Path, slugs: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(set(slugs))
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["afcc_slug"], lineterminator="\n")
        w.writeheader()
        for x in rows:
            w.writerow({"afcc_slug": x})


def _ld_graph_names(html: str) -> str | None:
    for sc in _iter_ld_json_blocks(html):
        g = sc.get("@graph")
        if isinstance(g, list):
            for it in g:
                if not isinstance(it, dict):
                    continue
                t = (it.get("@type") or "").lower()
                if "webpage" in t and it.get("name"):
                    name = str(it.get("name", ""))
                    if " - afcc" in name.lower() or "|" in name:
                        return re.sub(
                            r"\s*[\|–-]\s*AFCC.*$", "", name, flags=re.IGNORECASE
                        ).strip()
    return None


def _iter_ld_json_blocks(html: str) -> Iterator[dict[str, Any]]:
    s = BeautifulSoup(html, "lxml")
    for sc in s.find_all("script", type="application/ld+json"):
        raw = (sc.string or sc.get_text() or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            yield data
        elif isinstance(data, list):
            for d in data:
                if isinstance(d, dict):
                    yield d


def _meta_title(soup: BeautifulSoup) -> str | None:
    ogt = soup.find("meta", property="og:title")
    if ogt and ogt.get("content"):
        return str(ogt["content"]).strip()
    if soup.title and soup.title.get_text():
        return soup.title.get_text(strip=True)
    return None


def _clean_display_name(name: str) -> str:
    n = name.strip()
    n = re.sub(r"\s*[\|–-]\s*AFCC.*$", "", n, flags=re.IGNORECASE).strip()
    n = re.sub(r"-\s*AFCC.*$", "", n, flags=re.IGNORECASE).strip()
    return n


def _section_after_h2(
    main: Tag, h2_re: re.Pattern[str], same_tab_only: bool = True
) -> str:
    h2: Tag | None = None
    for hx in main.find_all("h2"):
        t = hx.get_text(" ", strip=True)
        if h2_re.search(t or ""):
            h2 = hx
            break
    if h2 is None:
        return ""
    parts: list[str] = []
    for sib in h2.find_next_siblings():
        if sib.name in ("h2", "h3", "h1"):
            break
        if sib.name == "p" or sib.name == "div" or sib.name == "ul" or sib.name == "section":
            parts.append(sib.get_text(" ", strip=True))
    return " ".join(p for p in parts if p)


def _collect_paragraphs_until_h2(main: Tag) -> str:
    ps: list[str] = []
    for p in main.find_all("p"):
        t = p.get_text(" ", strip=True)
        if len(t) > 25 and not t.lower().startswith("read more"):
            ps.append(t)
    if not ps:
        return ""
    return " ".join(ps[:6])


def _h2s_block(soup: BeautifulSoup, h2_titles: tuple[str, ...]) -> str:
    main = soup.select_one("main") or soup.find("body")
    if not main:
        return ""
    for title in h2_titles:
        p = re.compile(r"^" + re.escape(title) + r"$", re.I)
        text = _section_after_h2(main, p)
        if text:
            return text
    if "Profile" in h2_titles:
        t = _collect_paragraphs_until_h2(main)
        if t:
            return t
    return ""


def _pipe_services(soup: BeautifulSoup) -> str:
    main = soup.select_one("main") or soup.find("body")
    if not main:
        return ""
    services_h2: Tag | None = None
    for hx in main.find_all("h2"):
        if re.match(r"^Services\s*$", hx.get_text(" ", strip=True) or "", re.I):
            services_h2 = hx
            break
    items: list[str] = []
    if services_h2 is not None:
        for sib in services_h2.find_next_siblings():
            if sib.name in ("h2", "h3"):
                break
            if sib.name in ("ul", "ol"):
                for li in sib.find_all("li", recursive=False):
                    t = li.get_text(" ", strip=True)
                    if t:
                        items.append(t)
            if sib.name in ("p",) and not items:
                t = sib.get_text(" ", strip=True)
                if t:
                    items.append(t)
    if not items:
        # list items in main after first h2
        for ul in main.find_all("ul", limit=8):
            t = ul.get_text(" ", strip=True)
            if 8 < len(t) < 800:
                items = [x.get_text(" ", strip=True) for x in ul.find_all("li", recursive=False) if x.get_text(strip=True)]
                if len(items) >= 2:
                    break
    return " | ".join(items) if items else ""


def _ceremony_area(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    main = soup.select_one("main") or soup.find("body")
    if not main:
        return None, None
    for key in (r"Area\s*serviced", r"Service\s*area", r"Location", r"Regions"):
        for hx in main.find_all("h2"):
            htxt = hx.get_text(" ", strip=True) or ""
            if not re.search(key, htxt, re.I):
                continue
            parts: list[str] = []
            for sib in hx.find_next_siblings():
                if sib.name == "h2" or (sib.name in ("h3",) and len(parts) > 0):
                    break
                t = sib.get_text(" ", strip=True) if sib is not None else ""
                if t and len(t) < 2000:
                    parts.append(t)
            block = " ".join(parts).strip() or htxt
            st_m = AUS_STATE.search(block)
            state = st_m.group(1) if st_m else None
            for piece in re.split(r"[\n,;|]+", block)[:4]:
                pc = piece.strip()
                if not pc or len(pc) > 180:
                    continue
                if re.match(
                    r"^(all|nationwide|remote|australia|anywhere|various|online|world|zoom)", pc, re.I
                ):
                    return None, state
                if not state and AUS_STATE.search(pc):
                    state = AUS_STATE.search(pc).group(1)  # type: ignore[union-attr]
                if re.match(
                    r"^(NSW|VIC|QLD|SA|WA|TAS|ACT|NT|New|South|North|West|Queens|Vict|South Australia)$",
                    pc,
                    re.I,
                ) and " " not in pc[:3]:
                    continue
                if len(pc) >= 2:
                    return pc, state
            if block and len(block) < 200:
                return block, state
    return None, None


def _emails_tels_soup(
    *soups: BeautifulSoup,
) -> tuple[str | None, str | None, list[str]]:
    emails: list[str] = []
    tels: list[str] = []
    for soup in soups:
        for a in soup.find_all("a", href=True):
            h = a.get("href", "")
            if h.lower().startswith("mailto:"):
                e = h.split(":", 1)[-1]
                e = re.split(r"[&?#]", e, maxsplit=1)[0]
                e = re.sub(r"%20", " ", e).strip()
                if e and "afcc" not in e.lower() and "helpdesk" not in e.lower():
                    emails.append(e)
            elif h.lower().startswith("tel:"):
                tels.append(h[4:].strip())
    email = next((e for e in emails if e), None)
    for raw in tels:
        digits = re.sub(r"\D", "", raw)
        if not digits or digits.startswith("13") or digits.startswith("18"):
            continue
        m = AUS_MOBILE.search(raw) or (AUS_MOBILE.search(digits) if len(digits) == 10 else None)
        if m and not m.group(0).lstrip("0").startswith("1"):
            return email, m.group(0), tels
    for soup in soups:
        m = AUS_MOBILE.search(soup.get_text(" ", strip=True))
        if m:
            return email, m.group(0), tels
    for raw in tels:
        digits = re.sub(r"\D", "", raw)
        if not digits.startswith("13") and len(digits) >= 8:
            return email, raw, tels
    return email, None, tels


def _is_blocklisted_link(href: str, text: str) -> bool:
    h = (href or "").lower()
    for bad in (
        "afcc.com.au",
        "facebook.com",
        "instagram.com",
        "twitter.com",
        "linkedin.com",
        "youtu.be",
        "youtube.com",
        "google.com",
        "maps.google",
    ):
        if bad in h:
            return True
    if h.startswith(("/", "#", "javascript:", "mailto:", "tel:")):
        return True
    t = (text or "").lower()
    if "wolff" in t and "design" in t:  # common “site by”
        return True
    if "wolffdesign" in h:
        return True
    return False


def _find_website(*soups: BeautifulSoup) -> str | None:
    candidates: list[tuple[str, str]] = []
    for soup in soups:
        for a in soup.find_all("a", href=True):
            h = a["href"].strip()
            if not h.startswith("http") or _is_blocklisted_link(h, a.get_text(" ", strip=True) or ""):
                continue
            if urlparse(h).netloc and "afcc" not in h:
                text = a.get_text(" ", strip=True) or ""
                lab = f"{h} {text}".lower()
                if any(k in lab for k in ("website", "visit", "wedding", "home page", "http")) or len(text) < 60:
                    candidates.append((h, text))
    for h, t in candidates:
        tt = t.lower()
        if "website" in tt or "wedding" in t:
            return h
    for h, t in candidates:
        if t and 2 < len(t) < 80 and "wolff" not in t.lower() and "design" not in t.lower():
            return h
    return None


def _testimonials_reviews(soup: BeautifulSoup) -> tuple[str | None, str | None, str | None]:
    t1: str | None = None
    t2: str | None = None
    t3: str | None = None
    main = soup.select_one("main") or soup.find("body")
    if not main:
        return None, None, None
    bqs: list[str] = []
    for bq in main.find_all("blockquote"):
        x = bq.get_text(" ", strip=True)
        if 15 < len(x) < 4000 and "no reviews" not in x.lower():
            bqs.append(x)
    bqs = bqs[:3]
    if bqs:
        t1, t2, t3 = bqs[0], (bqs[1] if len(bqs) > 1 else None), (bqs[2] if len(bqs) > 2 else None)
    if t1 and "no reviews" in t1.lower():
        return None, None, None
    if t1 is None:
        for el in main.find_all(
            class_=re.compile(r"(reviews-box|testimonial|review)", re.I), limit=8
        ):
            tx = el.get_text(" ", strip=True)
            if 30 < len(tx) < 5000 and "no reviews" not in tx.lower():
                t1, t2, t3 = tx, None, None
                return t1, t2, t3
    return t1, t2, t3


def _trim(s: str | None, m: int) -> str | None:
    if not s:
        return None
    t = s.strip()
    if len(t) > m:
        t = t[: m - 3] + "..."
    return t or None


def _row_from_fetched_html(
    slug: str,
    h_main: str | None,
    h_ser: str | None,
    h_tst: str | None,
) -> ProfileRow | None:
    if not h_main and not h_ser:
        return None
    base = f"{AFCC_ORIGIN}/celebrant/{slug}/"
    s_main = BeautifulSoup(h_main or h_ser or "", "lxml")
    s_ser = BeautifulSoup(h_ser or h_main or "", "lxml")
    s_tst = BeautifulSoup(h_tst or "", "lxml")
    s_full = BeautifulSoup((h_main or h_ser or ""), "lxml")

    name = _ld_graph_names(h_main or h_ser or "")
    if not name:
        name = _meta_title(s_main) or _meta_title(s_ser) or _meta_title(s_tst) or None
    if name:
        name = _clean_display_name(name) or name

    summary = _h2s_block(s_full, ("Profile", "About me", "Welcome")) or _h2s_block(
        s_ser, ("Profile", "About me", "Welcome")
    )
    if not (summary and summary.strip()) and h_main and s_main:
        mroot = s_main.select_one("main")
        if isinstance(mroot, Tag):
            summary = _collect_paragraphs_until_h2(mroot) or None
    if not (summary and len(summary) >= 12):
        myv = _h2s_block(s_full, ("My Values",)) or _h2s_block(s_ser, ("My Values",))
        summary = myv or summary

    sub, st = _ceremony_area(s_ser) if h_ser else (None, None)
    if h_main:
        s2, st2 = _ceremony_area(BeautifulSoup(h_main, "lxml"))
        if s2 and not sub:
            sub = s2
        if st2 and not st:
            st = st2

    services = _pipe_services(s_ser) if h_ser else _pipe_services(s_main)

    email, mobile, _tels = _emails_tels_soup(s_main, s_ser, s_tst)
    web = _find_website(s_main, s_ser, s_tst)
    t1, t2, t3 = _testimonials_reviews(s_tst)
    dnow = datetime.now(SYD).date()
    sum_t = (summary or "").strip()
    if not sum_t:
        sum_t = None
    sub2 = (sub or "").strip()
    st2 = (st or "").strip()
    if st2 and sub2 and re.search(rf"\b{re.escape(st2)}\b", sub2, re.I):
        sub2 = re.sub(rf"\b{re.escape(st2)}\b", "", sub2, flags=re.I).strip(" ,-")
    return ProfileRow(
        afcc_slug=slug,
        full_name=_trim(name, 300),
        mobile=mobile,
        email=email,
        suburb=_trim(sub2 or None, 200),
        state=_trim(st2 or None, 32),
        afcc_profile_url=base,
        services=_trim(services, 4000),
        summary=_trim(sum_t, SUMMARY_MAX) if sum_t else None,
        testimonial_1=_trim(t1, 2000),
        testimonial_2=_trim(t2, 2000),
        testimonial_3=_trim(t3, 2000),
        website=web,
        scraped_date=dnow.isoformat(),
        matched_celebrant_id=None,
    )


def _parse_one_profile(
    client: httpx.Client,
    slug: str,
) -> ProfileRow | None:
    base = f"{AFCC_ORIGIN}/celebrant/{slug}/"
    h_main = _get_html(client, base)
    _sleep()
    h_ser = _get_html(client, f"{base}?tab=services")
    _sleep()
    h_tst = _get_html(client, f"{base}?tab=leave-a-review")
    _sleep()  # before the next celebrant
    return _row_from_fetched_html(slug, h_main, h_ser, h_tst)


# Minimal HTML (structure mirrors AFCC) to validate the parser when the live site returns 202/WAF.
_DEMO_MAIN = """<!DOCTYPE html><html><head>
<meta property="og:title" content="Sonya Nurthen - AFCC" />
</head><body><main>
<h2>Profile</h2>
<p>I'm passionate about making a positive contribution. This paragraph is the bio summary for tests.</p>
<a href="mailto:sonya.demo@example.com">email</a>
<a href="tel:0411999888">Call</a>
<a href="https://demo-sonya-wedding.example.com/">My website</a>
</main></body></html>"""

_DEMO_SER = """<!DOCTYPE html><html><body><main>
<h2>Services</h2>
<ul><li>Marriage ceremony</li><li>MC</li><li>Vow renewal</li></ul>
<h2>Area serviced</h2>
<div><p>Chatswood</p><p>NSW</p></div>
</main></body></html>"""

_DEMO_TST = """<!DOCTYPE html><html><body><main>
<blockquote>Perfect celebrant, highly recommend for your wedding day in Sydney.</blockquote>
<blockquote>So warm and professional, our guests still mention the ceremony years later.</blockquote>
<blockquote>Thank you for making our day stress-free and memorable from start to finish.</blockquote>
</main></body></html>"""


def _print_profile_debug(pr: ProfileRow) -> None:
    print("---", pr.afcc_slug, "---", flush=True)
    print("  full_name:", pr.full_name, flush=True)
    print("  mobile:", pr.mobile, "| email:", pr.email, "| website:", pr.website, flush=True)
    print("  location:", (pr.suburb, pr.state), flush=True)
    print("  services (trunc):", (pr.services or "")[:200], flush=True)
    print("  summary (trunc):", (pr.summary or "")[:220], flush=True)
    for j, t in enumerate((pr.testimonial_1, pr.testimonial_2, pr.testimonial_3), start=1):
        if t:
            print(f"  testimonial_{j} (trunc):", t[:160] + " ...", flush=True)


def _demo_parsed_row() -> ProfileRow | None:
    r = _row_from_fetched_html("sonya-nurthen", _DEMO_MAIN, _DEMO_SER, _DEMO_TST)
    return r


def _row_to_dict(row: ProfileRow) -> dict[str, Any]:
    return asdict(row)


def _supabase_upsert(rows: list[ProfileRow], url: str, key: str) -> None:
    if not rows:
        return
    from supabase import create_client  # type: ignore[import-untyped]

    sb = create_client(url, key)
    payload = [_row_to_dict(r) for r in rows]
    sb.table("afcc_profiles").upsert(payload, on_conflict="afcc_slug").execute()
    LOG.info("Upserted %s profile row(s) to public.afcc_profiles", len(payload))


def main(test_mode: bool = False) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    s = _load_settings()
    sb_url = (s.supabase_url or "").strip()
    sb_key = (s.supabase_service_role_key or s.supabase_key or "").strip()
    if not test_mode and (not sb_url or not sb_key):
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")
    with httpx.Client() as client:
        slugs = _read_slugs(SLUGS_CSV) if SLUGS_CSV.is_file() else []
        if not slugs and not test_mode:
            LOG.info("Discovering celebrant slugs from find-a-marriage-celebrant (paginated)…")
            try:
                found = discover_all_slugs(client)
            except Exception as e:  # noqa: BLE001
                LOG.exception("Discovery: %s", e)
                found = []
            if found:
                _write_slugs(SLUGS_CSV, found)
                slugs = found
        if not slugs and test_mode:
            slugs = list(FALLBACK_TEST_SLUGS)[:TEST_LIMIT]
            LOG.warning(
                "Using %s built-in test slugs (%s empty or undiscovered).",
                TEST_LIMIT,
                SLUGS_CSV,
            )
        if not slugs:
            raise RuntimeError(
                f"No slugs in {SLUGS_CSV} and discovery returned none. Add slugs to the CSV or run on Railway."
            )

        done: set[str] = set() if test_mode else _read_checkpoint(CHECKPOINT_CSV)
        if test_mode:
            pending = list(slugs[:TEST_LIMIT])
        else:
            pending = [x for x in slugs if x not in done]
        if not test_mode and not pending:
            LOG.info("All slugs in checkpoint; nothing to do. Delete %s to re-run.", CHECKPOINT_CSV)
            return

        total = len(pending)
        since_ck = 0
        batch: list[ProfileRow] = []
        buf_done = set(done)

        for i, slug in enumerate(pending):
            LOG.info("Scraping %s (%s/%s)", slug, i + 1, total)
            pr = _parse_one_profile(client, slug)
            if pr is None:
                LOG.warning("Skipped or failed: %s", slug)
                continue
            batch.append(pr)
            buf_done.add(slug)
            if test_mode:
                _print_profile_debug(pr)
            since_ck += 1
            if not test_mode and since_ck >= CHECKPOINT_EVERY and batch:
                if sb_url and sb_key:
                    _supabase_upsert(batch, sb_url, sb_key)
                batch = []
                _write_checkpoint(CHECKPOINT_CSV, buf_done)
                since_ck = 0

        if batch and not test_mode and sb_url and sb_key:
            _supabase_upsert(batch, sb_url, sb_key)
        if batch and test_mode and sb_url and sb_key:
            _supabase_upsert(batch, sb_url, sb_key)
            LOG.info("Test rows upserted to Supabase.")
        elif test_mode and batch and (not sb_url or not sb_key):
            LOG.warning("SUPABASE not configured — test run only printed; no database upsert.")
        if test_mode and not batch:
            print(
                "\n[All live requests failed or were blocked. Parser output from local fixture "
                "HTML (for CI / smoke test):]\n",
                flush=True,
            )
            demo = _demo_parsed_row()
            if demo:
                _print_profile_debug(demo)
            else:
                LOG.error("Internal fixture did not parse.")

        if (not test_mode) and buf_done and sb_url and sb_key:
            _write_checkpoint(CHECKPOINT_CSV, buf_done)


if __name__ == "__main__":
    if "--test" in sys.argv:
        main(test_mode=True)
    elif "--run-now" in sys.argv:
        main(test_mode=False)
    else:
        print("Usage: python -m scrapers.afcc_profile_scraper --test | --run-now", file=sys.stderr)
        sys.exit(2)
