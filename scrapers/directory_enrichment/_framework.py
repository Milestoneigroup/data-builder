"""Shared HTTP, logging, robots.txt, retry, fuzzy match, and Supabase helpers."""

from __future__ import annotations

import logging
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
from dotenv import load_dotenv
from thefuzz import fuzz

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Per brief: env.local wins (same pattern as other scrapers, explicit override).
load_dotenv(_ROOT / "env.local", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / ".env", override=True)

# Extended from ``celebrant_active_enrichment.RAILWAY_DIRECTORY_HEADERS`` with a branded UA.
RAILWAY_DIRECTORY_HEADERS: dict[str, str] = {
    "User-Agent": (
        "MilestoneDataBuilder/1.0 (+https://milestonei.com.au; directory-enrichment)"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
    "Referer": "https://www.google.com.au/",
}

FUZZY_MATCH_MIN = 85

_robots_cache: dict[str, RobotFileParser] = {}


def setup_logging() -> tuple[Path, logging.Logger]:
    """File + stdout logger under ``logs/directory_enrichment_{timestamp}.log``."""
    log_dir = _ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"directory_enrichment_{ts}.log"
    root = logging.getLogger("directory_enrichment")
    root.handlers.clear()
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass
    return log_path, root


def polite_delay() -> None:
    """Wait 2–3 s with jitter between directory requests."""
    time.sleep(random.uniform(2.0, 3.5))


def _origin(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def robots_allowed(client: httpx.Client, url: str, user_agent: str) -> bool:
    """Return True if robots.txt allows fetch, or if robots cannot be read (fail-open)."""
    origin = _origin(url)
    if origin not in _robots_cache:
        robots_url = f"{origin}/robots.txt"
        rp = RobotFileParser()
        try:
            r = client.get(robots_url, timeout=30.0)
            if r.status_code == 200:
                rp.parse(r.text.splitlines())
            else:
                rp.parse(["User-agent: *", "Allow: /"])
        except Exception:  # noqa: BLE001
            rp.parse(["User-agent: *", "Allow: /"])
        _robots_cache[origin] = rp
    try:
        return _robots_cache[origin].can_fetch(user_agent, url)
    except Exception:  # noqa: BLE001
        return True


def fetch_with_retry(
    client: httpx.Client,
    url: str,
    *,
    user_agent: str,
    max_attempts: int = 3,
) -> httpx.Response:
    """GET with exponential backoff on 429 / 503. Raises last error if all attempts fail."""
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        if not robots_allowed(client, url, user_agent):
            raise RuntimeError(f"robots.txt disallows fetch: {url}")
        try:
            r = client.get(url)
            if r.status_code in (429, 503):
                wait = 2 ** (attempt - 1) + random.uniform(0.0, 0.75)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < max_attempts:
                wait = 2 ** (attempt - 1) + random.uniform(0.0, 0.75)
                time.sleep(wait)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"fetch_with_retry exhausted attempts for {url}")


def token_sort_ratio(a: str, b: str) -> int:
    return int(fuzz.token_sort_ratio(a or "", b or ""))


def norm_name(s: str | None) -> str:
    if not s:
        return ""
    t = " ".join(str(s).split())
    return t.casefold()


# Easy Weddings often emits full state names; public.* tables use AU abbreviations.
_AU_STATE_BY_NORMALISED: dict[str, str] = {
    "new south wales": "NSW",
    "nsw": "NSW",
    "victoria": "VIC",
    "vic": "VIC",
    "queensland": "QLD",
    "qld": "QLD",
    "western australia": "WA",
    "wa": "WA",
    "south australia": "SA",
    "sa": "SA",
    "tasmania": "TAS",
    "tas": "TAS",
    "australian capital territory": "ACT",
    "act": "ACT",
    "northern territory": "NT",
    "nt": "NT",
}


def normalise_au_state(state: str | None) -> str | None:
    if state is None:
        return None
    raw = str(state).strip()
    if not raw:
        return None
    if len(raw) <= 3 and raw.isalpha():
        return raw.upper()
    key = " ".join(raw.split()).casefold()
    return _AU_STATE_BY_NORMALISED.get(key, raw.upper())


def get_supabase_client() -> Any:
    from supabase import create_client

    from data_builder.config import get_settings

    cfg = get_settings()
    url = (cfg.supabase_url or "").strip()
    key = (cfg.supabase_service_role_key or "").strip()
    if not url or not key:
        raise SystemExit(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required (see env.local)."
        )
    return create_client(url, key)


_BLOCKED_HOST_SUBSTRINGS = (
    "hellomay.com.au",
    "hello-may.myshopify.com",
    "myshopify.com",
    "statcounter.com",
    "twitter.com/",
    "x.com/",
    "hellomaymagazine",
    "tiktok.com/@hellomay",
)

_SOCIAL_HINT = re.compile(r"(instagram\.com|facebook\.com|fb\.me|tiktok\.com|pinterest\.)", re.I)


def is_blocked_non_vendor_url(href: str) -> bool:
    h = href.lower()
    if any(x in h for x in _BLOCKED_HOST_SUBSTRINGS):
        return True
    if "pinterest.com" in h and "hellomay" in h:
        return True
    return False


def looks_like_vendor_website(href: str, text: str) -> bool:
    if not href.startswith("http"):
        return False
    if is_blocked_non_vendor_url(href):
        return False
    if _SOCIAL_HINT.search(href):
        return False
    if re.search(r"website", text or "", re.I):
        return True
    low = href.lower()
    return "google." not in low and "/maps" not in low
