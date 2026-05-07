"""Polite HTTP helpers, URL normalisation, and small parsing utilities."""

from __future__ import annotations

import random
import re
import time
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

USER_AGENT = (
    "Mozilla/5.0 (compatible; MilestoneVerifier/1.0; +https://milestoneigroup.com)"
)
REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-AU,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
REQUEST_TIMEOUT = 20
POLITE_DELAY_RANGE = (1.5, 3.0)
MAX_REDIRECTS = 5


def polite_gap() -> None:
    """Random delay between high-level verifier steps (in addition to per-request delays)."""
    time.sleep(random.uniform(*POLITE_DELAY_RANGE))


def _strip_tracking_params(parsed) -> tuple:  # noqa: ANN001
    """Remove common tracking query keys from a parsed URL."""
    q = parse_qs(parsed.query, keep_blank_values=False)
    drop_prefixes = ("utm_",)
    drop_exact = {
        "fbclid",
        "gclid",
        "msclkid",
        "twclid",
        "_ga",
        "mc_eid",
        "ref",
    }
    keep: dict[str, list[str]] = {}
    for k, vals in q.items():
        kl = k.lower()
        if kl in drop_exact or any(kl.startswith(p) for p in drop_prefixes):
            continue
        keep[k] = vals
    new_query = urlencode(keep, doseq=True)
    return parsed._replace(query=new_query)


def normalise_url(url: str) -> str:
    """Ensure https:// prefix, strip whitespace, lowercase host, strip tracking params."""
    s = (url or "").strip()
    if not s:
        return ""
    if not re.match(r"^[a-z][a-z0-9+.-]*://", s, re.I):
        s = "https://" + s
    try:
        p = urlparse(s)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        if not netloc and p.path:
            # urlparse sometimes treats "example.com/path" as path-only
            p = urlparse("https://" + s.lstrip("/"))
            scheme = "https"
            netloc = (p.netloc or "").lower()
        path = p.path or ""
        if path != "/" and not path.endswith("/") and not path.split("/")[-1].count("."):
            pass
        p2 = p._replace(scheme=scheme, netloc=netloc)
        p2 = _strip_tracking_params(p2)
        out = urlunparse(p2)
        return out.rstrip() if out else ""
    except Exception:
        return s


def extract_handle_from_instagram_url(url: str) -> str:
    """Return Instagram handle slug from a URL, or "" if not parseable."""
    s = (url or "").strip()
    if not s:
        return ""
    if not re.match(r"^[a-z][a-z0-9+.-]*://", s, re.I):
        s = "https://" + s
    p = urlparse(s)
    host_full = (p.netloc or "").lower()
    if host_full.startswith("www."):
        host_full = host_full[4:]
    if host_full not in (
        "instagram.com",
        "instagr.am",
        "m.instagram.com",
    ) and not host_full.endswith(".instagram.com"):
        return ""
    path = (p.path or "").strip("/")
    segments = [x for x in path.split("/") if x]
    if not segments:
        return ""
    handle = segments[0].split("?")[0].strip()
    if handle in ("p", "reel", "reels", "stories", "explore", "accounts"):
        if len(segments) > 1:
            return segments[1].split("?")[0].strip()
        return ""
    return handle.lstrip("@") if handle else ""


def extract_facebook_page_slug(url: str) -> str:
    """Return the primary Facebook page slug from a URL for matching."""
    s = (url or "").strip()
    if not s:
        return ""
    if not re.match(r"^[a-z][a-z0-9+.-]*://", s, re.I):
        s = "https://" + s
    p = urlparse(s)
    host = (p.netloc or "").lower().replace("www.", "")
    if not any(
        x in host for x in ("facebook.com", "fb.com", "fb.me", "m.facebook.com")
    ):
        return ""
    path = (p.path or "").strip("/")
    parts = [x for x in path.split("/") if x]
    skip = {
        "profile.php",
        "people",
        "pages",
        "groups",
        "watch",
        "share",
        "login.php",
        "home.php",
    }
    i = 0
    while i < len(parts) and parts[i].lower() in skip:
        i += 1
        if i > 0 and parts[i - 1].lower() == "profile.php":
            return ""
    if i >= len(parts):
        return ""
    slug = parts[i].split("?")[0].strip()
    return slug.lower() if slug else ""


def polite_get(
    url: str,
    *,
    logger: logging.Logger,
    allow_redirects: bool = True,
    skip_hosts: set[str] | None = None,
) -> dict:
    """GET with a random delay first; return response metadata and body text.

    Returns keys: status_code, final_url, body_text, response_ms, error.
    """
    delay = random.uniform(*POLITE_DELAY_RANGE)
    time.sleep(delay)
    out = {
        "status_code": 0,
        "final_url": "",
        "body_text": "",
        "response_ms": 0,
        "error": "",
    }
    raw_url = (url or "").strip()
    if not raw_url:
        out["error"] = "empty_url"
        return out
    try:
        nu = normalise_url(raw_url)
        host = urlparse(nu).hostname or ""
        host = host.lower()
        if host.startswith("www."):
            host = host[4:]
        if skip_hosts and host in skip_hosts:
            out["error"] = "skipped_host_after_403"
            return out
        t0 = time.perf_counter()
        try:
            r = requests.get(
                nu,
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=allow_redirects,
                max_redirects=MAX_REDIRECTS,
            )
        except TypeError:
            r = requests.get(
                nu,
                headers=REQUEST_HEADERS,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=allow_redirects,
            )
        out["response_ms"] = int((time.perf_counter() - t0) * 1000)
        out["status_code"] = int(r.status_code)
        out["final_url"] = str(r.url or nu)
        out["body_text"] = r.text or ""
        if r.status_code == 403 and skip_hosts is not None and host:
            skip_hosts.add(host)
            out["error"] = "http_403_host_skipped"
        elif r.status_code >= 400:
            out["error"] = f"http_{r.status_code}"
    except requests.TooManyRedirects:
        out["error"] = "too_many_redirects"
    except requests.RequestException as e:
        out["error"] = str(e)[:500]
    return out
