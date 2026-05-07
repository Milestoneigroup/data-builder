"""Layer 3–4: best-effort Instagram and Facebook reachability (no auth)."""

from __future__ import annotations

import logging
import re
from html import unescape

from bs4 import BeautifulSoup

from scrapers.llm_data_verifier._framework import normalise_url, polite_get

_META_OG_DESC = re.compile(
    r'<meta[^>]+property=["\']og:description["\'][^>]*content=["\']([^"\']+)["\']',
    re.I,
)
_META_NAME_DESC = re.compile(
    r'<meta[^>]+name=["\']description["\'][^>]*content=["\']([^"\']+)["\']',
    re.I,
)


def _extract_meta_description(html: str) -> str:
    raw = html or ""
    m = _META_OG_DESC.search(raw)
    if m:
        return unescape(m.group(1).strip())[:2000]
    m2 = _META_NAME_DESC.search(raw)
    if m2:
        return unescape(m2.group(1).strip())[:2000]
    try:
        soup = BeautifulSoup(raw, "lxml")
        tag = soup.find("meta", property="og:description")
        if tag and tag.get("content"):
            return str(tag["content"]).strip()[:2000]
        tag2 = soup.find("meta", attrs={"name": "description"})
        if tag2 and tag2.get("content"):
            return str(tag2["content"]).strip()[:2000]
    except Exception:
        pass
    return ""


def check_instagram(handle_url: str, logger: logging.Logger, skip_hosts: set[str] | None = None) -> dict:
    """Best-effort Instagram profile probe (false negatives acceptable)."""
    _ = logger
    out = {
        "instagram_url_status": 0,
        "instagram_appears_real": False,
        "instagram_bio_text": "",
        "instagram_check_notes": "",
    }
    url = normalise_url((handle_url or "").strip())
    if not url:
        out["instagram_check_notes"] = "no_instagram_url"
        return out

    res = polite_get(url, logger=logger, skip_hosts=skip_hosts)
    code = int(res["status_code"])
    body = res["body_text"] or ""
    out["instagram_url_status"] = code
    notes: list[str] = []
    if res.get("error"):
        notes.append(res["error"])

    if code == 0:
        out["instagram_appears_real"] = False
        out["instagram_check_notes"] = "; ".join(notes) if notes else "connection_error"
        return out

    low = body.lower()
    if code == 403:
        out["instagram_appears_real"] = False
        notes.append("http_403")
        out["instagram_check_notes"] = "; ".join(notes)
        return out

    if code == 429 or "rate limit" in low:
        out["instagram_appears_real"] = False
        notes.append("rate_limited")
        out["instagram_check_notes"] = "; ".join(notes)
        return out

    if "sorry, this page isn't available" in low or "page not found" in low:
        out["instagram_appears_real"] = False
        notes.append("404_or_unavailable_page")
        out["instagram_check_notes"] = "; ".join(notes)
        return out

    if 200 <= code < 400:
        dead_signals = (
            "sorry, this page isn't available",
            "the link you followed may be broken",
        )
        if any(s in low for s in dead_signals):
            out["instagram_appears_real"] = False
            notes.append("unavailable_message_in_body")
        else:
            out["instagram_appears_real"] = True
        out["instagram_bio_text"] = _extract_meta_description(body)
    else:
        out["instagram_appears_real"] = False
        notes.append(f"http_{code}")

    out["instagram_check_notes"] = "; ".join(notes)
    return out


def check_facebook(page_url: str, logger: logging.Logger, skip_hosts: set[str] | None = None) -> dict:
    """Best-effort Facebook page probe (login walls vs missing pages)."""
    _ = logger
    out = {
        "facebook_url_status": 0,
        "facebook_appears_real": False,
        "facebook_bio_text": "",
        "facebook_check_notes": "",
    }
    url = normalise_url((page_url or "").strip())
    if not url:
        out["facebook_check_notes"] = "no_facebook_url"
        return out

    res = polite_get(url, logger=logger, skip_hosts=skip_hosts)
    code = int(res["status_code"])
    body = res["body_text"] or ""
    out["facebook_url_status"] = code
    notes: list[str] = []
    if res.get("error"):
        notes.append(res["error"])

    if code == 0:
        out["facebook_check_notes"] = "; ".join(notes) if notes else "connection_error"
        return out

    low = body.lower()
    final = (res["final_url"] or "").lower()

    if code == 403:
        out["facebook_appears_real"] = False
        notes.append("http_403")
        out["facebook_check_notes"] = "; ".join(notes)
        return out

    if code == 404:
        out["facebook_appears_real"] = False
        notes.append("404_not_found")
        out["facebook_check_notes"] = "; ".join(notes)
        return out

    if "login.php" in final or "/login/" in final or "facebook.com/login" in final:
        out["facebook_appears_real"] = False
        notes.append("redirected_to_login_wall")
        out["facebook_bio_text"] = _extract_meta_description(body)
        out["facebook_check_notes"] = "; ".join(notes)
        return out

    if "sorry, this content isn't available" in low or "page is not available" in low:
        out["facebook_appears_real"] = False
        notes.append("content_not_available_message")
        out["facebook_check_notes"] = "; ".join(notes)
        return out

    if 200 <= code < 400:
        if "you must log in to continue" in low or "log in to facebook" in low[:6000]:
            out["facebook_appears_real"] = False
            notes.append("login_wall_in_body")
        else:
            out["facebook_appears_real"] = True
        out["facebook_bio_text"] = _extract_meta_description(body)
    else:
        out["facebook_appears_real"] = False
        notes.append(f"http_{code}")

    out["facebook_check_notes"] = "; ".join(notes)
    return out
