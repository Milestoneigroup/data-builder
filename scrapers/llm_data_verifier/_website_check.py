"""Layer 1–2: website reachability and on-page corroboration."""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from scrapers.llm_data_verifier._content_match import name_appears_on_page, text_appears_on_page
from scrapers.llm_data_verifier._framework import normalise_url, polite_get


def check_website(
    *,
    vendor_id: str,
    business_name: str,
    website_url: str,
    email_to_check: str,
    phone_to_check: str,
    logger: logging.Logger,
    skip_hosts: set[str] | None = None,
) -> dict:
    """Fetch vendor website and score name / email / phone presence."""
    _ = vendor_id, logger
    empty = {
        "website_alive": False,
        "website_status_code": 0,
        "website_final_url": "",
        "website_response_ms": 0,
        "website_page_title": "",
        "name_match_on_page": False,
        "email_appears_on_page": None,
        "phone_appears_on_page": None,
        "website_check_notes": "",
        "website_html": "",
    }
    url = normalise_url((website_url or "").strip())
    if not url:
        empty["website_check_notes"] = "no_website_url"
        return empty

    orig_host = urlparse(url).hostname or ""
    orig_host = orig_host.lower()
    if orig_host.startswith("www."):
        orig_host = orig_host[4:]
    res = polite_get(url, logger=logger, skip_hosts=skip_hosts)
    code = int(res["status_code"])
    final = (res["final_url"] or "").strip()
    final_host = (urlparse(final).hostname or "") if final else ""
    final_host = final_host.lower()
    if final_host.startswith("www."):
        final_host = final_host[4:]
    html = res["body_text"] or ""
    notes: list[str] = []
    if res.get("error"):
        notes.append(res["error"])

    alive = 200 <= code < 400
    if code == 0:
        alive = False

    if orig_host and final_host and orig_host != final_host:
        notes.append("redirected_to_different_domain")

    title = ""
    try:
        soup = BeautifulSoup(html, "lxml")
        t = soup.title
        if t and t.string:
            title = t.string.strip()[:500]
    except Exception:
        pass

    email_raw = (email_to_check or "").strip()
    phone_raw = (phone_to_check or "").strip()
    email_flag: bool | None = None
    phone_flag: bool | None = None
    if email_raw:
        email_flag = text_appears_on_page(email_raw, html) if alive else False
    if phone_raw:
        phone_flag = text_appears_on_page(phone_raw, html) if alive else False

    name_match = name_appears_on_page(business_name, html) if alive else False

    return {
        "website_alive": alive,
        "website_status_code": code,
        "website_final_url": final or url,
        "website_response_ms": int(res["response_ms"]),
        "website_page_title": title,
        "name_match_on_page": name_match,
        "email_appears_on_page": email_flag,
        "phone_appears_on_page": phone_flag,
        "website_check_notes": "; ".join(notes) if notes else "",
        "website_html": html if alive else html,
    }
