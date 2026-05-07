"""Business name tokenisation and loose text matching on HTML pages."""

from __future__ import annotations

import re
from html import unescape

from bs4 import BeautifulSoup

STOPWORDS = {
    "the",
    "and",
    "of",
    "for",
    "by",
    "with",
    "photography",
    "photo",
    "photos",
    "studio",
    "studios",
    "celebrant",
    "celebrants",
    "marriage",
    "weddings",
    "wedding",
    "co",
    "ltd",
    "pty",
    "pty ltd",
    "australia",
    "au",
    "official",
    "site",
    "website",
}


def _normalise_business_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = s.replace("&", " and ")
    s = s.replace("’", "'").replace("`", "'")
    return s


def significant_tokens(business_name: str) -> set[str]:
    """Tokenise, lowercase, drop stopwords, keep tokens with length >= 3."""
    s = _normalise_business_name(business_name)
    raw = re.split(r"[^a-z0-9]+", s)
    out: set[str] = set()
    for t in raw:
        if len(t) < 3:
            continue
        if t in STOPWORDS:
            continue
        if t == "pty" or t == "ltd":
            continue
        out.add(t)
    return out


def _page_visible_text(page_html: str) -> str:
    """Rough visible text from HTML for matching."""
    soup = BeautifulSoup(page_html or "", "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    text = unescape(text)
    return text.lower()


def name_appears_on_page(business_name: str, page_html: str) -> bool:
    """True if at least 60% of significant tokens appear in page text."""
    tokens = significant_tokens(business_name)
    if not tokens:
        return False
    visible = _page_visible_text(page_html)
    if len(tokens) == 1:
        (only,) = tuple(tokens)
        return only in visible
    found = sum(1 for t in tokens if t in visible)
    return found >= max(1, int(0.6 * len(tokens) + 0.9999))


def _alnum_only(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def text_appears_on_page(needle: str, page_html: str) -> bool:
    """Loose match on alphanumeric-only substring."""
    n = _alnum_only(needle)
    if not n:
        return False
    visible = _alnum_only(_page_visible_text(page_html))
    return n in visible
