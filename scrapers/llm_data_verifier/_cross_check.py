"""Layer 5: vendor website self-attestation of social profiles."""

from __future__ import annotations

import re

from scrapers.llm_data_verifier._framework import (
    extract_facebook_page_slug,
    extract_handle_from_instagram_url,
)


def website_mentions_handle(page_html: str, social_url: str, platform: str) -> bool:
    """True if the vendor site HTML links to or cites the same social handle/page."""
    html = page_html or ""
    if not html.strip():
        return False

    if platform == "instagram":
        handle = extract_handle_from_instagram_url(social_url).lower().strip()
        if not handle:
            return False
        h = html.lower()
        patterns = [
            rf"instagram\.com/{re.escape(handle)}/",
            rf"instagram\.com/{re.escape(handle)}\?",
            rf"instagram\.com/{re.escape(handle)}[\"'>\s]",
            rf"instagr\.am/{re.escape(handle)}/",
            rf"instagr\.am/{re.escape(handle)}\?",
            rf"instagr\.am/{re.escape(handle)}[\"'>\s]",
        ]
        if any(re.search(p, h) for p in patterns):
            return True
        at = f"@{handle}"
        if at in h and re.search(r"instagram", h):
            return True
        return False

    if platform == "facebook":
        slug = extract_facebook_page_slug(social_url)
        if not slug:
            return False
        h = html.lower()
        variants = {slug, slug.replace("-", ".")}
        for slug_variant in variants:
            if not slug_variant:
                continue
            ps = re.escape(slug_variant)
            for pat in (
                rf"facebook\.com/{ps}/",
                rf"facebook\.com/{ps}\?",
                rf"facebook\.com/{ps}\"",
                rf"facebook\.com/{ps}['>\s]",
                rf"fb\.me/{ps}/",
                rf"fb\.me/{ps}\?",
                rf"fb\.com/{ps}/",
                rf"fb\.com/{ps}\?",
            ):
                if re.search(pat, h, re.I):
                    return True
        return False

    raise ValueError(f"unknown platform: {platform}")
