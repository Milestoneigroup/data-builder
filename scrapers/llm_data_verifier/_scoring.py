"""Verification score (percentage of applicable checks) and tier labels."""

from __future__ import annotations

from typing import Any


def compute_verification_score_and_tier(results: dict[str, Any]) -> tuple[float, str]:
    """Return (score_pct 0–100, tier enum string)."""
    website_found = bool(results.get("_website_found_claimed"))
    instagram_found = bool(results.get("_instagram_found_claimed"))
    facebook_found = bool(results.get("_facebook_found_claimed"))
    email_claimed = bool(results.get("_email_claimed"))
    phone_claimed = bool(results.get("_phone_claimed"))

    website_alive = bool(results.get("website_alive"))
    name_match = bool(results.get("name_match_on_page"))
    email_ok = results.get("email_appears_on_page")
    phone_ok = results.get("phone_appears_on_page")
    links_ig = bool(results.get("website_links_to_instagram"))
    ig_real = bool(results.get("instagram_appears_real"))
    links_fb = bool(results.get("website_links_to_facebook"))
    fb_real = bool(results.get("facebook_appears_real"))

    maximum = 0
    earned = 0

    if website_found:
        maximum += 1
        if website_alive:
            earned += 1

    if website_found and website_alive:
        maximum += 1
        if name_match:
            earned += 1

    if website_found and website_alive and email_claimed:
        maximum += 1
        if email_ok is True:
            earned += 1

    if website_found and website_alive and phone_claimed:
        maximum += 1
        if phone_ok is True:
            earned += 1

    if website_found and instagram_found and website_alive:
        maximum += 1
        if links_ig:
            earned += 1

    if instagram_found:
        maximum += 1
        if ig_real:
            earned += 1

    if website_found and facebook_found and website_alive:
        maximum += 1
        if links_fb:
            earned += 1

    if facebook_found:
        maximum += 1
        if fb_real:
            earned += 1

    if maximum <= 0:
        return 0.0, "NO_DATA"

    pct = round(100.0 * earned / maximum, 2)

    if website_found and not website_alive:
        return pct, "REJECTED"

    social_claimed = instagram_found or facebook_found
    link_evidence = False
    if instagram_found and links_ig:
        link_evidence = True
    if facebook_found and links_fb:
        link_evidence = True
    social_clause = (not social_claimed) or link_evidence

    if (
        pct >= 75.0
        and website_found
        and website_alive
        and name_match
        and social_clause
    ):
        return pct, "HIGH"

    if pct >= 50.0 and website_found and website_alive:
        return pct, "MEDIUM"

    if website_found and website_alive:
        return pct, "LOW"

    # Social-only rows: no website URL to verify; tier from corroboration strength.
    if not website_found and social_claimed:
        if pct >= 50.0:
            return pct, "MEDIUM"
        return pct, "LOW"

    return pct, "NO_DATA"
