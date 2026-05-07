"""Orchestrate live verification for photographer and celebrant master CSVs."""

from __future__ import annotations

import csv
import logging
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scrapers.llm_data_verifier._cross_check import website_mentions_handle
from scrapers.llm_data_verifier._framework import polite_gap
from scrapers.llm_data_verifier._scoring import compute_verification_score_and_tier
from scrapers.llm_data_verifier._social_check import check_facebook, check_instagram
from scrapers.llm_data_verifier._supabase_read import lookup_business_name
from scrapers.llm_data_verifier._website_check import check_website

TRADING_NAME_RE = re.compile(r"Trading name:\s*([^|]+)", re.I)

VERIFICATION_FIELDS: tuple[str, ...] = (
    "website_alive",
    "website_status_code",
    "website_final_url",
    "website_response_ms",
    "website_page_title",
    "name_match_on_page",
    "email_appears_on_page",
    "phone_appears_on_page",
    "website_check_notes",
    "instagram_url_status",
    "instagram_appears_real",
    "instagram_bio_text",
    "instagram_check_notes",
    "facebook_url_status",
    "facebook_appears_real",
    "facebook_bio_text",
    "facebook_check_notes",
    "website_links_to_instagram",
    "website_links_to_facebook",
    "verification_score",
    "verification_tier",
)

_EMAIL_KEYS = (
    "email_found",
    "email",
    "primary_email",
    "contact_email",
    "business_email",
)
_PHONE_KEYS = (
    "phone_found",
    "phone",
    "primary_phone",
    "contact_phone",
    "mobile",
    "mobile_phone",
)

_MERGE_ORDER = {"HIGH": 0, "MEDIUM": 1, "YELLOW": 2, "RED": 3}


def _strip(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _vendor_id(row: dict[str, str]) -> str:
    for k in ("vendor_id", "celebrant_id", "photographer_id", "id"):
        v = _strip(row.get(k, ""))
        if v:
            return v
    return ""


def _first_nonempty(row: dict[str, str], keys: tuple[str, ...]) -> str:
    for k in keys:
        v = _strip(row.get(k, ""))
        if v:
            return v
    return ""


def resolve_business_name(
    row: dict[str, str],
    vertical: str,
    logger: logging.Logger,
) -> str:
    """Trading name from notes, known columns, then optional Supabase read."""
    notes = _strip(row.get("notes_combined", ""))
    m = TRADING_NAME_RE.search(notes)
    if m:
        name = m.group(1).strip()
        if name:
            return name
    for k in ("trading_name_known", "business_name_known", "business_name", "trading_name"):
        v = _strip(row.get(k, ""))
        if v:
            return v
    vid = _vendor_id(row)
    if vid:
        sb = lookup_business_name(vid, vertical, logger=logger)
        if sb:
            return sb
    return ""


def _merge_rank(row: dict[str, str]) -> int:
    t = _strip(row.get("merge_tier", "")).upper()
    return _MERGE_ORDER.get(t, 99)


def load_and_sort_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []
        rows = [dict(r) for r in reader]
    rows.sort(key=lambda r: (_merge_rank(r), _vendor_id(r)))
    return rows


def _default_verification() -> dict[str, Any]:
    return {
        "website_alive": False,
        "website_status_code": 0,
        "website_final_url": "",
        "website_response_ms": 0,
        "website_page_title": "",
        "name_match_on_page": False,
        "email_appears_on_page": None,
        "phone_appears_on_page": None,
        "website_check_notes": "",
        "instagram_url_status": 0,
        "instagram_appears_real": False,
        "instagram_bio_text": "",
        "instagram_check_notes": "",
        "facebook_url_status": 0,
        "facebook_appears_real": False,
        "facebook_bio_text": "",
        "facebook_check_notes": "",
        "website_links_to_instagram": False,
        "website_links_to_facebook": False,
    }


def _verification_to_cells(vf: dict[str, Any], score: float, tier: str) -> dict[str, str]:
    cells: dict[str, str] = {}
    for k in VERIFICATION_FIELDS:
        if k == "verification_score":
            cells[k] = str(score)
            continue
        if k == "verification_tier":
            cells[k] = tier
            continue
        v = vf.get(k)
        if k in ("email_appears_on_page", "phone_appears_on_page"):
            cells[k] = "" if v is None else str(bool(v))
        else:
            cells[k] = "" if v is None else str(v)
    return cells


def process_row(
    row: dict[str, str],
    vertical: str,
    logger: logging.Logger,
    skip_hosts: set[str],
) -> dict[str, str]:
    """Return the row merged with verification columns."""
    out = dict(row)
    web = _strip(row.get("website_found", ""))
    ig = _strip(row.get("instagram_found", ""))
    fb = _strip(row.get("facebook_found", ""))
    if not web and not ig and not fb:
        cells = _verification_to_cells(_default_verification(), 0.0, "NO_DATA")
        out.update(cells)
        return out

    wid = _vendor_id(row)
    business = resolve_business_name(row, vertical, logger)
    email_guess = _first_nonempty(row, _EMAIL_KEYS)
    phone_guess = _first_nonempty(row, _PHONE_KEYS)

    results: dict[str, Any] = {
        "_website_found_claimed": bool(web),
        "_instagram_found_claimed": bool(ig),
        "_facebook_found_claimed": bool(fb),
        "_email_claimed": bool(email_guess),
        "_phone_claimed": bool(phone_guess),
    }
    base = _default_verification()
    results.update(base)

    html = ""
    if web:
        wres = check_website(
            vendor_id=wid,
            business_name=business,
            website_url=web,
            email_to_check=email_guess,
            phone_to_check=phone_guess,
            logger=logger,
            skip_hosts=skip_hosts,
        )
        html = str(wres.pop("website_html", "") or "")
        results.update(wres)
        polite_gap()

    if ig:
        results.update(check_instagram(ig, logger, skip_hosts=skip_hosts))
        polite_gap()

    if fb:
        results.update(check_facebook(fb, logger, skip_hosts=skip_hosts))
        polite_gap()

    alive = bool(results.get("website_alive"))
    if alive and ig:
        results["website_links_to_instagram"] = website_mentions_handle(
            html, ig, "instagram"
        )
    if alive and fb:
        results["website_links_to_facebook"] = website_mentions_handle(html, fb, "facebook")

    scoring_payload = dict(results)
    score, tier = compute_verification_score_and_tier(scoring_payload)

    vf_out = {k: results.get(k) for k in base.keys()}
    out.update(_verification_to_cells(vf_out, score, tier))
    return out


def _output_fieldnames(original: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for c in original:
        if c not in seen:
            seen.add(c)
            out.append(c)
    for c in VERIFICATION_FIELDS:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def write_summary_md(
    path: Path,
    *,
    iso_date: str,
    photographers_meta: dict[str, Any],
    celebrants_meta: dict[str, Any],
    rejected: list[dict[str, str]],
) -> None:
    """Write Markdown summary for operators."""
    pt = photographers_meta.get("tiers") or {}
    ct = celebrants_meta.get("tiers") or {}
    combined = Counter(pt) + Counter(ct)

    lines = [
        f"# LLM data verification summary ({iso_date})",
        "",
        "## All vendors (combined)",
        f"- Total processed: {int(sum(combined.values()))}",
        f"- Tier counts: {_format_counts(dict(combined))}",
        "",
        "## Photographers",
        f"- Vendors processed: {photographers_meta.get('processed', 0)}",
        f"- Tier counts: {_format_counts(pt)}",
        "",
        "## Celebrants",
        f"- Vendors processed: {celebrants_meta.get('processed', 0)}",
        f"- Tier counts: {_format_counts(ct)}",
        "",
        "## Rejected vendors",
        "",
    ]
    if not rejected:
        lines.append("_None recorded._")
    else:
        for r in rejected:
            vid = r.get("vendor_id", "")
            reason = r.get("rejection_reason", "")
            tier = r.get("merge_tier", "")
            lines.append(f"- **{vid}** (merge_tier {tier}): {reason}")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _format_counts(raw: Any) -> str:
    if not isinstance(raw, dict):
        return "{}"
    parts = [f"{k}: {v}" for k, v in sorted(raw.items())]
    return "{" + ", ".join(parts) + "}"


def run_vertical(
    *,
    input_csv: Path,
    output_csv: Path,
    vertical: str,
    logger: logging.Logger,
    skip_hosts: set[str],
    max_vendors: int,
    start_after: str | None,
    flush_every: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]], Counter[str]]:
    """Process one master CSV; return enriched rows, rejected audit rows, tier counts."""
    rows_in = load_and_sort_rows(input_csv)
    if not rows_in:
        logger.warning("No rows loaded from %s", input_csv)
        return [], [], Counter()

    resume = (start_after or "").strip()
    pending_resume = bool(resume)
    active: list[dict[str, str]] = []
    for r in rows_in:
        vid = _vendor_id(r)
        if pending_resume:
            if vid == resume:
                pending_resume = False
            continue
        active.append(r)
        if max_vendors and len(active) >= max_vendors:
            break

    if resume and pending_resume:
        logger.warning("start-after id %r not found in %s — no rows processed", resume, input_csv)
        return [], [], Counter()

    original_fields = list(rows_in[0].keys())
    fieldnames = _output_fieldnames(original_fields)

    done: list[dict[str, str]] = []
    rejected: list[dict[str, str]] = []
    tiers: Counter[str] = Counter()

    for i, row in enumerate(active, start=1):
        try:
            enriched = process_row(row, vertical, logger, skip_hosts)
        except KeyboardInterrupt:
            logger.warning("Interrupted — writing partial CSV (%s rows)", len(done))
            _write_csv(output_csv, done, fieldnames)
            raise
        done.append(enriched)
        tier = _strip(enriched.get("verification_tier", ""))
        tiers[tier] += 1
        if tier == "REJECTED":
            rejected.append(
                {
                    "vendor_id": _vendor_id(row),
                    "vertical": vertical,
                    "merge_tier": _strip(row.get("merge_tier", "")),
                    "website_found": _strip(row.get("website_found", "")),
                    "rejection_reason": _strip(enriched.get("website_check_notes", ""))
                    or "website_not_alive",
                }
            )
        if i % 10 == 0:
            logger.info("Progress %s: %s / %s rows", vertical, i, len(active))
        if flush_every and len(done) % flush_every == 0:
            _write_csv(output_csv, done, fieldnames)
            logger.info("Flushed partial output (%s rows) to %s", len(done), output_csv)

    _write_csv(output_csv, done, fieldnames)
    return done, rejected, tiers


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def iso_date_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()
