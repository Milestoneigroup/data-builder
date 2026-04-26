"""Enrich shared.ref_park_authorities: replace VERIFY_REQUIRED with scraped + Claude values.

Reads rows from Supabase where enrichment fields are VERIFY_REQUIRED, fetches each
authority homepage with httpx, sends HTML to Claude for structured extraction,
then updates contact_email, contact_phone, website_weddings_permit_page,
hq_suburb, and typical_permit_fee_range_aud (missing values become NOT_FOUND).

Log file: logs/park_authorities_enrichment.log

Requires SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, ANTHROPIC_API_KEY.
Optional: ANTHROPIC_MODEL (default claude-sonnet-4-20250514), SCRAPER_USER_AGENT.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

LOG_PATH = _ROOT / "logs" / "park_authorities_enrichment.log"
VERIFY = "VERIFY_REQUIRED"
ENRICH_FIELDS = (
    "contact_email",
    "contact_phone",
    "website_weddings_permit_page",
    "hq_suburb",
    "typical_permit_fee_range_aud",
)
HTTP_DELAY_S = 0.5
CLAUDE_DELAY_S = 0.5
MAX_HTML_CHARS = 180_000
# Slow government sites; can override via pydantic REQUEST_TIMEOUT_SECONDS in .env
MIN_HTTP_TIMEOUT_S = 75.0


def _setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("park_authorities_enrichment")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    return log


def _load_env() -> None:
    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _row_needs_enrichment(row: dict[str, Any]) -> bool:
    for f in ENRICH_FIELDS:
        v = row.get(f)
        if v is None:
            continue
        if str(v).strip() == VERIFY:
            return True
    return False


def _truncate_html(html: str) -> str:
    if len(html) <= MAX_HTML_CHARS:
        return html
    head = (MAX_HTML_CHARS * 2) // 3
    tail = MAX_HTML_CHARS - head - 80
    return html[:head] + "\n<!-- TRUNCATED -->\n" + html[-tail:]


def _parse_json_from_claude(text: str) -> dict[str, Any]:
    t = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", t)
    if fence:
        t = fence.group(1).strip()
    return json.loads(t)


def _norm_value(val: Any, *, base_url: str, key: str) -> str:
    if val is None:
        return "NOT_FOUND"
    s = str(val).strip()
    if not s or s.lower() in ("null", "none", "n/a", "unknown"):
        return "NOT_FOUND"
    if key == "permit_url" and s != "NOT_FOUND":
        if s.startswith("//"):
            p = urlparse(base_url)
            s = f"{p.scheme}:{s}"
        elif s.startswith("/"):
            s = urljoin(base_url.rstrip("/") + "/", s)
        elif not re.match(r"^https?://", s, re.I):
            s = urljoin(base_url if base_url.endswith("/") else base_url + "/", s)
    return s


def _fetch_homepage(
    client: httpx.Client,
    url: str,
    user_agent: str,
    timeout_s: float,
    log: logging.Logger,
) -> str | None:
    try:
        r = client.get(
            url,
            headers={
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-AU,en;q=0.9",
            },
            follow_redirects=True,
            timeout=timeout_s,
        )
        r.raise_for_status()
        ct = (r.headers.get("content-type") or "").lower()
        if "text/html" not in ct and "application/xhtml" not in ct:
            log.warning("Non-HTML content-type for %s: %s", url, ct)
        return r.text
    except Exception as e:  # noqa: BLE001
        log.error("HTTP fetch failed url=%s err=%s", url, e)
        return None


def _claude_extract(
    *,
    api_key: str,
    model: str,
    homepage_url: str,
    authority_name: str,
    html: str,
    log: logging.Logger,
) -> dict[str, str] | None:
    from anthropic import Anthropic

    prompt = f"""Here is the HTML of the homepage for this Australian national park / parks authority:

Authority name: {authority_name}
Homepage URL: {homepage_url}

Extract from this national park authority website:
1. contact_email (look in Contact/About pages — use the main public enquiries email if shown)
2. contact_phone
3. wedding or event permit page URL (absolute https URL if possible)
4. HQ suburb/city (where head office is, not a random park location)
5. Typical permit fee if mentioned (short human-readable range, e.g. \"$100–$500\")

Return JSON only with exactly these keys (use null for unknown): email, phone, permit_url, hq_suburb, fee_range

HTML follows:
"""
    truncated = _truncate_html(html)
    user_content = prompt + "\n" + truncated

    try:
        ac = Anthropic(api_key=api_key)
        msg = ac.messages.create(
            model=model,
            max_tokens=1200,
            messages=[{"role": "user", "content": user_content}],
        )
        blocks = getattr(msg, "content", None) or []
        text_parts: list[str] = []
        for b in blocks:
            if hasattr(b, "text"):
                text_parts.append(b.text)
            elif isinstance(b, dict) and b.get("type") == "text":
                text_parts.append(str(b.get("text", "")))
        raw = "".join(text_parts).strip()
        data = _parse_json_from_claude(raw)
        return {
            "email": _norm_value(data.get("email"), base_url=homepage_url, key="email"),
            "phone": _norm_value(data.get("phone"), base_url=homepage_url, key="phone"),
            "permit_url": _norm_value(data.get("permit_url"), base_url=homepage_url, key="permit_url"),
            "hq_suburb": _norm_value(data.get("hq_suburb"), base_url=homepage_url, key="hq_suburb"),
            "fee_range": _norm_value(data.get("fee_range"), base_url=homepage_url, key="fee_range"),
        }
    except Exception as e:  # noqa: BLE001
        log.error("Claude extraction failed authority=%r err=%s", authority_name, e)
        return None


def main() -> None:
    _load_env()
    log = _setup_logging()
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    from data_builder.config import get_settings

    settings = get_settings()
    url = (settings.supabase_url or "").strip()
    key = (settings.supabase_service_role_key or "").strip()
    anthropic_key = (settings.anthropic_api_key or "").strip()
    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5-20250929").strip()
    http_timeout = max(MIN_HTTP_TIMEOUT_S, float(settings.request_timeout_seconds or MIN_HTTP_TIMEOUT_S))
    ua = (settings.scraper_user_agent or "MilestoneDataBuilder/0.1").strip()

    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required.")
    if not anthropic_key:
        raise SystemExit("ANTHROPIC_API_KEY is required.")

    from supabase import create_client

    sb = create_client(url, key)
    tbl = sb.schema("shared").table("ref_park_authorities")

    res = tbl.select(
        "npark_id,authority_name,website_homepage,"
        "contact_email,contact_phone,website_weddings_permit_page,hq_suburb,typical_permit_fee_range_aud"
    ).execute()
    rows: list[dict[str, Any]] = list(res.data or [])
    todo = [r for r in rows if _row_needs_enrichment(r)]
    log.info("Loaded %s park authority rows; %s need enrichment", len(rows), len(todo))

    if not todo:
        log.info("Nothing to do (no VERIFY_REQUIRED rows). Exiting.")
        return

    today = date.today().isoformat()
    ok = skipped = failed = 0

    log.info("HTTP timeout=%ss Claude model=%s", http_timeout, model)

    with httpx.Client() as http_client:
        for i, row in enumerate(todo):
            npark_id = row.get("npark_id")
            name = row.get("authority_name") or ""
            home = (row.get("website_homepage") or "").strip()
            log.info("(%s/%s) %s | %s", i + 1, len(todo), npark_id, name)

            if not home:
                log.warning("Empty website_homepage for %s; skip", npark_id)
                skipped += 1
                continue

            html = _fetch_homepage(http_client, home, ua, http_timeout, log)
            time.sleep(HTTP_DELAY_S)
            if not html:
                failed += 1
                continue

            extracted = _claude_extract(
                api_key=anthropic_key,
                model=model,
                homepage_url=home,
                authority_name=str(name),
                html=html,
                log=log,
            )
            time.sleep(CLAUDE_DELAY_S)
            if not extracted:
                failed += 1
                continue

            patch: dict[str, Any] = {}
            for col, ek in (
                ("contact_email", "email"),
                ("contact_phone", "phone"),
                ("website_weddings_permit_page", "permit_url"),
                ("hq_suburb", "hq_suburb"),
                ("typical_permit_fee_range_aud", "fee_range"),
            ):
                cur = row.get(col)
                if cur is not None and str(cur).strip() == VERIFY:
                    patch[col] = extracted[ek]

            if not patch:
                log.info("No VERIFY_REQUIRED fields to patch for %s (unexpected)", npark_id)
                skipped += 1
                continue

            patch["updated_at"] = today
            try:
                tbl.update(patch).eq("npark_id", npark_id).execute()
                log.info(
                    "Updated %s: email=%s phone=%s permit_page=%s hq=%s fee=%s",
                    npark_id,
                    patch.get("contact_email", "—"),
                    patch.get("contact_phone", "—"),
                    (patch.get("website_weddings_permit_page") or "")[:80],
                    patch.get("hq_suburb", "—"),
                    patch.get("typical_permit_fee_range_aud", "—"),
                )
                ok += 1
            except Exception as e:  # noqa: BLE001
                log.error("Supabase update failed npark_id=%s err=%s", npark_id, e)
                failed += 1

    log.info(
        "Done. updated=%s skipped=%s failed=%s log=%s",
        ok,
        skipped,
        failed,
        LOG_PATH,
    )
    print(f"Park authorities enrichment: updated={ok} skipped={skipped} failed={failed}")
    print(f"Log: {LOG_PATH}")


if __name__ == "__main__":
    main()
