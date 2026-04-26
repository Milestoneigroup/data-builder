"""Council permit enrichment driven by shared.ref_councils verified websites."""

from __future__ import annotations

import argparse
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
from bs4 import BeautifulSoup
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

LOG_PATH = _ROOT / "logs" / "council_permit_enrichment.log"
MAX_PAGE_CHARS = 70_000
HTTP_DELAY_S = 0.4
CLAUDE_DELAY_S = 0.5
LINK_HINTS = ("permit", "event", "park", "wedding", "outdoor", "reserve", "recreation")


def _setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("council_permit_enrichment")
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
    for p in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if p.is_file():
            load_dotenv(p, override=True, encoding="utf-8")


def _parse_json(text: str) -> dict[str, Any]:
    t = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", t)
    if m:
        t = m.group(1).strip()
    return json.loads(t)


def _norm_text(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in {"null", "none", "n/a", "unknown"}:
        return None
    return s


def _norm_bool(v: Any) -> bool | None:
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"true", "yes", "1"}:
        return True
    if s in {"false", "no", "0"}:
        return False
    return None


def _norm_int(v: Any) -> int | None:
    if isinstance(v, int):
        return v
    if v is None:
        return None
    d = re.sub(r"[^\d-]", "", str(v))
    if not d:
        return None
    try:
        return int(d)
    except ValueError:
        return None


def _norm_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if v is None:
        return []
    return [x.strip() for x in re.split(r"[,;\n]+", str(v)) if x.strip()]


def _fetch(client: httpx.Client, url: str, timeout_s: float, ua: str, log: logging.Logger) -> str | None:
    try:
        r = client.get(
            url,
            follow_redirects=True,
            timeout=timeout_s,
            headers={
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-AU,en;q=0.9",
            },
        )
        r.raise_for_status()
        return r.text
    except Exception as e:  # noqa: BLE001
        log.warning("Fetch failed url=%s err=%s", url, e)
        return None


def _snapshot(url: str, html: str) -> tuple[str, str, list[str]]:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.get_text(strip=True) if soup.title else "")[:300]
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    text = soup.get_text("\n", strip=True)
    if len(text) > MAX_PAGE_CHARS:
        text = text[:MAX_PAGE_CHARS] + "\n[TRUNCATED]"
    links: list[str] = []
    seen: set[str] = set()
    for a in soup.select("a[href]"):
        href = str(a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(url, href)
        if full in seen:
            continue
        seen.add(full)
        links.append(full)
    return title, text, links


def _permit_links(base_url: str, links: list[str]) -> list[str]:
    host = urlparse(base_url).netloc.lower()
    scored: list[tuple[int, str]] = []
    for link in links:
        p = urlparse(link)
        if p.scheme not in ("http", "https") or p.netloc.lower() != host:
            continue
        low = link.lower()
        score = sum(1 for k in LINK_HINTS if k in low)
        if score > 0:
            scored.append((score, link))
    scored.sort(key=lambda x: x[0], reverse=True)
    out: list[str] = []
    seen: set[str] = set()
    for _, link in scored:
        if link not in seen:
            seen.add(link)
            out.append(link)
        if len(out) >= 8:
            break
    return out


def _claude_text(msg: Any) -> str:
    parts: list[str] = []
    for b in getattr(msg, "content", []) or []:
        if hasattr(b, "text"):
            parts.append(str(b.text))
        elif isinstance(b, dict) and b.get("type") == "text":
            parts.append(str(b.get("text") or ""))
    return "".join(parts).strip()


def _extract_from_pages(*, anthropic_key: str, model: str, council: dict[str, Any], pages: list[dict[str, Any]]) -> dict[str, Any] | None:
    from anthropic import Anthropic

    body_parts: list[str] = []
    for i, page in enumerate(pages, start=1):
        body_parts.append(
            f"PAGE {i}\nURL: {page['url']}\nTITLE: {page['title']}\nCONTENT:\n{page['text']}\n"
        )
    prompt = (
        "Extract outdoor wedding/event permit information from this council website content.\n"
        "Return JSON only with keys: "
        "council_name,state_code,permit_page_url,permit_required,permit_fee_aud,permit_lead_time_days,"
        "max_guests_outdoor,approved_locations,restricted_times,insurance_required,insurance_min_cover_aud,"
        "alcohol_permitted,caterers_approved_list,contact_name,contact_email,contact_phone,application_url,"
        "application_form_url,notes,data_confidence.\n"
        f"Council name: {council.get('council_name')}\nState: {council.get('state_code')}\n"
        "If unknown, return null.\n\n"
        + "\n\n".join(body_parts)
    )
    try:
        ac = Anthropic(api_key=anthropic_key)
        msg = ac.messages.create(model=model, max_tokens=1800, messages=[{"role": "user", "content": prompt}])
        data = _parse_json(_claude_text(msg))
    except Exception:
        return None

    return {
        "council_name": _norm_text(data.get("council_name")) or _norm_text(council.get("council_name")),
        "state_code": _norm_text(data.get("state_code")) or _norm_text(council.get("state_code")),
        "permit_page_url": _norm_text(data.get("permit_page_url")),
        "permit_required": _norm_bool(data.get("permit_required")),
        "permit_fee_aud": _norm_text(data.get("permit_fee_aud")),
        "permit_lead_time_days": _norm_int(data.get("permit_lead_time_days")),
        "max_guests_outdoor": _norm_int(data.get("max_guests_outdoor")),
        "approved_locations": _norm_list(data.get("approved_locations")),
        "restricted_times": _norm_text(data.get("restricted_times")),
        "insurance_required": _norm_bool(data.get("insurance_required")),
        "insurance_min_cover_aud": _norm_text(data.get("insurance_min_cover_aud")),
        "alcohol_permitted": _norm_bool(data.get("alcohol_permitted")),
        "caterers_approved_list": _norm_bool(data.get("caterers_approved_list")),
        "contact_name": _norm_text(data.get("contact_name")),
        "contact_email": _norm_text(data.get("contact_email")),
        "contact_phone": _norm_text(data.get("contact_phone")),
        "application_url": _norm_text(data.get("application_url")),
        "application_form_url": _norm_text(data.get("application_form_url")),
        "notes": _norm_text(data.get("notes")),
        "scraped_date": date.today().isoformat(),
        "data_confidence": (_norm_text(data.get("data_confidence")) or "low").lower(),
    }


def _pick_mixed(rows: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        buckets.setdefault(str(r.get("state_code") or "UNK"), []).append(r)
    states = sorted(buckets.keys())
    out: list[dict[str, Any]] = []
    i = 0
    while len(out) < n:
        moved = False
        for st in states:
            arr = buckets[st]
            if i < len(arr):
                out.append(arr[i])
                moved = True
                if len(out) >= n:
                    break
        if not moved:
            break
        i += 1
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Council permit enrichment")
    parser.add_argument("--test-count", type=int, default=5)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    _load_env()
    log = _setup_logging()
    from data_builder.config import get_settings
    from supabase import create_client

    settings = get_settings()
    sb = create_client((settings.supabase_url or "").strip(), (settings.supabase_service_role_key or "").strip())
    anthropic_key = (settings.anthropic_api_key or "").strip()
    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5-20250929").strip()
    timeout_s = max(45.0, float(settings.request_timeout_seconds or 45.0))
    ua = (settings.scraper_user_agent or "MilestoneDataBuilder/0.1").strip()
    dry_run = not args.apply

    councils = (
        sb.schema("shared")
        .table("ref_councils")
        .select("council_id,council_name,state_code,website,aligned_destination_ids")
        .eq("is_active", True)
        .not_.is_("website", "null")
        .neq("website", "")
        .execute()
    ).data or []
    councils = [c for c in councils if re.match(r"^https?://", str(c.get("website") or "").strip(), re.I)]
    target = _pick_mixed(councils, args.test_count) if dry_run else councils

    results: list[dict[str, Any]] = []
    with httpx.Client() as client:
        for i, council in enumerate(target, start=1):
            site = str(council.get("website") or "").strip()
            log.info("[%s/%s] %s | %s", i, len(target), council.get("council_name"), site)
            html = _fetch(client, site, timeout_s, ua, log)
            time.sleep(HTTP_DELAY_S)
            if not html:
                continue
            title, text, links = _snapshot(site, html)
            pages = [{"url": site, "title": title, "text": text}]
            for link in _permit_links(site, links)[:2]:
                h2 = _fetch(client, link, timeout_s, ua, log)
                time.sleep(HTTP_DELAY_S)
                if not h2:
                    continue
                t2, txt2, _ = _snapshot(link, h2)
                pages.append({"url": link, "title": t2, "text": txt2})
            ex = _extract_from_pages(anthropic_key=anthropic_key, model=model, council=council, pages=pages)
            time.sleep(CLAUDE_DELAY_S)
            if not ex:
                continue
            ex["council_id"] = council.get("council_id")
            ex["aligned_destination_ids"] = council.get("aligned_destination_ids") or []
            results.append(ex)

    print("\n=== Extracted council permit JSON ===")
    for r in results[: args.test_count]:
        print(json.dumps(r, ensure_ascii=False, indent=2))
        print("---")

    if dry_run:
        print("\nDRY RUN complete. No DB writes were made.")
        print(f"Log: {LOG_PATH}")
        return

    upserts = 0
    url_updates = 0
    for r in results:
        for dest_id in r.get("aligned_destination_ids") or []:
            row = {
                "destination_id": dest_id,
                "council_name": r.get("council_name"),
                "state_code": r.get("state_code"),
                "permit_page_url": r.get("permit_page_url"),
                "permit_required": r.get("permit_required"),
                "permit_fee_aud": r.get("permit_fee_aud"),
                "permit_lead_time_days": r.get("permit_lead_time_days"),
                "max_guests_outdoor": r.get("max_guests_outdoor"),
                "approved_locations": r.get("approved_locations") or [],
                "restricted_times": r.get("restricted_times"),
                "insurance_required": r.get("insurance_required"),
                "insurance_min_cover_aud": r.get("insurance_min_cover_aud"),
                "alcohol_permitted": r.get("alcohol_permitted"),
                "caterers_approved_list": r.get("caterers_approved_list"),
                "contact_name": r.get("contact_name"),
                "contact_email": r.get("contact_email"),
                "contact_phone": r.get("contact_phone"),
                "application_url": r.get("application_url"),
                "application_form_url": r.get("application_form_url"),
                "notes": r.get("notes"),
                "scraped_date": r.get("scraped_date"),
                "data_confidence": r.get("data_confidence"),
            }
            sb.schema("shared").table("ref_council_permits").upsert(row, on_conflict="destination_id").execute()
            upserts += 1
            if r.get("permit_page_url"):
                sb.schema("shared").table("ref_destinations").update({"council_permit_url": r["permit_page_url"]}).eq(
                    "destination_id", dest_id
                ).execute()
                url_updates += 1

    print(f"\nApply complete. permit_upserts={upserts} destination_url_updates={url_updates}")
    print(f"Log: {LOG_PATH}")


if __name__ == "__main__":
    main()
