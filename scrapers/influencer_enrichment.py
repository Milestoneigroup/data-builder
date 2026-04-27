"""Enrich ``shared.ref_influencers`` (Claude + light blog scrape) for Railway.

Processes rows where ``specialism_primary`` is null, with a 3s delay between sources.
Writes ``ref_influencer_content`` and ``ref_influencer_signals``.

Run: ``python -m scrapers.influencer_enrichment``

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY``, ``ANTHROPIC_API_KEY``.
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
from bs4 import BeautifulSoup
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger(__name__)
DELAY_S = float(os.getenv("INFLUENCER_ENRICH_DELAY_S", "3"))
BATCH_LOG = 10

CLAUDE_PROMPT = """You are building a wedding influencer intelligence database for an Australian wedding insurance company.

Analyse this wedding website and return JSON only:
{
  "founder_name": "string or null",
  "founder_gender": "M/F/NB or null",
  "specialism_primary": "one of [planning/photography/styling/venue_discovery/fashion_dress/honeymoon_travel/cultural/elopement/lgbtq/budget/luxury/real_weddings/suppliers/entertainment/food_cake/flowers]",
  "specialism_tags": "pipe-delimited secondary tags",
  "specialism_description": "2 sentences max",
  "audience_size_estimate": "small/medium/large/mega",
  "audience_type": "couples/industry/both",
  "contact_email": "string or null",
  "instagram_handle": "string or null",
  "tiktok_handle": "string or null",
  "pinterest_handle": "string or null",
  "facebook_url": "string or null",
  "youtube_channel": "string or null",
  "has_advertising": "true/false/null",
  "has_affiliate": "true/false/null",
  "has_brand_collab": "true/false/null",
  "partnership_potential": "high/medium/low",
  "mig_relevance_score": "1-10 integer",
  "insurance_hook": "true/false",
  "insurance_hook_reason": "string or null",
  "blog_index_url": "URL or null",
  "about_url": "URL or null",
  "avg_posts_per_month": "integer or null"
}"""

PRIMARY_SPECIALISMS = frozenset(
    {
        "planning",
        "photography",
        "styling",
        "venue_discovery",
        "fashion_dress",
        "honeymoon_travel",
        "cultural",
        "elopement",
        "lgbtq",
        "budget",
        "luxury",
        "real_weddings",
        "suppliers",
        "entertainment",
        "food_cake",
        "flowers",
    }
)

INSURANCE_TITLE_KW = (
    "cancel",
    "weather",
    "risk",
    "permit",
    "vendor",
    "supplier",
    "fail",
    "rain",
    "backup",
    "insurance",
    "cost",
    "outdoor",
    "policy",
    "disaster",
    "emergency",
)

_JSON_OBJ = re.compile(r"\{[\s\S]*\}")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def _fetch_text(client: httpx.Client, url: str, headers: dict[str, str], timeout: float) -> str:
    try:
        r = client.get(url, headers=headers, timeout=timeout, follow_redirects=True)
        if r.status_code >= 400:
            return ""
        return r.text or ""
    except Exception:  # noqa: BLE001
        LOG.debug("fetch failed %s", url, exc_info=True)
        return ""


def _parse_json_obj(text: str) -> dict[str, Any]:
    t = (text or "").strip()
    if not t:
        return {}
    m = _JSON_OBJ.search(t)
    if not m:
        return {}
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _tri_bool(v: Any) -> bool | None:
    if v is None or v == "null":
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def _int_or_none(v: Any) -> int | None:
    if v is None or v == "null":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _norm_primary(v: Any) -> str | None:
    if not v:
        return None
    s = str(v).strip().lower()
    return s if s in PRIMARY_SPECIALISMS else None


def _collab_angle(primary: str | None) -> str:
    if primary == "planning":
        return "Rain predictor tool / permit guide"
    if primary == "photography":
        return "Venue photo gallery partnership"
    if primary == "real_weddings":
        return "Weather risk real wedding feature"
    if primary == "venue_discovery":
        return "WedTools venue finder integration"
    if primary == "elopement":
        return "Elopement permit guide collaboration"
    return "Wedding risk planning content collaboration"


def _outreach_priority(score: int | None) -> str:
    if score is None:
        return "p3_backlog"
    if score >= 8:
        return "p1_this_month"
    if score >= 6:
        return "p2_this_quarter"
    return "p3_backlog"


def _insurance_hit_title(title: str) -> bool:
    t = title.lower()
    return any(k in t for k in INSURANCE_TITLE_KW)


def _content_suffix(source_id: str) -> str:
    m = re.search(r"(\d+)$", source_id or "")
    if m:
        return m.group(1)[-3:].zfill(3)
    return "000"


def _next_content_seq(client: Any, source_id: str) -> int:
    resp = (
        client.schema("shared")
        .table("ref_influencer_content")
        .select("content_id")
        .eq("source_id", source_id)
        .execute()
    )
    rows = getattr(resp, "data", None) or []
    mx = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("content_id") or "")
        parts = cid.split("-")
        if parts and parts[-1].isdigit():
            mx = max(mx, int(parts[-1]))
    return mx + 1


def _extract_articles(html: str, index_url: str, limit: int = 10) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    base_p = urlparse(index_url)
    host = (base_p.netloc or "").lower()
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        title = (a.get_text() or "").strip()
        if not href or len(title) < 12:
            continue
        absu = urljoin(index_url, href)
        pu = urlparse(absu)
        if (pu.netloc or "").lower() != host:
            continue
        path = (pu.path or "").lower()
        if any(x in path for x in ("/about", "/contact", "/privacy", "/tag/", "/page/", "/author/", "/login")):
            continue
        if absu.rstrip("/") == index_url.rstrip("/"):
            continue
        slug = path.rstrip("/").split("/")[-1]
        if not slug or slug in ("blog", "category", "archives"):
            continue
        if "/20" in path or "/blog" in path or len(slug) > 14:
            if absu in seen:
                continue
            seen.add(absu)
            out.append({"title": title[:500], "url": absu, "published_date": None, "author_name": None})
        if len(out) >= limit * 4:
            break
    return out[:limit]


def _fetch_all_pending(client: Any, *, page: int = 200) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        resp = (
            client.schema("shared")
            .table("ref_influencers")
            .select("source_id,name,url,about_url,blog_index_url,specialism_primary,is_active,notes")
            .eq("is_active", True)
            .is_("specialism_primary", None)
            .order("source_id")
            .range(offset, offset + page - 1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if not rows:
            break
        out.extend(r for r in rows if isinstance(r, dict))
        if len(rows) < page:
            break
        offset += page
    return out


def main() -> int:
    _setup_logging()
    from anthropic import Anthropic

    from data_builder.config import get_settings
    from supabase import create_client

    s = get_settings()
    sb_url = (s.supabase_url or "").strip()
    sb_key = (s.supabase_service_role_key or s.supabase_key or "").strip()
    anth_key = (s.anthropic_api_key or os.getenv("ANTHROPIC_API_KEY") or "").strip()
    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-20250514").strip()
    ua = (s.scraper_user_agent or "MilestoneDataBuilder/0.1").strip()
    timeout = float(s.request_timeout_seconds or 30.0)

    if not sb_url or not sb_key:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required.")
        return 1
    if not anth_key:
        LOG.error("ANTHROPIC_API_KEY required.")
        return 1

    client_sb = create_client(sb_url, sb_key)
    anth = Anthropic(api_key=anth_key, timeout=float(os.getenv("ANTHROPIC_TIMEOUT_S", "900")))
    headers = {"User-Agent": ua, "Accept": "text/html,application/xhtml+xml"}

    pending = _fetch_all_pending(client_sb)
    processed = 0
    with httpx.Client() as http:
        for row in pending:
            sid = str(row.get("source_id") or "")
            name = str(row.get("name") or "")
            home = str(row.get("url") or "").strip()
            about_u = (row.get("about_url") or "").strip() if row.get("about_url") else ""
            if not sid or not home:
                continue

            home_html = _fetch_text(http, home, headers, timeout)
            about_html = _fetch_text(http, about_u, headers, timeout) if about_u else ""
            bundle = f"=== HOMEPAGE {home} ===\n{home_html[:120_000]}\n\n=== ABOUT {about_u or '(none)'} ===\n{about_html[:80_000]}"

            user_msg = CLAUDE_PROMPT + "\n\n--- SITE CONTENT ---\n" + bundle
            try:
                msg = anth.messages.create(
                    model=model,
                    max_tokens=4096,
                    messages=[{"role": "user", "content": user_msg}],
                )
                text_parts: list[str] = []
                for b in msg.content or []:
                    if hasattr(b, "text"):
                        text_parts.append(getattr(b, "text", "") or "")
                raw_text = "".join(text_parts)
                data = _parse_json_obj(raw_text)
            except Exception as e:  # noqa: BLE001
                LOG.warning("Claude failed for %s: %s", sid, e)
                time.sleep(DELAY_S)
                processed += 1
                continue

            primary = _norm_primary(data.get("specialism_primary"))
            tags = data.get("specialism_tags")
            if isinstance(tags, list):
                tags = "|".join(str(x).strip() for x in tags if str(x).strip())
            else:
                tags = str(tags or "").strip() or None

            score = _int_or_none(data.get("mig_relevance_score"))
            if score is not None:
                score = max(1, min(10, score))

            ins_hook = bool(data.get("insurance_hook")) if data.get("insurance_hook") is not None else False
            hook_reason = data.get("insurance_hook_reason")
            hook_reason_s = str(hook_reason).strip() if hook_reason else None

            fg_raw = data.get("founder_gender")
            fg = str(fg_raw).strip().upper() if fg_raw not in (None, "", "null") else ""
            if fg in ("M", "F", "NB"):
                founder_gender = fg
            elif fg in ("FEMALE", "WOMAN"):
                founder_gender = "F"
            elif fg in ("MALE", "MAN"):
                founder_gender = "M"
            elif fg in ("NONBINARY", "NON-BINARY", "ENBY"):
                founder_gender = "NB"
            else:
                founder_gender = None

            ig_raw = data.get("instagram_handle")
            instagram_handle = str(ig_raw).strip().lstrip("@") if ig_raw not in (None, "", "null") else None

            asz = data.get("audience_size_estimate")
            if str(asz or "").strip().lower() not in ("small", "medium", "large", "mega"):
                asz = None
            atype = data.get("audience_type")
            if str(atype or "").strip().lower() not in ("couples", "industry", "both"):
                atype = None
            pp = data.get("partnership_potential")
            if str(pp or "").strip().lower() not in ("high", "medium", "low"):
                pp = None

            upd: dict[str, Any] = {
                "founder_name": data.get("founder_name") or None,
                "founder_gender": founder_gender,
                "specialism_primary": primary,
                "specialism_tags": tags,
                "specialism_description": data.get("specialism_description") or None,
                "audience_size_estimate": asz,
                "audience_type": atype,
                "contact_email": data.get("contact_email") or None,
                "instagram_handle": instagram_handle,
                "tiktok_handle": data.get("tiktok_handle") or None,
                "pinterest_handle": data.get("pinterest_handle") or None,
                "facebook_url": data.get("facebook_url") or None,
                "youtube_channel": data.get("youtube_channel") or None,
                "has_advertising": _tri_bool(data.get("has_advertising")),
                "has_affiliate": _tri_bool(data.get("has_affiliate")),
                "has_brand_collab": _tri_bool(data.get("has_brand_collab")),
                "partnership_potential": pp,
                "mig_relevance_score": score,
                "insurance_hook": ins_hook,
                "blog_index_url": data.get("blog_index_url") or row.get("blog_index_url") or None,
                "about_url": data.get("about_url") or (about_u or None),
                "avg_posts_per_month": _int_or_none(data.get("avg_posts_per_month")),
            }
            upd = {k: v for k, v in upd.items() if v is not None}

            if hook_reason_s:
                note_append = f"Insurance hook (enrichment): {hook_reason_s[:500]}"
                prev_notes = str(row.get("notes") or "").strip()
                upd["notes"] = (prev_notes + "\n" + note_append).strip() if prev_notes else note_append

            try:
                client_sb.schema("shared").table("ref_influencers").update(upd).eq("source_id", sid).execute()
            except Exception as e:  # noqa: BLE001
                LOG.warning("DB update failed %s: %s", sid, e)
                time.sleep(DELAY_S)
                processed += 1
                continue

            any_article_ins = False
            blog_url = str(upd.get("blog_index_url") or row.get("blog_index_url") or "").strip()
            if blog_url:
                idx_html = _fetch_text(http, blog_url, headers, timeout)
                articles = _extract_articles(idx_html, blog_url, limit=10)
                suf = _content_suffix(sid)
                seq0 = _next_content_seq(client_sb, sid)
                seq = seq0
                for art in articles:
                    title = str(art.get("title") or "")
                    aurl = str(art.get("url") or "")
                    if not title or not aurl:
                        continue
                    ins_rel = _insurance_hit_title(title)
                    if ins_rel:
                        any_article_ins = True
                    cid = f"CONT-INF-{suf}-{seq:03d}"
                    seq += 1
                    row_ins = {
                        "content_id": cid,
                        "source_id": sid,
                        "title": title,
                        "url": aurl,
                        "published_date": art.get("published_date"),
                        "author_name": art.get("author_name"),
                        "insurance_relevant": ins_rel,
                        "scraped_date": date.today().isoformat(),
                    }
                    try:
                        client_sb.schema("shared").table("ref_influencer_content").insert(row_ins).execute()
                    except Exception:  # noqa: BLE001
                        LOG.debug("content insert skip %s", aurl, exc_info=True)

            final_ins_hook = bool(ins_hook or any_article_ins)
            if final_ins_hook != ins_hook:
                try:
                    client_sb.schema("shared").table("ref_influencers").update({"insurance_hook": final_ins_hook}).eq(
                        "source_id", sid
                    ).execute()
                except Exception:  # noqa: BLE001
                    pass

            prio = _outreach_priority(score)
            sig = {
                "source_id": sid,
                "outreach_priority": prio,
                "best_collab_angle": _collab_angle(primary),
                "we_can_offer": "directory_listing|article_citations|venue_referrals",
                "has_sponsored_content": bool(_tri_bool(data.get("has_advertising"))),
            }
            try:
                client_sb.schema("shared").table("ref_influencer_signals").upsert(sig, on_conflict="source_id").execute()
            except Exception as e:  # noqa: BLE001
                LOG.warning("signals upsert failed %s: %s", sid, e)

            ins_yes = "yes" if final_ins_hook else "no"
            LOG.info("Enriched: %s | score:%s | insurance:%s | priority:%s", name, score, ins_yes, prio)
            processed += 1
            if processed % BATCH_LOG == 0:
                print(f"Progress: enriched {processed} sources", flush=True)

            time.sleep(DELAY_S)

    print(f"Done. Total enriched this run: {processed}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
