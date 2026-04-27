"""Australian wedding influencer / content source discovery via Claude web search.

Loads existing homepage URLs from ``shared.ref_influencers`` (plus optional
``data/influencer_existing_urls.txt``) for deduplication, runs 52 fixed queries through
the Anthropic Messages API with the web_search tool, appends new rows to
``data/influencer_discovered_new.csv``, and **upserts** new sources into
``shared.ref_influencers`` (``discovery_source`` = ``auto_discovery_<date>``,
``data_confidence`` = ``low``).

Examples
--------
  # Smoke test (first query only):
  python scrapers/influencer_discovery.py --limit-queries 1

  # Full run (52 queries):
  python scrapers/influencer_discovery.py

Requires: ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY
with service privileges) for dedupe + inserts. Optional: ANTHROPIC_MODEL (must
support web search).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

DATA_DIR = _ROOT / "data"
LOG_DIR = _ROOT / "logs"
EXISTING_URLS_PATH = DATA_DIR / "influencer_existing_urls.txt"
OUTPUT_CSV_PATH = DATA_DIR / "influencer_discovered_new.csv"
LOG_PATH = LOG_DIR / "influencer_discovery.log"

SOURCE_TYPES = frozenset(
    {
        "photographer_blog",
        "bridal_editorial",
        "regional_guide",
        "cultural_specialist",
        "wedding_planner_blog",
        "venue_owner_content",
        "elopement_specialist",
        "government_permit",
        "local_tourism",
        "real_bride_community",
    }
)

QUERIES: list[str] = [
    '"best Australian wedding venue guide" site blog -pinterest -instagram',
    '"Hunter Valley wedding guide" blog 2024 2025',
    '"Yarra Valley wedding guide" blog',
    '"Margaret River wedding guide" blog',
    '"Byron Bay wedding guide" blog',
    '"Mornington Peninsula wedding guide"',
    '"Barossa Valley wedding guide"',
    '"Gold Coast Hinterland wedding guide"',
    '"Sunshine Coast wedding guide blog"',
    '"Blue Mountains wedding guide"',
    '"Noosa wedding guide blog"',
    '"Whitsundays wedding guide"',
    '"Tasmania wedding venue guide blog"',
    '"Perth wedding guide blog"',
    '"Adelaide Hills wedding guide blog"',
    '"Daylesford wedding guide blog"',
    '"Southern Highlands wedding guide"',
    '"Tropical North Queensland wedding blog"',
    '"Australian wedding blog" editorial magazine 2024 2025',
    '"Australian wedding inspiration blog" -pinterest',
    '"wedding blog Australia real weddings editorial"',
    '"luxury wedding blog Australia"',
    '"boho wedding blog Australia"',
    '"Australian wedding magazine online"',
    '"real weddings Australia blog 2024"',
    '"wedding photographer blog Western Australia"',
    '"wedding photographer blog Tasmania"',
    '"wedding photographer blog Canberra ACT"',
    '"wedding photographer blog Darwin NT"',
    '"wedding photographer blog South Australia"',
    '"Australian wedding planner blog"',
    '"Sydney wedding planner blog tips advice"',
    '"Melbourne wedding planner blog"',
    '"destination wedding planner Australia blog"',
    '"Chinese Australian wedding blog"',
    '"Indian Australian wedding blog"',
    '"Greek Australian wedding blog"',
    '"Italian Australian wedding ceremony blog"',
    '"Vietnamese Australian wedding blog"',
    '"Lebanese Australian wedding Sydney blog"',
    '"Filipino Australian wedding blog"',
    '"elopement Australia blog 2024"',
    '"micro wedding Australia blog"',
    '"intimate wedding Australia blog"',
    '"elope Tasmania blog"',
    '"elope Western Australia blog"',
    '"wedding weekend guide guests Australia blog"',
    '"real bride Australia blog forum community"',
    '"Australian bride planning blog 2024"',
    '"wedding ceremony permit beach Australia site:gov.au"',
    '"park wedding permit Victoria site:vic.gov.au"',
    '"outdoor wedding permit New South Wales site:nsw.gov.au"',
]

CSV_FIELDNAMES = [
    "source_id",
    "Name",
    "URL",
    "source_type",
    "states",
    "specialism",
    "instagram",
    "trust_level",
    "key_locations",
    "discovery_query",
    "data_confidence",
    "last_verified",
]

# Prefer older web search first — fewer moving parts for long batch runs.
WEB_SEARCH_TOOLS: list[dict[str, str]] = [
    {"type": "web_search_20250305", "name": "web_search"},
    {"type": "web_search_20260209", "name": "web_search"},
]


def _setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("influencer_discovery")
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


def _load_dotenv() -> None:
    from dotenv import load_dotenv

    for path in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
        if path.is_file():
            load_dotenv(path, override=True, encoding="utf-8")


def _norm_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "").rstrip("/") or ""
        return f"{p.scheme.lower()}://{host}{path}"
    except Exception:  # noqa: BLE001
        return u.lower().rstrip("/")


def _dedupe_key(url: str) -> str:
    return _norm_url(url)


def load_existing_urls(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    keys: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        k = _dedupe_key(line)
        if k:
            keys.add(k)
    return keys


def bootstrap_urls_from_csv(*, csv_path: Path, url_column: str, out_path: Path, log: logging.Logger) -> int:
    import pandas as pd

    df = pd.read_csv(csv_path)
    if url_column not in df.columns:
        raise SystemExit(f"Column {url_column!r} not in CSV. Columns: {list(df.columns)}")
    series = df[url_column].dropna().astype(str).str.strip()
    series = series[series.str.lower().str.startswith("http")]
    uniq = sorted({ _dedupe_key(u) for u in series if _dedupe_key(u) })
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(uniq) + ("\n" if uniq else ""), encoding="utf-8")
    log.info("Wrote %s URLs to %s", len(uniq), out_path)
    return len(uniq)


def _parse_json_array(text: str) -> list[dict[str, Any]]:
    t = (text or "").strip()
    if not t:
        return []
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", t, flags=re.I)
    if fence:
        t = fence.group(1).strip()
    try:
        data = json.loads(t)
    except json.JSONDecodeError:
        m = re.search(r"\[[\s\S]*\]", t)
        if not m:
            return []
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            return []
    if isinstance(data, dict) and "sources" in data:
        data = data["sources"]
    if not isinstance(data, list):
        return []
    out: list[dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            out.append(item)
    return out


def _coerce_row(
    raw: dict[str, Any],
    *,
    discovery_query: str,
    source_id: str,
    today: str,
) -> dict[str, str] | None:
    name = str(raw.get("name") or raw.get("Name") or "").strip()
    url = str(raw.get("url") or raw.get("URL") or "").strip()
    if not url or not name:
        return None
    st = str(raw.get("source_type") or "").strip()
    if st not in SOURCE_TYPES:
        return None
    states = str(raw.get("states") or "").strip()
    specialism = str(raw.get("specialism") or "").strip()
    instagram = str(raw.get("instagram") or "").strip()
    trust = str(raw.get("trust_level") or "medium").strip().lower()
    if trust not in {"high", "medium", "low"}:
        trust = "medium"
    kl = raw.get("key_locations")
    if isinstance(kl, list):
        key_locations = "; ".join(str(x).strip() for x in kl if str(x).strip())
    else:
        key_locations = str(kl or "").strip()
    return {
        "source_id": source_id,
        "Name": name,
        "URL": _norm_url(url) or url,
        "source_type": st,
        "states": states,
        "specialism": specialism,
        "instagram": instagram,
        "trust_level": trust,
        "key_locations": key_locations,
        "discovery_query": discovery_query,
        "data_confidence": str(raw.get("data_confidence") or "medium").strip() or "medium",
        "last_verified": str(raw.get("last_verified") or today).strip() or today,
    }


def _max_disc_index(path: Path) -> int:
    if not path.is_file():
        return 0
    mx = 0
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            sid = (row.get("source_id") or "").strip()
            m = re.match(r"INF-DISC-(\d+)$", sid, flags=re.I)
            if m:
                mx = max(mx, int(m.group(1)))
    return mx


def _load_output_url_keys(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    keys: set[str] = set()
    with path.open(encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            u = _dedupe_key(row.get("URL") or "")
            if u:
                keys.add(u)
    return keys


def _claude_search_and_extract(
    *,
    client: Any,
    model: str,
    query: str,
    existing_domains_hint: str,
    log: logging.Logger,
) -> str:
    user = f"""You are building a curated list of Australian wedding industry content sources (blogs, guides, magazines, planners, photographers, government permit pages).

Use web search for this exact research intent:
{query}

Rules for each candidate:
1. Must be Australian (.au strongly preferred, or clear AU focus and AU locations). Skip overseas-only sites.
2. Must be substantially about weddings (planning, real weddings, venues, photography, elopements, cultural weddings, permits). Skip generic travel with no wedding angle.
3. Prefer canonical site homepage or main blog index URL (not a single dated article unless it is a government permit page).
4. Skip: pinterest.com, instagram.com, facebook.com, etsy.com, pure aggregator with no editorial voice.

Known existing domains/URLs to AVOID (already in directory — do not include again):
{existing_domains_hint}

Return **JSON only** (no markdown), a JSON array of up to 12 objects, each:
{{
  "name": "site or publication name",
  "url": "https://...",
  "source_type": "one of: photographer_blog, bridal_editorial, regional_guide, cultural_specialist, wedding_planner_blog, venue_owner_content, elopement_specialist, government_permit, local_tourism, real_bride_community",
  "states": "comma-separated Australian state codes e.g. NSW,VIC or ALL if national",
  "specialism": "one sentence niche",
  "instagram": "@handle or empty string if unknown",
  "trust_level": "high|medium|low per rubric: high = known publication or established brand with regular editorial; medium = active blog with identifiable voice; low = thin or unclear authorship",
  "key_locations": ["City or region", "..."],
  "data_confidence": "medium"
}}

If nothing qualifies, return [].
"""
    last_err: Exception | None = None
    for tool in WEB_SEARCH_TOOLS:
        try:
            msg = client.messages.create(
                model=model,
                max_tokens=8192,
                messages=[{"role": "user", "content": user}],
                tools=[tool],
            )
            parts: list[str] = []
            for b in msg.content or []:
                if hasattr(b, "text"):
                    parts.append(getattr(b, "text", "") or "")
                elif isinstance(b, dict) and b.get("type") == "text":
                    parts.append(str(b.get("text", "")))
            return "".join(parts).strip()
        except Exception as e:  # noqa: BLE001
            last_err = e
            log.warning("Claude call failed tool=%s err=%s — retrying next tool variant.", tool.get("type"), e)
    if last_err:
        raise last_err
    return ""


def _anthropic_timeout_s() -> float:
    return float(os.getenv("ANTHROPIC_TIMEOUT_S", "900"))


def run(
    *,
    limit_queries: int | None,
    resume: bool,
    allow_empty_existing: bool,
    log: logging.Logger,
) -> None:
    _load_dotenv()
    from anthropic import Anthropic

    from data_builder.config import get_settings
    from supabase import create_client

    from scrapers.load_influencers_supabase import (
        fetch_url_to_source_id,
        influencer_rec_from_discovery_row,
        upsert_single_influencer,
    )

    settings = get_settings()
    api_key = (settings.anthropic_api_key or os.getenv("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("ANTHROPIC_API_KEY is required.")

    model = (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5-20250929").strip()

    sb_url = (settings.supabase_url or "").strip()
    sb_key = (settings.supabase_service_role_key or settings.supabase_key or "").strip()
    sb_client = create_client(sb_url, sb_key) if sb_url and sb_key else None
    if sb_client is None:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    existing: set[str] = set(load_existing_urls(EXISTING_URLS_PATH))
    url_map = fetch_url_to_source_id(sb_client)
    existing |= set(url_map.keys())
    if not existing and not allow_empty_existing:
        raise SystemExit(
            f"No URLs loaded from Supabase or {EXISTING_URLS_PATH}. "
            "Load influencers first, or pass --allow-empty-existing (not recommended)."
        )

    try:
        EXISTING_URLS_PATH.parent.mkdir(parents=True, exist_ok=True)
        EXISTING_URLS_PATH.write_text("\n".join(sorted(existing)) + ("\n" if existing else ""), encoding="utf-8")
        log.info("Synced %s URL keys to %s", len(existing), EXISTING_URLS_PATH)
    except Exception as e:  # noqa: BLE001
        log.warning("Could not write %s: %s", EXISTING_URLS_PATH, e)

    hint_lines = sorted(existing)[:80]
    existing_hint = "\n".join(hint_lines) if hint_lines else "(none — directory empty)"

    seen: set[str] = set(existing)
    dupes_session = 0
    new_rows: list[dict[str, str]] = []
    supabase_inserts = 0

    if resume and OUTPUT_CSV_PATH.is_file():
        seen |= _load_output_url_keys(OUTPUT_CSV_PATH)
        log.info("Resume: merged %s URLs from existing output into dedupe set.", len(seen) - len(existing))

    next_idx = _max_disc_index(OUTPUT_CSV_PATH) + 1
    if not resume or next_idx == 1:
        if OUTPUT_CSV_PATH.is_file() and not resume:
            OUTPUT_CSV_PATH.unlink()
        write_header = not (OUTPUT_CSV_PATH.is_file() and resume)
        if write_header:
            with OUTPUT_CSV_PATH.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                w.writeheader()

    today = date.today().isoformat()
    discovery_tag = f"auto_discovery_{today}"
    client = Anthropic(api_key=api_key, timeout=_anthropic_timeout_s())
    queries = QUERIES[: limit_queries if limit_queries else len(QUERIES)]
    total_q = len(queries)

    trust_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    state_counts: Counter[str] = Counter()

    for i, q in enumerate(queries, start=1):
        print(f"Query {i}/{total_q} | New found: {len(new_rows)} | Dupes skipped: {dupes_session}", flush=True)
        log.info("Query %s/%s | %s", i, total_q, q[:200])
        text = ""
        try:
            text = _claude_search_and_extract(
                client=client,
                model=model,
                query=q,
                existing_domains_hint=existing_hint,
                log=log,
            )
            batch = _parse_json_array(text)
        except Exception as e:  # noqa: BLE001
            log.error("Query failed: %s", e)
            batch = []

        if not batch and (text or "").strip():
            log.warning("Unparseable JSON (first 400 chars): %s", text[:400].replace("\n", " "))

        added_this_query = 0
        for raw in batch:
            url_key = _dedupe_key(str(raw.get("url") or raw.get("URL") or ""))
            if not url_key:
                continue
            if url_key in seen:
                dupes_session += 1
                log.debug("skip dupe: %s", url_key)
                continue
            sid = f"INF-DISC-{next_idx:03d}"
            row = _coerce_row(raw, discovery_query=q, source_id=sid, today=today)
            if not row:
                continue
            if _dedupe_key(row["URL"]) != url_key:
                url_key = _dedupe_key(row["URL"])
            if url_key in seen:
                dupes_session += 1
                continue
            seen.add(url_key)
            new_rows.append(row)
            try:
                rec = influencer_rec_from_discovery_row(row, discovery_source=discovery_tag)
                kind, _ = upsert_single_influencer(sb_client, rec, url_map)
                if kind == "insert":
                    supabase_inserts += 1
            except Exception as e:  # noqa: BLE001
                log.error("Supabase upsert failed for %s: %s", url_key, e)
            trust_counts[row["trust_level"]] += 1
            type_counts[row["source_type"]] += 1
            for p in re.split(r"[,;]+", row["states"]):
                p = p.strip().upper()
                if p and p != "ALL":
                    state_counts[p] += 1
                elif p == "ALL":
                    state_counts["ALL"] += 1
            with OUTPUT_CSV_PATH.open("a", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                w.writerow(row)
            next_idx += 1
            added_this_query += 1
            existing_hint = "\n".join(sorted(seen)[:80])

        log.info("Query %s done: added %s rows (parse len=%s).", i, added_this_query, len(batch))
        time.sleep(float(os.getenv("INFLUENCER_DISCOVERY_DELAY_S", "2.0")))

    print(f"Query {total_q}/{total_q} | New found: {len(new_rows)} | Dupes skipped: {dupes_session}", flush=True)
    log.info(
        "Finished. queries=%s new_sources=%s dupes_skipped=%s",
        total_q,
        len(new_rows),
        dupes_session,
    )
    by_type = dict(type_counts.most_common())
    by_state = dict(state_counts.most_common())
    summary = (
        f"\n=== Influencer discovery summary ===\n"
        f"  Queries run: {total_q}\n"
        f"  New sources found: {len(new_rows)}\n"
        f"  Dupes skipped (existing + session): {dupes_session}\n"
        f"  By type: {by_type}\n"
        f"  By state token: {by_state}\n"
        f"  High trust: {trust_counts.get('high', 0)}\n"
        f"  Medium trust: {trust_counts.get('medium', 0)}\n"
        f"  Low trust: {trust_counts.get('low', 0)}\n"
        f"  Saved to: {OUTPUT_CSV_PATH}\n"
    )
    print(summary, flush=True)
    log.info(summary.strip())
    print(f"New sources added to Supabase: {supabase_inserts}", flush=True)
    log.info("New sources added to Supabase: %s", supabase_inserts)
    try:
        EXISTING_URLS_PATH.write_text("\n".join(sorted(seen)) + ("\n" if seen else ""), encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        log.warning("Could not refresh %s: %s", EXISTING_URLS_PATH, e)


def main() -> None:
    ap = argparse.ArgumentParser(description="Claude web search influencer discovery (AU wedding sources).")
    ap.add_argument(
        "--bootstrap-from-csv",
        type=Path,
        help="Read URLs from this CSV column and write influencer_existing_urls.txt",
    )
    ap.add_argument("--url-column", default="URL", help="Column name for URLs when bootstrapping (default: URL).")
    ap.add_argument("--limit-queries", type=int, default=None, help="Only run first N queries (smoke test).")
    ap.add_argument("--resume", action="store_true", help="Append to output CSV and continue source_id sequence.")
    ap.add_argument(
        "--allow-empty-existing",
        action="store_true",
        help="Allow run when influencer_existing_urls.txt is empty (dedupe within run only).",
    )
    args = ap.parse_args()
    log = _setup_logging()

    if args.bootstrap_from_csv:
        n = bootstrap_urls_from_csv(
            csv_path=args.bootstrap_from_csv.expanduser().resolve(),
            url_column=args.url_column,
            out_path=EXISTING_URLS_PATH,
            log=log,
        )
        print(f"Bootstrap complete: {n} URLs -> {EXISTING_URLS_PATH}", flush=True)
        return

    run(
        limit_queries=args.limit_queries,
        resume=args.resume,
        allow_empty_existing=args.allow_empty_existing,
        log=log,
    )


if __name__ == "__main__":
    main()
