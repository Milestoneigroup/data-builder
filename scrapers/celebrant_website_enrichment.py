"""Claude (Anthropic) extraction from celebrant websites; updates ``celebrants_merged.csv``.

Run: ``python -m scrapers.celebrant_website_enrichment``
Requires ``ANTHROPIC_API_KEY`` in .env.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import httpx
import pandas as pd
from anthropic import Anthropic
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

MERGED = _ROOT / "data" / "celebrants_merged.csv"
AW = _ROOT / "data" / "celebrants_au_v1.csv"
LOG = _ROOT / "logs" / "celebrant_website_enrich.log"
SENT = "VERIFY_REQUIRED"
USER_PROMPT = """From this celebrant website extract the following. Return a single JSON object with these keys (use null for unknown):
- instagram: Instagram handle (without @) or full profile URL, or null
- facebook: Facebook page URL, or null
- phone_website: phone number if different from any phone already known, or null
- min_price_aud: number (AUD) for starting/minimum price, or null
- max_price_aud: number (AUD) for maximum/peak price, or null
- years_experience: years of experience as a number, or null; if only ceremony count, put in ceremony_count
- ceremony_count: estimated total ceremonies, or null
- languages_non_english: array of language names, or null
- celebrant_institute_member: true/false (Joshua Withers / Celebrant Institute)
- joshua_withers_mentioned: true/false
JSON only, no markdown."""

DELAY = 0.5
MAX_HTML = 100_000


def _is_sent(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    t = str(v).strip()
    return t == "" or t == SENT or t.lower() == "nan"


def _award_full_names() -> set[str]:
    if not AW.is_file():
        return set()
    a = pd.read_csv(AW, dtype=str, keep_default_na=False)
    c = "full_name" if "full_name" in a.columns else a.columns[0]
    return {_norm_n(str(x)) for x in a[c].values if str(x).strip()}


def _norm_n(s: str) -> str:
    return " ".join(s.lower().split())


def _build_queue(df: pd.DataFrame) -> list[int]:
    """1) The 27 award celebrants (by full_name match) with a website, 2) any other with website."""
    names = _award_full_names()
    first: list[int] = []
    second: list[int] = []
    seen: set[int] = set()
    for i, r in df.iterrows():
        ix = int(i) if not isinstance(i, int) else i
        w = str(r.get("website", "")).strip()
        if _is_sent(w) or not w.startswith("http"):
            continue
        fn = _norm_n(str(r.get("full_name", "")))
        in_award = bool(names) and fn in names
        if in_award:
            if ix not in seen:
                first.append(ix)
                seen.add(ix)
        else:
            if ix not in seen:
                second.append(ix)
                seen.add(ix)
    return first + [x for x in second if x not in first]


def _fetch_text(url: str) -> str:
    try:
        r = httpx.get(
            url,
            timeout=30.0,
            follow_redirects=True,
            headers={"User-Agent": "MilestoneDataBuilder/1.0 (celebrant-enrich; +https://milestonei.com)"},
        )
        r.raise_for_status()
        body = r.text
        if len(body) > MAX_HTML:
            body = body[:MAX_HTML]
        return body
    except Exception as e:  # noqa: BLE001
        return f"<!-- fetch error: {e} -->"


def _claude_json(client: Anthropic, html: str) -> dict:
    text = f"Website content (HTML or text) follows.\n\n{html[:80000]}"
    msg = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=1200,
        messages=[{"role": "user", "content": f"{USER_PROMPT}\n\n---\n{text}"}],
    )
    raw = msg.content[0].text
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return {}


def run() -> int:
    LOG.parent.mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", filename=LOG, filemode="a")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set; skipping website enrichment.", file=sys.stderr)
        return 0
    if not MERGED.is_file():
        print(f"ERROR: {MERGED} not found", file=sys.stderr)
        return 1
    df = pd.read_csv(MERGED, dtype=str, keep_default_na=False)
    client = Anthropic()
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).isoformat()
    n_done = 0
    for idx in _build_queue(df):
        row = df.loc[idx]
        url = str(row.get("website", "")).strip()
        if _is_sent(url) or not url.startswith("http"):
            continue
        logging.info("Enriching %s %s", row.get("celebrant_id"), url)
        html = _fetch_text(url)
        data = _claude_json(client, html)
        if data.get("instagram"):
            df.at[idx, "instagram_handle_or_url"] = str(data["instagram"])
        if data.get("facebook"):
            df.at[idx, "facebook_url"] = str(data["facebook"])
        if data.get("phone_website"):
            df.at[idx, "phone_from_website"] = str(data["phone_website"])
        for k, col in (
            ("min_price_aud", "min_price_aud"),
            ("max_price_aud", "max_price_aud"),
            ("years_experience", "years_experience"),
        ):
            if data.get(k) is not None:
                df.at[idx, col] = str(data[k])
        if data.get("ceremony_count") is not None:
            df.at[idx, "estimated_ceremonies"] = str(data["ceremony_count"])
        if data.get("languages_non_english"):
            df.at[idx, "languages_non_english"] = json.dumps(data["languages_non_english"])
        if data.get("celebrant_institute_member") is not None:
            df.at[idx, "celebrant_institute_member"] = str(data["celebrant_institute_member"]).lower()
        if data.get("joshua_withers_mentioned") is not None:
            df.at[idx, "joshua_withers_mentioned"] = str(data["joshua_withers_mentioned"]).lower()
        df.at[idx, "last_website_enrich_at"] = ts
        df.at[idx, "last_updated_source"] = "claude_website"
        n_done += 1
        time.sleep(DELAY)
    df.to_csv(MERGED, index=False)
    print(f"Updated {n_done} celebrants in {MERGED} (log: {LOG})")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
