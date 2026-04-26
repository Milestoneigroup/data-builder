"""Google Places (New) for top 300 celebrants; writes ``data/celebrants_enriched_top300.csv``.

Search query: ``{full_name} celebrant {state} Australia`` — same API pattern as ``places_enrichment``.

Run: ``python -m scrapers.celebrant_places_enrichment``
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

MERGED = _ROOT / "data" / "celebrants_merged.csv"
OUT = _ROOT / "data" / "celebrants_enriched_top300.csv"
LOG = _ROOT / "logs" / "celebrant_places.log"
SENT = "VERIFY_REQUIRED"
TOP_N = 300
REQUEST_DELAY = 0.3

TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
TS_FIELD = "places.id,places.name,places.displayName,places.formattedAddress,places.rating,userRatingCount,places.websiteUri"
D_FIELD = "id,displayName,formattedAddress,rating,userRatingCount,websiteUri,nationalPhoneNumber"


def _is_sent(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return True
    t = str(v).strip()
    return t in ("", SENT) or t.lower() == "nan"


def _api_key() -> str:
    k = (os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
    if not k:
        print("ERROR: GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY not set", file=sys.stderr)
        sys.exit(1)
    return k


def _text_search(client: httpx.Client, query: str, key: str) -> dict[str, Any]:
    r = client.post(
        TEXT_SEARCH_URL,
        headers={"Content-Type": "application/json", "X-Goog-Api-Key": key, "X-Goog-FieldMask": TS_FIELD},
        json={"textQuery": query},
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json()


def _place_details(client: httpx.Client, place_id: str, key: str) -> dict[str, Any]:
    pid = quote(place_id, safe="")
    r = client.get(
        f"https://places.googleapis.com/v1/places/{pid}",
        headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": D_FIELD},
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json()


def _score_row(r: pd.Series) -> float:
    s = 0.0
    if not _is_sent(r.get("abia_winner", "")) and str(r.get("abia_winner", "")).lower() not in (
        "0",
        "no",
        "false",
    ):
        s += 1_000_000.0
    dqs = str(r.get("data_quality_score", "0") or "0")
    try:
        s += float(dqs)
    except ValueError:
        s += 0.0
    return s


def _local_name(p: dict[str, Any]) -> str:
    d = p.get("displayName") or {}
    if isinstance(d, dict) and d.get("text"):
        return str(d.get("text"))
    return str(p.get("name") or "")


def run() -> int:
    LOG.parent.mkdir(exist_ok=True)
    if not MERGED.is_file():
        print(f"ERROR: {MERGED} missing", file=sys.stderr)
        return 1
    key = _api_key()
    df = pd.read_csv(MERGED, dtype=str, keep_default_na=False)
    df["_sort"] = df.apply(_score_row, axis=1)
    df = df.sort_values("_sort", ascending=False).head(TOP_N).drop(columns=["_sort"])
    out_rows: list[dict[str, str]] = []
    with httpx.Client() as client:
        for i, (_, row) in enumerate(df.iterrows()):
            name = str(row.get("full_name", "")).strip()
            st = str(row.get("state", "")).strip()
            if st == SENT:
                st = ""
            q = f"{name} celebrant {st} Australia".strip()
            line = f"{i+1}/{len(df)} {q}\n"
            with LOG.open("a", encoding="utf-8") as log:
                log.write(f"{datetime.now(timezone.utc).isoformat()}\t{q}\n")
            try:
                data = _text_search(client, q, key)
                time.sleep(REQUEST_DELAY)
                places = data.get("places") or []
                if not places:
                    r = {str(k): str(v) for k, v in row.items()}
                    r["places_status"] = "no_results"
                    out_rows.append(r)
                    time.sleep(REQUEST_DELAY)
                    continue
                raw_id = str(places[0].get("name", "") or places[0].get("id", ""))
                if raw_id.startswith("places/"):
                    raw_id = raw_id.split("/", 1)[-1]
                time.sleep(REQUEST_DELAY)
                det = _place_details(client, raw_id, key)
            except Exception as e:  # noqa: BLE001
                r = {str(k): str(v) for k, v in row.items()}
                r["places_error"] = str(e)[:500]
                out_rows.append(r)
                time.sleep(REQUEST_DELAY)
                continue
            r = {str(k): str(v) for k, v in row.items()}
            r["google_place_id"] = str(
                (det.get("name") or det.get("id", "") or raw_id)
            )
            if r["google_place_id"].startswith("places/"):
                r["google_place_id"] = r["google_place_id"].split("/", 1)[-1]
            r["google_rating"] = str(det.get("rating", "") or SENT)
            r["google_review_count"] = str(det.get("userRatingCount", "") or SENT)
            w = str(det.get("websiteUri", "") or "")
            if w and _is_sent(r.get("website", SENT)):
                r["website_from_places"] = w
            elif w:
                r["website_from_places"] = w
            else:
                r["website_from_places"] = r.get("website_from_places", SENT)
            ph = str(det.get("nationalPhoneNumber", "") or "")
            if ph and _is_sent(r.get("phone", SENT)):
                r["phone_from_places"] = ph
            elif ph:
                r["phone_from_places"] = ph
            else:
                r["phone_from_places"] = r.get("phone_from_places", SENT)
            r["places_match_name"] = _local_name(det)
            r["last_places_enrich_at"] = datetime.now(timezone.utc).isoformat()
            r["last_updated_source"] = "google_places"
            out_rows.append(r)
            time.sleep(REQUEST_DELAY)
    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(OUT, index=False)
    print(f"Wrote {len(out_df)} rows to {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
