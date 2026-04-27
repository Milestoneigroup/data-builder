"""Monthly venue snapshot: Places, Pollen, Air Quality, Claude sentiment -> Supabase venue_ratings.

Run once: ``python -m scrapers.monthly_snapshot --run-now`` (venue snapshot only).

On Railway (default): ``python -m scrapers.monthly_snapshot`` starts a **BlockingScheduler** (UTC):

* **1st** 17:00 — venue snapshot (Places, pollen, AQ, sentiment) → ``venue_ratings``
* **5th** 18:00 — influencer enrichment (:mod:`scrapers.influencer_enrichment`)
* **6th** 18:00 — influencer discovery (:mod:`scrapers.influencer_discovery`)
* **Quarterly** (Jan/Apr/Jul/Oct **5th** 17:00) — AFCC profiles (:mod:`scrapers.afcc_profile_scraper`)

Requires env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY with service privileges),
GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY, ANTHROPIC_API_KEY.

Supabase ``venues`` rows must include ``id``, ``place_id``, and coordinates as ``latitude``/``longitude``
or ``lat``/``lng``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

LOG = logging.getLogger(__name__)
PROGRESS_EVERY = 50
REQUEST_DELAY_S = 0.3
PLACES_FIELD_MASK = "rating,userRatingCount,businessStatus,reviews"
PLACES_GET_TMPL = "https://places.googleapis.com/v1/places/{place_id}"
POLLEN_URL = "https://pollen.googleapis.com/v1/forecast:lookup"
AIR_URL = "https://airquality.googleapis.com/v1/currentConditions:lookup"
SYDNEY_TZ = ZoneInfo("Australia/Sydney")

POLLEN_ORDER = {"GRASS": 0, "TREE": 1, "WEED": 2}


def _sleep() -> None:
    time.sleep(REQUEST_DELAY_S)


def _venue_coords(row: dict[str, Any]) -> tuple[float | None, float | None]:
    lat = row.get("latitude")
    if lat is None:
        lat = row.get("lat")
    lng = row.get("longitude")
    if lng is None:
        lng = row.get("lng")
    try:
        if lat is not None and lng is not None:
            return float(lat), float(lng)
    except (TypeError, ValueError):
        pass
    return None, None


def _place_id_raw(row: dict[str, Any]) -> str:
    pid = row.get("place_id") or row.get("google_place_id") or ""
    return str(pid).strip()


def _normalize_place_id_for_url(place_id: str) -> str:
    s = place_id.strip()
    if s.startswith("places/"):
        return s.split("/", 1)[1]
    return s


def _localized_review_text(rv: dict[str, Any]) -> str:
    for key in ("text", "originalText"):
        t = rv.get(key)
        if isinstance(t, dict):
            s = t.get("text")
            if s:
                return str(s)
        elif isinstance(t, str) and t:
            return t
    return ""


def _extract_reviews_places(data: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    reviews = data.get("reviews") if isinstance(data.get("reviews"), list) else []
    for i in range(5):
        n = i + 1
        if i < len(reviews) and isinstance(reviews[i], dict):
            rv = reviews[i]
            text = _localized_review_text(rv)
            auth = rv.get("authorAttribution") or {}
            author = str(auth.get("displayName") or "") if isinstance(auth, dict) else ""
            rating_rv = rv.get("rating")
            try:
                rating_int = int(rating_rv) if rating_rv is not None else None
            except (TypeError, ValueError):
                rating_int = None
            pub = str(rv.get("publishTime") or "")
            rd: datetime | None = None
            if pub:
                try:
                    rd = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                except ValueError:
                    rd = None
        else:
            text, author, rating_int, rd = "", "", None, None
        out[f"review_text_{n}"] = text or None
        out[f"review_author_{n}"] = author or None
        out[f"review_rating_{n}"] = rating_int
        out[f"review_date_{n}"] = rd.isoformat() if rd else None
    return out


def _places_snapshot(client: httpx.Client, api_key: str, place_id: str) -> dict[str, Any]:
    pid = quote(_normalize_place_id_for_url(place_id), safe="")
    url = PLACES_GET_TMPL.format(place_id=pid)
    r = client.get(
        url,
        headers={
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": PLACES_FIELD_MASK,
        },
        timeout=60.0,
    )
    _sleep()
    if r.status_code >= 400:
        try:
            body = r.json()
            msg = (body.get("error") or {}).get("message", r.text[:400])
        except Exception:  # noqa: BLE001
            msg = r.text[:400] if r.text else str(r.status_code)
        return {"_error": msg}
    data = r.json()
    if not isinstance(data, dict):
        return {"_error": "invalid_json"}
    if "error" in data:
        err = data.get("error") or {}
        return {"_error": str(err.get("message", err))}
    rating = data.get("rating")
    try:
        rating_f = float(rating) if rating is not None else None
    except (TypeError, ValueError):
        rating_f = None
    urc = data.get("userRatingCount")
    try:
        review_count = int(urc) if urc is not None else None
    except (TypeError, ValueError):
        review_count = None
    merged = {
        "google_rating": rating_f,
        "review_count": review_count,
        **_extract_reviews_places(data),
    }
    return merged


def _pollen_index_from_type_info(items: list[dict[str, Any]], code: str) -> int | None:
    for it in items:
        if not isinstance(it, dict):
            continue
        if str(it.get("code", "")).upper() != code:
            continue
        idx = it.get("indexInfo")
        if not isinstance(idx, dict):
            return None
        val = idx.get("value")
        try:
            if val is None:
                return None
            v = int(val)
            if 0 <= v <= 5:
                return v
            return max(0, min(5, v))
        except (TypeError, ValueError):
            return None
    return None


def _pollen_snapshot(
    client: httpx.Client, api_key: str, lat: float, lng: float
) -> dict[str, Any | None]:
    out: dict[str, Any | None] = {
        "pollen_grass_index": None,
        "pollen_tree_index": None,
        "pollen_weed_index": None,
        "dominant_pollen_type": None,
    }
    try:
        r = client.post(
            f"{POLLEN_URL}?key={quote(api_key, safe='')}",
            headers={"Content-Type": "application/json"},
            json={"location": {"latitude": lat, "longitude": lng}, "days": 1},
            timeout=60.0,
        )
        _sleep()
        if r.status_code >= 400:
            return out
        body = r.json()
        if not isinstance(body, dict):
            return out
        daily = body.get("dailyInfo")
        if not isinstance(daily, list) or not daily:
            return out
        day0 = daily[0]
        if not isinstance(day0, dict):
            return out
        pti = day0.get("pollenTypeInfo")
        if not isinstance(pti, list):
            return out
        g = _pollen_index_from_type_info(pti, "GRASS")
        t = _pollen_index_from_type_info(pti, "TREE")
        w = _pollen_index_from_type_info(pti, "WEED")
        out["pollen_grass_index"] = g
        out["pollen_tree_index"] = t
        out["pollen_weed_index"] = w
        triple: list[tuple[str, int]] = []
        for k, v in (("GRASS", g), ("TREE", t), ("WEED", w)):
            if v is not None:
                triple.append((k, v))
        if not triple:
            return out
        triple.sort(key=lambda kv: (-kv[1], POLLEN_ORDER.get(kv[0], 99)))
        out["dominant_pollen_type"] = triple[0][0]
    except Exception:  # noqa: BLE001
        LOG.debug("pollen error: %s", traceback.format_exc())
    return out


def _air_snapshot(client: httpx.Client, api_key: str, lat: float, lng: float) -> dict[str, Any | None]:
    out: dict[str, Any | None] = {
        "air_quality_index": None,
        "air_quality_category": None,
        "dominant_pollutant": None,
    }
    try:
        r = client.post(
            f"{AIR_URL}?key={quote(api_key, safe='')}",
            headers={"Content-Type": "application/json"},
            json={"location": {"latitude": lat, "longitude": lng}},
            timeout=60.0,
        )
        _sleep()
        if r.status_code >= 400:
            return out
        body = r.json()
        if not isinstance(body, dict):
            return out
        indexes = body.get("indexes")
        if not isinstance(indexes, list):
            return out
        chosen: dict[str, Any] | None = None
        for idx in indexes:
            if isinstance(idx, dict) and str(idx.get("code", "")).lower() == "uaqi":
                chosen = idx
                break
        if chosen is None and indexes:
            first = indexes[0]
            chosen = first if isinstance(first, dict) else None
        if not chosen:
            return out
        aqi = chosen.get("aqi")
        try:
            out["air_quality_index"] = int(aqi) if aqi is not None else None
        except (TypeError, ValueError):
            out["air_quality_index"] = None
        cat = chosen.get("category")
        out["air_quality_category"] = str(cat) if cat is not None else None
        dp = chosen.get("dominantPollutant")
        out["dominant_pollutant"] = str(dp).lower() if dp else None
    except Exception:  # noqa: BLE001
        LOG.debug("air quality error: %s", traceback.format_exc())
    return out


def _count_nonempty_reviews(snap: dict[str, Any]) -> int:
    n = 0
    for i in range(1, 6):
        t = snap.get(f"review_text_{i}")
        if t and str(t).strip():
            n += 1
    return n


def _empty_places_snapshot() -> dict[str, Any]:
    d: dict[str, Any] = {"google_rating": None, "review_count": None}
    for i in range(1, 6):
        d[f"review_text_{i}"] = None
        d[f"review_author_{i}"] = None
        d[f"review_rating_{i}"] = None
        d[f"review_date_{i}"] = None
    return d


_JSON_BLOCK = re.compile(r"\{[\s\S]*\}")


def _claude_sentiment(api_key: str, review_texts: list[str]) -> dict[str, Any] | None:
    try:
        import anthropic
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("Install anthropic package for sentiment analysis.") from e

    joined = "\n---\n".join(f"Review {i + 1}: {t}" for i, t in enumerate(review_texts))
    prompt = (
        "Analyse these wedding venue reviews. Return JSON only, no other text:\n"
        '{"sentiment_score": 0.0-1.0, "top_themes": [array of max 3 strings], '
        '"red_flags": [array of strings, empty array if none], '
        '"one_line_summary": "max 20 words"}\n\n'
        f"{joined}"
    )
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    text = ""
    for block in msg.content:
        if hasattr(block, "text"):
            text += block.text
    text = text.strip()
    m = _JSON_BLOCK.search(text)
    if not m:
        return None
    try:
        data = json.loads(m.group())
    except json.JSONDecodeError:
        return None
    score = data.get("sentiment_score")
    try:
        score_f = float(score) if score is not None else None
        if score_f is not None:
            score_f = max(0.0, min(1.0, score_f))
    except (TypeError, ValueError):
        score_f = None
    themes = data.get("top_themes")
    if not isinstance(themes, list):
        themes = []
    themes = [str(x) for x in themes[:3]]
    flags = data.get("red_flags")
    if not isinstance(flags, list):
        flags = []
    flags = [str(x) for x in flags]
    summary = data.get("one_line_summary")
    summary_s = str(summary).strip() if summary is not None else ""
    return {
        "sentiment_score": score_f,
        "sentiment_themes": themes,
        "red_flags": flags,
        "one_line_summary": summary_s or None,
    }


def _load_previous_ratings(
    supabase: Any, captured_date: date, venue_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Latest snapshot per venue strictly before this month's captured_date."""
    prev: dict[str, dict[str, Any]] = {}
    if not venue_ids:
        return prev
    # Pull historical rows for deltas (bounded by reasonable row count).
    resp = (
        supabase.table("venue_ratings")
        .select("venue_id, captured_date, google_rating, review_count")
        .lt("captured_date", captured_date.isoformat())
        .execute()
    )
    rows = getattr(resp, "data", None) or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        vid = str(row.get("venue_id", ""))
        if not vid:
            continue
        cd = row.get("captured_date")
        if not cd:
            continue
        if vid not in prev:
            prev[vid] = dict(row)
            continue
        old_cd = prev[vid].get("captured_date", "")
        if str(cd) > str(old_cd):
            prev[vid] = dict(row)
    return prev


def _fetch_venues(supabase: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    page = 1000
    while True:
        resp = (
            supabase.table("venues")
            .select("id, place_id, lat, lng, name")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = getattr(resp, "data", None) or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def _filter_venues(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        pid = _place_id_raw(row)
        if not pid:
            continue
        lat, lng = _venue_coords(row)
        if lat is None or lng is None:
            continue
        filtered.append(row)
    return filtered


def _insert_venue_rating(supabase: Any, row: dict[str, Any]) -> tuple[bool, bool]:
    """Returns (inserted_ok, skipped_duplicate)."""
    try:
        supabase.table("venue_ratings").insert(row).execute()
        return True, False
    except Exception as e:  # noqa: BLE001
        err = str(e).lower()
        if "duplicate" in err or "23505" in err or "unique" in err:
            return False, True
        LOG.exception("insert venue_ratings failed: %s", e)
        return False, False


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    from data_builder.config import get_settings

    settings = get_settings()
    sb_url = (settings.supabase_url or "").strip()
    sb_key = (settings.supabase_service_role_key or settings.supabase_key or "").strip()
    g_key = (
        os.getenv("GOOGLE_PLACES_API_KEY")
        or os.getenv("GOOGLE_MAPS_API_KEY")
        or (settings.google_places_api_key or settings.google_maps_api_key or "").strip()
    ).strip()
    anth_key = (settings.anthropic_api_key or "").strip()

    if not sb_url or not sb_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) are required.")
    if not g_key:
        raise RuntimeError("Set GOOGLE_PLACES_API_KEY or GOOGLE_MAPS_API_KEY.")
    if not anth_key:
        raise RuntimeError("ANTHROPIC_API_KEY is required.")

    from supabase import create_client

    supabase = create_client(sb_url, sb_key)

    now_syd = datetime.now(SYDNEY_TZ)
    captured_date = date(now_syd.year, now_syd.month, 1)
    captured_at = datetime.now(timezone.utc).isoformat()

    raw_rows = _fetch_venues(supabase)
    venues = _filter_venues(raw_rows)
    total = len(venues)
    venue_ids = [str(v["id"]) for v in venues if v.get("id") is not None]
    prev_by_venue = _load_previous_ratings(supabase, captured_date, venue_ids)

    rated = pollen_ok = aq_ok = sentiment_ok = errors = 0
    processed = 0

    with httpx.Client() as http:
        for row in venues:
            processed += 1
            vid = row.get("id")
            if vid is None:
                errors += 1
                continue
            venue_id_str = str(vid)
            place_id = _place_id_raw(row)
            lat, lng = _venue_coords(row)
            assert lat is not None and lng is not None

            snap: dict[str, Any] = {}
            try:
                snap = _places_snapshot(http, g_key, place_id)
            except Exception:  # noqa: BLE001
                LOG.exception("places snapshot exception venue_id=%s", venue_id_str)
                errors += 1
                snap = _empty_places_snapshot()
            else:
                if snap.get("_error"):
                    errors += 1
                    LOG.warning(
                        "places error venue_id=%s: %s",
                        venue_id_str,
                        snap.get("_error"),
                    )
                    snap = _empty_places_snapshot()
                else:
                    rated += 1

            pollen = _pollen_snapshot(http, g_key, lat, lng)
            if (
                pollen.get("pollen_grass_index") is not None
                or pollen.get("pollen_tree_index") is not None
                or pollen.get("pollen_weed_index") is not None
            ):
                pollen_ok += 1

            air = _air_snapshot(http, g_key, lat, lng)
            if air.get("air_quality_index") is not None:
                aq_ok += 1

            n_reviews = _count_nonempty_reviews(snap)
            sentiment_score = None
            sentiment_themes: list[str] | None = None
            red_flags: list[str] | None = None
            one_line_summary = None

            if n_reviews >= 2:
                texts = [
                    str(snap.get(f"review_text_{i}") or "").strip()
                    for i in range(1, 6)
                    if str(snap.get(f"review_text_{i}") or "").strip()
                ]
                try:
                    cs = _claude_sentiment(anth_key, texts[:5])
                    if cs and cs.get("sentiment_score") is not None:
                        sentiment_score = cs["sentiment_score"]
                        sentiment_themes = cs.get("sentiment_themes") or []
                        red_flags = cs.get("red_flags") or []
                        one_line_summary = cs.get("one_line_summary")
                        sentiment_ok += 1
                except Exception:  # noqa: BLE001
                    LOG.warning("claude sentiment failed venue_id=%s", venue_id_str, exc_info=True)

            prev = prev_by_venue.get(venue_id_str)
            review_delta = None
            rating_delta = None
            if prev:
                prc = prev.get("review_count")
                pgr = prev.get("google_rating")
                try:
                    if snap.get("review_count") is not None and prc is not None:
                        review_delta = int(snap["review_count"]) - int(prc)
                except (TypeError, ValueError):
                    review_delta = None
                try:
                    if snap.get("google_rating") is not None and pgr is not None:
                        rating_delta = float(snap["google_rating"]) - float(pgr)
                except (TypeError, ValueError):
                    rating_delta = None

            insert_row: dict[str, Any] = {
                "venue_id": venue_id_str,
                "captured_date": captured_date.isoformat(),
                "google_rating": snap.get("google_rating"),
                "review_count": snap.get("review_count"),
                "review_delta": review_delta,
                "rating_delta": rating_delta,
                **{f"review_text_{i}": snap.get(f"review_text_{i}") for i in range(1, 6)},
                **{f"review_author_{i}": snap.get(f"review_author_{i}") for i in range(1, 6)},
                **{f"review_rating_{i}": snap.get(f"review_rating_{i}") for i in range(1, 6)},
                **{f"review_date_{i}": snap.get(f"review_date_{i}") for i in range(1, 6)},
                "pollen_grass_index": pollen.get("pollen_grass_index"),
                "pollen_tree_index": pollen.get("pollen_tree_index"),
                "pollen_weed_index": pollen.get("pollen_weed_index"),
                "dominant_pollen_type": pollen.get("dominant_pollen_type"),
                "air_quality_index": air.get("air_quality_index"),
                "air_quality_category": air.get("air_quality_category"),
                "dominant_pollutant": air.get("dominant_pollutant"),
                "sentiment_score": sentiment_score,
                "sentiment_themes": sentiment_themes,
                "red_flags": red_flags,
                "one_line_summary": one_line_summary,
                "captured_at": captured_at,
            }

            ok, dup = _insert_venue_rating(supabase, insert_row)
            if not ok and not dup:
                errors += 1
            if ok:
                # Refresh prev for same run if ever re-processing same month (normally skipped by PK).
                prev_by_venue[venue_id_str] = {
                    "venue_id": venue_id_str,
                    "captured_date": captured_date.isoformat(),
                    "google_rating": snap.get("google_rating"),
                    "review_count": snap.get("review_count"),
                }

            if processed % PROGRESS_EVERY == 0 or processed == total:
                print(
                    f"Progress: {processed}/{total} | Rated: {rated} | Pollen: {pollen_ok} | "
                    f"AQ: {aq_ok} | Sentiment: {sentiment_ok} | Errors: {errors}",
                    flush=True,
                )


if __name__ == "__main__":
    import sys

    if "--run-now" in sys.argv:
        main()
    else:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
        import pytz

        scheduler = BlockingScheduler(timezone=pytz.utc)
        scheduler.add_job(
            main,
            CronTrigger(day=1, hour=17, minute=0),
            id="monthly_venue_snapshot",
        )

        def _afcc_quarterly() -> None:
            try:
                from scrapers import afcc_profile_scraper

                afcc_profile_scraper.main()
            except Exception:  # noqa: BLE001
                LOG.exception("afcc_profile_scraper quarterly job failed")

        def _influencer_enrichment_job() -> None:
            try:
                from scrapers.influencer_enrichment import main as inf_main

                inf_main()
            except Exception:  # noqa: BLE001
                LOG.exception("influencer_enrichment monthly job failed")

        def _influencer_discovery_job() -> None:
            try:
                from scrapers.influencer_discovery import main as disc_main

                disc_main()
            except Exception:  # noqa: BLE001
                LOG.exception("influencer_discovery monthly job failed")

        scheduler.add_job(
            _afcc_quarterly,
            CronTrigger(month="1,4,7,10", day=5, hour=17, minute=0),
            id="quarterly_afcc_profile_scrape",
        )
        scheduler.add_job(
            _influencer_enrichment_job,
            CronTrigger(day=5, hour=18, minute=0),
            id="monthly_influencer_enrichment",
        )
        scheduler.add_job(
            _influencer_discovery_job,
            CronTrigger(day=6, hour=18, minute=0),
            id="monthly_influencer_discovery",
        )
        print(
            "Scheduler started (UTC): venue snapshot 1st 17:00; "
            "influencer enrichment 5th 18:00; influencer discovery 6th 18:00; "
            "AFCC profiles 5th Jan/Apr/Jul/Oct 17:00 (before influencer enrichment at 18:00). "
            "Use --run-now for an immediate venue snapshot."
        )
        scheduler.start()
