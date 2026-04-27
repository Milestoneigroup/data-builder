"""Load wedding influencer directory rows into ``shared.ref_influencers``.

Reads ``data/influencer_master.csv`` or ``data/influencer_master.xlsx`` (Excel files
saved with a ``.csv`` extension are detected via ZIP magic).

Upserts by **normalised URL** (fetch existing ``url`` → ``source_id``, then update in
place so ``source_id`` stays stable).

Run: ``python -m scrapers.load_influencers_supabase``

Requires: ``SUPABASE_URL``, ``SUPABASE_SERVICE_ROLE_KEY`` (or ``SUPABASE_KEY`` with
service privileges). Migration ``008_influencer_intelligence.sql`` applied.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / ".env", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / "env.local", override=True)

from scrapers.influencer_util import dedupe_key, is_xlsx_file, norm_url, root_domain, states_to_pipe

MASTER = _ROOT / "data" / "influencer_master.csv"
EXISTING_URLS_TXT = _ROOT / "data" / "influencer_existing_urls.txt"

DEFAULT_LAST_VERIFIED = "2026-04-27"
DEFAULT_DISCOVERY = "MIG_Directory_v2"
DEFAULT_CONFIDENCE = "medium"
DEFAULT_COUNTRY = "Australia"


def _clean(v: Any) -> Any:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return None
    return s


def _parse_bool(v: Any, default: bool = True) -> bool:
    s = _clean(v)
    if s is None:
        return default
    return str(s).lower() in ("1", "true", "yes", "y", "active")


def _read_master(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(str(path))
    if is_xlsx_file(path):
        return pd.read_excel(path, dtype=str, keep_default_na=False)
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace", on_bad_lines="skip")


def _row_from_series(r: pd.Series) -> dict[str, Any]:
    def col(*names: str) -> Any:
        for n in names:
            if n in r.index:
                return r.get(n)
        return None

    name = _clean(col("NAME", "Name", "name"))
    url = _clean(col("URL", "url"))
    source_type = _clean(col("SOURCE TYPE", "source_type", "Source Type"))
    states_raw = _clean(col("STATES", "states"))
    specialism = _clean(col("SPECIALISM", "specialism", "Specialism"))
    instagram_raw = _clean(col("INSTAGRAM", "instagram", "Instagram"))
    instagram = (
        instagram_raw[1:].strip()
        if isinstance(instagram_raw, str) and instagram_raw.startswith("@")
        else instagram_raw
    )
    trust = _clean(col("TRUST LEVEL", "trust_level", "Trust Level"))
    key_locations = _clean(col("KEY LOCATIONS", "key_locations", "Key Locations"))
    about_url = _clean(col("ABOUT URL", "about_url", "About URL"))
    blog_index_url = _clean(col("BLOG INDEX URL", "blog_index_url", "Blog Index URL"))
    notes = _clean(col("NOTES", "notes"))
    summary = _clean(col("SUMMARY", "summary"))
    source_id = _clean(col("SOURCE ID", "source_id", "Source ID"))
    data_conf = _clean(col("DATA CONFIDENCE", "data_confidence"))
    last_ver = _clean(col("LAST VERIFIED", "last_verified"))
    is_active = _parse_bool(col("IS ACTIVE", "is_active"), default=True)

    if trust:
        trust = str(trust).strip().lower()
        if trust not in ("high", "medium", "low"):
            trust = None
    notes_out = notes
    if not notes_out and summary:
        notes_out = summary

    rec: dict[str, Any] = {
        "name": name,
        "url": url,
        "source_type": source_type,
        "states": states_to_pipe(states_raw) if states_raw else None,
        "specialism_description": specialism,
        "instagram_handle": instagram,
        "trust_level": trust,
        "key_locations": key_locations,
        "about_url": about_url,
        "blog_index_url": blog_index_url,
        "notes": notes_out,
        "is_active": is_active,
        "country": DEFAULT_COUNTRY,
        "is_international": False,
        "data_confidence": data_conf or DEFAULT_CONFIDENCE,
        "last_verified": last_ver or DEFAULT_LAST_VERIFIED,
        "discovery_source": DEFAULT_DISCOVERY,
    }
    if source_id:
        rec["source_id"] = source_id
    return rec


NEW_SOURCES: list[dict[str, Any]] = [
    {
        "name": "Polka Dot Bride",
        "url": "https://polkadotbride.com.au",
        "source_type": "bridal_editorial",
        "states": "NSW|VIC|QLD|WA|SA",
        "trust_level": "high",
        "founder_name": "Ms Polka",
        "specialism_description": (
            "Australia's leading wedding blog with real weddings, planning advice and vendor directory"
        ),
    },
    {
        "name": "The Brides Tree",
        "url": "https://thebridestree.com.au",
        "source_type": "regional_guide",
        "states": "QLD",
        "trust_level": "high",
        "founder_name": "Sally Townsend",
        "key_locations": "Sunshine Coast",
        "specialism_description": (
            "Sunshine Coast specialist wedding guide with local vendor recommendations and real weddings"
        ),
    },
    {
        "name": "The White Files",
        "url": "https://thewhitefiles.net",
        "source_type": "bridal_editorial",
        "states": "VIC",
        "trust_level": "high",
        "founder_name": "Ella Zampatti",
        "specialism_description": (
            "Melbourne-focused modern and minimalist wedding inspiration with styled shoots"
        ),
    },
    {
        "name": "Wedded Wonderland",
        "url": "https://weddedwonderland.com",
        "source_type": "bridal_editorial",
        "states": "NSW",
        "trust_level": "high",
        "founder_name": "Nikki Pash",
        "specialism_description": (
            "Luxurious and glamorous wedding inspiration with premium vendor directory"
        ),
    },
    {
        "name": "Nouba Blog",
        "url": "https://nouba.com.au",
        "source_type": "bridal_editorial",
        "states": "QLD",
        "trust_level": "high",
        "specialism_description": (
            "Creative and original wedding inspiration focused on unique and offbeat ceremonies"
        ),
    },
    {
        "name": "Ivory Tribe",
        "url": "https://ivorytribe.com.au",
        "source_type": "bridal_editorial",
        "states": "NSW|VIC|QLD",
        "trust_level": "high",
        "specialism_description": (
            "Australian wedding blog featuring celebrity and influencer weddings plus planning inspiration"
        ),
    },
    {
        "name": "Hello May",
        "url": "https://hellomay.com.au",
        "source_type": "bridal_editorial",
        "states": "NSW|VIC|QLD|WA|SA",
        "trust_level": "high",
        "founder_name": "Sophie Lord",
        "specialism_description": (
            "Premium Australian wedding magazine and directory connecting couples with creative suppliers"
        ),
    },
    {
        "name": "The Lane",
        "url": "https://thelane.com",
        "source_type": "bridal_editorial",
        "states": "NSW|VIC|QLD",
        "trust_level": "high",
        "specialism_description": (
            "Curated editorial wedding platform featuring high-end real weddings and inspiration"
        ),
    },
    {
        "name": "WedShed",
        "url": "https://wedshed.com.au",
        "source_type": "regional_guide",
        "states": "NSW|VIC|QLD|WA|SA|TAS",
        "trust_level": "high",
        "specialism_description": (
            "Unique and alternative wedding venue discovery platform covering all Australian states"
        ),
    },
    {
        "name": "Vogue Australia Brides",
        "url": "https://vogue.com.au/brides",
        "source_type": "bridal_editorial",
        "states": "NSW|VIC",
        "trust_level": "high",
        "specialism_description": (
            "Luxury bridal editorial from Vogue Australia covering fashion, real weddings and trends"
        ),
    },
    {
        "name": "Rock and Roll Bride",
        "url": "https://rocknrollbride.com",
        "source_type": "bridal_editorial",
        "states": "NSW|VIC|QLD",
        "trust_level": "high",
        "is_international": True,
        "country": "UK",
        "founder_name": "Kat Williams",
        "specialism_description": (
            "Alternative and unconventional wedding inspiration encouraging couples to personalise their day"
        ),
    },
    {
        "name": "Green Wedding Shoes",
        "url": "https://greenweddingshoes.com",
        "source_type": "bridal_editorial",
        "states": "NSW|VIC|QLD",
        "trust_level": "high",
        "is_international": True,
        "country": "US",
        "specialism_description": (
            "Globally-read wedding inspiration blog with DIY projects and diverse real wedding stories"
        ),
    },
]


def _merge_defaults(rec: dict[str, Any]) -> dict[str, Any]:
    out = dict(rec)
    out.setdefault("country", DEFAULT_COUNTRY)
    out.setdefault("is_international", False)
    out.setdefault("is_active", True)
    out.setdefault("data_confidence", DEFAULT_CONFIDENCE)
    out.setdefault("last_verified", DEFAULT_LAST_VERIFIED)
    out.setdefault("discovery_source", DEFAULT_DISCOVERY)
    return out


def _strip_none(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def _build_payload(rec: dict[str, Any]) -> dict[str, Any]:
    rec = _merge_defaults(dict(rec))
    url = rec.get("url")
    if not url:
        raise ValueError("missing url")
    nu = norm_url(str(url))
    payload: dict[str, Any] = {
        "name": rec["name"],
        "url": nu,
        "root_domain": root_domain(nu),
        "source_type": rec.get("source_type"),
        "states": rec.get("states"),
        "primary_state": rec.get("primary_state"),
        "key_locations": rec.get("key_locations"),
        "is_international": bool(rec.get("is_international", False)),
        "country": rec.get("country") or DEFAULT_COUNTRY,
        "specialism_description": rec.get("specialism_description"),
        "founder_name": rec.get("founder_name"),
        "founder_gender": rec.get("founder_gender"),
        "instagram_handle": rec.get("instagram_handle"),
        "trust_level": rec.get("trust_level"),
        "about_url": rec.get("about_url"),
        "blog_index_url": rec.get("blog_index_url"),
        "notes": rec.get("notes"),
        "is_active": bool(rec.get("is_active", True)),
        "data_confidence": rec.get("data_confidence"),
        "last_verified": rec.get("last_verified"),
        "discovery_source": rec.get("discovery_source"),
    }
    if rec.get("source_id"):
        payload["source_id"] = rec["source_id"]
    return _strip_none(payload)


def fetch_url_to_source_id(client: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    offset = 0
    page = 1000
    while True:
        resp = (
            client.schema("shared")
            .table("ref_influencers")
            .select("source_id,url")
            .range(offset, offset + page - 1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if not rows:
            break
        for row in rows:
            if not isinstance(row, dict):
                continue
            u = row.get("url")
            sid = row.get("source_id")
            if u and sid:
                k = dedupe_key(str(u))
                if k:
                    out[k] = str(sid)
        if len(rows) < page:
            break
        offset += page
    return out


def upsert_single_influencer(client: Any, rec: dict[str, Any], url_map: dict[str, str] | None = None) -> tuple[Literal["insert", "update"], str]:
    """Insert or update one row by normalised URL. Optionally mutates ``url_map`` in place."""
    payload = _build_payload(rec)
    nu = dedupe_key(payload["url"])
    local_map = url_map if url_map is not None else fetch_url_to_source_id(client)
    existing_sid = local_map.get(nu)
    if existing_sid:
        upd = {k: v for k, v in payload.items() if k != "source_id"}
        upd = _strip_none(upd)
        client.schema("shared").table("ref_influencers").update(upd).eq("source_id", existing_sid).execute()
        return "update", existing_sid
    ins = _strip_none(payload)
    client.schema("shared").table("ref_influencers").insert(ins).execute()
    sel = (
        client.schema("shared")
        .table("ref_influencers")
        .select("source_id")
        .eq("url", ins["url"])
        .limit(1)
        .execute()
    )
    rows = getattr(sel, "data", None) or []
    sid = str(rows[0]["source_id"]) if rows and isinstance(rows[0], dict) and rows[0].get("source_id") else ""
    if sid and url_map is not None:
        url_map[nu] = sid
    return "insert", sid


def influencer_rec_from_discovery_row(row: dict[str, str], *, discovery_source: str) -> dict[str, Any]:
    """Map discovery CSV-shaped dict (Name, URL, …) to a loader record for :func:`upsert_single_influencer`."""
    kl_raw = (row.get("key_locations") or "").strip()
    kl_pipe = kl_raw.replace("; ", "|").replace(",", "|") if kl_raw else None
    ig = (row.get("instagram") or "").strip()
    if ig.startswith("@"):
        ig = ig[1:]
    trust = (row.get("trust_level") or "medium").strip().lower()
    if trust not in ("high", "medium", "low"):
        trust = "medium"
    rec: dict[str, Any] = {
        "name": row.get("Name") or row.get("name"),
        "url": row.get("URL") or row.get("url"),
        "source_type": row.get("source_type"),
        "states": states_to_pipe(row.get("states") or ""),
        "specialism_description": row.get("specialism"),
        "instagram_handle": ig or None,
        "trust_level": trust,
        "key_locations": kl_pipe,
        "discovery_source": discovery_source,
        "data_confidence": "low",
        "last_verified": date.today().isoformat(),
        "is_active": True,
        "country": "Australia",
        "is_international": False,
    }
    return _merge_defaults(rec)


def write_influencer_urls_txt(client: Any, path: Path) -> int:
    """Write one normalised URL per line from Supabase (bootstrap for discovery)."""
    keys: list[str] = []
    offset = 0
    page = 2000
    while True:
        resp = (
            client.schema("shared")
            .table("ref_influencers")
            .select("url")
            .range(offset, offset + page - 1)
            .execute()
        )
        rows = getattr(resp, "data", None) or []
        if not rows:
            break
        for row in rows:
            if isinstance(row, dict) and row.get("url"):
                k = dedupe_key(str(row["url"]))
                if k:
                    keys.append(k)
        if len(rows) < page:
            break
        offset += page
    uniq = sorted(set(keys))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(uniq) + ("\n" if uniq else ""), encoding="utf-8")
    return len(uniq)


def main() -> int:
    from data_builder.config import get_settings
    from supabase import create_client

    s = get_settings()
    url = (s.supabase_url or "").strip()
    key = (s.supabase_service_role_key or s.supabase_key or "").strip()
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required", file=sys.stderr)
        return 1
    if not MASTER.is_file():
        print(f"ERROR: {MASTER} not found", file=sys.stderr)
        return 1

    df = _read_master(MASTER)
    df.columns = [str(c).strip() for c in df.columns]

    incoming: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        incoming.append(_row_from_series(r))
    for extra in NEW_SOURCES:
        incoming.append(_merge_defaults(dict(extra)))

    seen_in_file: set[str] = set()
    deduped: list[dict[str, Any]] = []
    skipped_in_file = 0
    for rec in incoming:
        nu = dedupe_key(str(rec.get("url") or ""))
        if not rec.get("name") or not nu:
            skipped_in_file += 1
            continue
        if nu in seen_in_file:
            skipped_in_file += 1
            continue
        seen_in_file.add(nu)
        deduped.append(rec)

    client = create_client(url, key)
    try:
        client.schema("shared").table("ref_influencers").select("source_id", count="exact").limit(1).execute()
    except Exception as e:  # noqa: BLE001
        print(
            "ERROR: shared.ref_influencers not reachable. Apply migration 008_influencer_intelligence.sql.\n"
            f"{e!r}",
            file=sys.stderr,
        )
        return 1

    url_map = fetch_url_to_source_id(client)
    loaded = 0
    updated = 0
    inserted = 0
    errors = 0
    for rec in deduped:
        try:
            kind, _ = upsert_single_influencer(client, rec, url_map)
            loaded += 1
            if kind == "update":
                updated += 1
            else:
                inserted += 1
        except Exception as e:  # noqa: BLE001
            errors += 1
            print(f"ERROR row {rec.get('name')!r} {rec.get('url')!r}: {e!r}"[:500], file=sys.stderr)

    n_txt = 0
    try:
        n_txt = write_influencer_urls_txt(client, EXISTING_URLS_TXT)
    except Exception as e:  # noqa: BLE001
        print(f"WARN: could not write {EXISTING_URLS_TXT}: {e!r}", file=sys.stderr)

    print(
        f"Summary: loaded {loaded} (inserted {inserted}, updated {updated}), "
        f"skipped {skipped_in_file} in-file/invalid duplicates, errors {errors}. "
        f"Wrote {n_txt} URLs to {EXISTING_URLS_TXT}."
    )
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
