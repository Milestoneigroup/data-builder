"""Load wedding photographer v0.3 seed CSV into ``public.photographers``.

Natural key: ``(business_name, state)`` with augmentation-only updates (NULL cells
filled from CSV; non-NULL values are never overwritten).

Runs from repo root with ``env.local`` supplying ``SUPABASE_URL`` and
``SUPABASE_SERVICE_ROLE_KEY``. No external APIs.

Run: ``python -m scrapers.load_photographers_from_csv``
"""

from __future__ import annotations

import csv
import logging
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

LOG_PATH = _ROOT / "logs" / "load_photographers_from_csv.log"
RUNTIME_LIMIT_S = 300.0
CSV_RELATIVE = Path("data/seeds/wedding_photographers_au_v0_3.csv")

AU_STATES = frozenset({"NSW", "QLD", "VIC", "WA", "SA", "TAS", "ACT", "NT"})

# Columns we may backfill on existing rows (never overwrite non-NULL).
AUGMENT_FIELDS: tuple[str, ...] = (
    "region",
    "suburb",
    "website",
    "instagram_url",
    "instagram_handle",
    "tier",
    "data_quality_score",
    "data_source_primary",
    "data_source_secondary",
    "notes",
    "abia_winner",
    "abia_awards_text",
    "travels_nationally",
    "service_area_notes",
)

_TRAVELS_NAT_RE = re.compile(r"travels\s+nationally", re.IGNORECASE)
_ID_TAIL_RE = re.compile(r"^PHO-[A-Z]{3}-(\d{6})$", re.I)


@dataclass
class PhotoLoaderState:
    by_natural_key: dict[tuple[str, str | None], dict[str, Any]] = field(
        default_factory=dict
    )
    photographer_ids_used: set[str] = field(default_factory=set)
    max_seq_by_prefix: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_supabase(cls, sb: Any) -> PhotoLoaderState:
        st = cls()
        page_size = 1000
        offset = 0
        while True:
            res = (
                sb.table("photographers")
                .select(
                    "photographer_id,business_name,state,region,suburb,website,"
                    "instagram_url,instagram_handle,tier,data_quality_score,"
                    "data_source_primary,data_source_secondary,notes,abia_winner,"
                    "abia_awards_text,travels_nationally,service_area_notes"
                )
                .range(offset, offset + page_size - 1)
                .execute()
            )
            rows = res.data or []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                pid = str(row.get("photographer_id") or "").strip()
                if pid:
                    st.photographer_ids_used.add(pid)
                    m = _ID_TAIL_RE.match(pid)
                    if m:
                        prefix = pid.split("-")[1].upper()
                        seq = int(m.group(1))
                        st.max_seq_by_prefix[prefix] = max(
                            st.max_seq_by_prefix.get(prefix, 0), seq
                        )
                bn = _norm_business_name(row.get("business_name"))
                st_k = _norm_state_key(row.get("state"))
                if bn:
                    st.by_natural_key[(bn, st_k)] = dict(row)
            if len(rows) < page_size:
                break
            offset += page_size
        return st

    def next_id(self, prefix: str) -> str:
        mx = self.max_seq_by_prefix.get(prefix, 0) + 1
        cand = f"PHO-{prefix}-{mx:06d}"
        while cand in self.photographer_ids_used:
            mx += 1
            cand = f"PHO-{prefix}-{mx:06d}"
        self.max_seq_by_prefix[prefix] = mx
        return cand

    def register_id(self, prefix: str, photographer_id: str) -> None:
        self.photographer_ids_used.add(photographer_id)
        m = _ID_TAIL_RE.match(photographer_id)
        if m and photographer_id.upper().startswith(f"PHO-{prefix.upper()}-"):
            seq = int(m.group(1))
            self.max_seq_by_prefix[prefix] = max(
                self.max_seq_by_prefix.get(prefix, 0), seq
            )


def _setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("load_photographers_from_csv")
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


def _norm_optional_str(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if isinstance(val, float) and val.is_integer():
            val = int(val)
        val = str(val)
    if not isinstance(val, str):
        val = str(val)
    stripped = val.strip()
    return stripped if stripped else None


def _norm_business_name(val: Any) -> str | None:
    v = _norm_optional_str(val)
    return v


def _norm_state_raw(val: Any) -> str:
    return (val or "").strip() if isinstance(val, str) else ""


def _norm_state_key(state_db: str | None) -> str | None:
    if state_db is None:
        return None
    s = state_db.strip().upper()
    return s if s else None


def _id_prefix_and_db_state(state_raw: str) -> tuple[str, str | None]:
    s = state_raw.strip().upper()
    if not s:
        return "NAT", None
    if s in {"NATIONAL", "NAT"}:
        return "NAT", None
    if s in AU_STATES:
        return s, s
    return "NAT", None


def _instagram_handle_from_url(url: str | None) -> str | None:
    u = _norm_optional_str(url)
    if not u:
        return None
    try:
        p = urlparse(u if "://" in u else f"https://{u}")
        path = (p.path or "").strip("/")
        parts = [x for x in path.split("/") if x]
        if not parts:
            return None
        h = parts[0]
        if h.lower() in ("p", "reel", "reels", "stories", "explore", "s"):
            h = parts[1] if len(parts) > 1 else ""
        h = h.lstrip("@")
        return h if h else None
    except Exception:  # noqa: BLE001
        return None


def _bool_abia_winner(raw: Any) -> bool:
    s = _norm_optional_str(raw)
    if not s:
        return False
    return s.lower() in {"true", "1", "yes"}


def _parse_quality_score(raw: Any) -> int | None:
    s = _norm_optional_str(raw)
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _tier_norm(raw: Any) -> str | None:
    s = _norm_optional_str(raw)
    if not s:
        return None
    u = s.upper()
    if u in {"A", "B"}:
        return u
    return None


def _csv_row_to_payload(row: dict[str, str]) -> tuple[dict[str, Any], str] | None:
    """Build insert/update payload fields from one CSV row (excluding photographer_id)."""
    name = _norm_business_name(
        row.get("name") or row.get("business_name"),
    )
    if not name:
        return None

    state_raw = _norm_state_raw(row.get("state"))
    id_prefix, state_db = _id_prefix_and_db_state(state_raw)

    region = _norm_optional_str(row.get("region")) or _norm_optional_str(
        row.get("location_notes")
    )
    if not region:
        region = _norm_optional_str(row.get("service_regions"))

    suburb = _norm_optional_str(row.get("suburb")) or _norm_optional_str(
        row.get("city")
    )

    website = _norm_optional_str(row.get("website") or row.get("website_url"))

    ig_url = _norm_optional_str(row.get("instagram_url"))
    ig_from_col = _norm_optional_str(row.get("instagram_handle"))
    ig_handle = _instagram_handle_from_url(ig_url) if ig_url else None
    if not ig_handle:
        ig_handle = ig_from_col
        if ig_handle:
            ig_handle = ig_handle.lstrip("@")

    notes = _norm_optional_str(row.get("notes"))

    travels = False
    if state_raw.strip().lower() == "national":
        travels = True
    if notes and _TRAVELS_NAT_RE.search(notes):
        travels = True

    svc_notes = _norm_optional_str(row.get("service_area")) or _norm_optional_str(
        row.get("geographical_notes")
    )
    if not svc_notes:
        svc_notes = _norm_optional_str(row.get("service_regions"))

    abia_text = _norm_optional_str(
        row.get("abia_awards_text") or row.get("abia_award")
    )
    abia_win = _bool_abia_winner(row.get("abia_winner"))

    payload: dict[str, Any] = {
        "business_name": name,
        "state": state_db,
        "region": region,
        "suburb": suburb,
        "website": website,
        "instagram_url": ig_url,
        "instagram_handle": ig_handle,
        "tier": _tier_norm(row.get("tier")),
        "data_quality_score": _parse_quality_score(row.get("data_quality_score")),
        "data_source_primary": _norm_optional_str(
            row.get("data_source") or row.get("data_source_primary")
        ),
        "data_source_secondary": _norm_optional_str(
            row.get("data_source_secondary")
        ),
        "notes": notes,
        "abia_winner": abia_win,
        "abia_awards_text": abia_text,
        "travels_nationally": travels,
        "service_area_notes": svc_notes,
    }
    return payload, id_prefix


def _natural_key_from_payload(payload: dict[str, Any]) -> tuple[str, str | None]:
    return (
        str(payload["business_name"]),
        _norm_state_key(payload.get("state")),
    )


def _merge_augmentation(
    existing: dict[str, Any], incoming: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    patch: dict[str, Any] = {}
    filled = False
    for key in AUGMENT_FIELDS:
        if key not in incoming:
            continue
        seed_val = incoming[key]
        if seed_val is None:
            continue
        cur = existing.get(key)
        if cur is not None:
            continue
        patch[key] = seed_val
        filled = True
    return patch, filled


def _assign_deterministic_ids(
    parsed_rows: list[tuple[dict[str, Any], str]],
) -> dict[tuple[str, str | None], str]:
    """Stable PHO-{STATE|NAT}-{nnnnnn} from sorted order within id prefix."""
    by_prefix: dict[str, list[tuple[str, str | None]]] = {}
    for payload, id_prefix in parsed_rows:
        nk = _natural_key_from_payload(payload)
        by_prefix.setdefault(id_prefix, []).append(nk)

    for prefix in by_prefix:
        by_prefix[prefix].sort(key=lambda x: (x[0].lower(), x[1] or ""))

    out: dict[tuple[str, str | None], str] = {}
    for prefix, keys in by_prefix.items():
        for i, nk in enumerate(keys, start=1):
            out[nk] = f"PHO-{prefix}-{i:06d}"
    return out


def main() -> None:
    import os

    os.chdir(_ROOT)
    load_dotenv("env.local", override=True)

    from data_builder.config import get_settings
    from supabase import create_client

    log = _setup_logging()
    t0 = time.monotonic()
    deadline = t0 + RUNTIME_LIMIT_S

    settings = get_settings()
    url = (settings.supabase_url or "").strip()
    key = (settings.supabase_service_role_key or "").strip()
    if not url or not key:
        log.error(
            "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing after loading env.local",
        )
        raise SystemExit(1)

    csv_path = _ROOT / CSV_RELATIVE
    if not csv_path.is_file():
        log.error("CSV missing: %s", csv_path)
        raise SystemExit(1)

    sb = create_client(url, key)
    st = PhotoLoaderState.from_supabase(sb)

    with csv_path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        csv_rows = list(reader)

    if len(csv_rows) != 286:
        log.warning(
            "Expected 286 data rows, found %s (continuing)",
            len(csv_rows),
        )

    parsed_rows: list[tuple[dict[str, Any], str]] = []
    for row in csv_rows:
        built = _csv_row_to_payload(row)
        if built is None:
            continue
        payload, id_prefix = built
        parsed_rows.append((payload, id_prefix))

    id_by_natural_key = _assign_deterministic_ids(parsed_rows)

    inserted = 0
    updated_filled_nulls = 0
    skipped = 0
    errors = 0

    for payload, id_prefix in parsed_rows:
        if time.monotonic() > deadline:
            log.error(
                "Hard runtime limit (%.0fs) exceeded – stopping early",
                RUNTIME_LIMIT_S,
            )
            break
        try:
            nk = _natural_key_from_payload(payload)
            existing = st.by_natural_key.get(nk)

            if not existing:
                pid = id_by_natural_key.get(nk) or st.next_id(id_prefix)
                if pid in st.photographer_ids_used:
                    pid = st.next_id(id_prefix)
                insert_body = {"photographer_id": pid, **payload}
                sb.table("photographers").insert(insert_body).execute()
                st.register_id(id_prefix, pid)
                st.by_natural_key[nk] = {**insert_body}
                inserted += 1
                log.info(
                    "%s | %s | %s | inserted",
                    pid,
                    payload["business_name"],
                    nk[1] or "",
                )
            else:
                pid = str(existing.get("photographer_id") or "").strip()
                patch, filled_any = _merge_augmentation(existing, payload)
                if patch:
                    sb.table("photographers").update(patch).eq(
                        "photographer_id", pid
                    ).execute()
                    merged = dict(existing)
                    for k, v in patch.items():
                        merged[k] = v
                    st.by_natural_key[nk] = merged
                if filled_any:
                    updated_filled_nulls += 1
                    log.info(
                        "%s | %s | %s | updated_filled_nulls",
                        pid,
                        payload["business_name"],
                        nk[1] or "",
                    )
                else:
                    skipped += 1
                    log.info(
                        "%s | %s | %s | skipped",
                        pid,
                        payload["business_name"],
                        nk[1] or "",
                    )
        except Exception:
            errors += 1
            log.exception(
                "Row failed (business_name=%s)",
                payload.get("business_name"),
            )

    elapsed = time.monotonic() - t0
    log.info(
        "Summary | inserted=%s | updated_filled_nulls=%s | skipped=%s | errors=%s | "
        "runtime_s=%.2f",
        inserted,
        updated_filled_nulls,
        skipped,
        errors,
        elapsed,
    )
    print(
        "\n--- load_photographers_from_csv summary ---\n"
        f"inserted:             {inserted}\n"
        f"updated_filled_nulls: {updated_filled_nulls}\n"
        f"skipped:              {skipped}\n"
        f"errors:               {errors}\n"
        f"runtime_s:            {elapsed:.2f}\n"
        f"log file:             {LOG_PATH}\n"
    )


if __name__ == "__main__":
    main()
