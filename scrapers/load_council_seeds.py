"""Load Queensland and Western Australian council seeds into shared.ref_councils.

Augmentation discipline: existing rows matched on (state_code, council_name) only
receive values for columns that are currently NULL - never overwriting non-null data.
Exceptions: ``is_active`` is set true and ``scraped_date`` is set to today for every
processed row per run-book.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

LOG_PATH = _ROOT / "logs" / "load_council_seeds.log"
RUNTIME_LIMIT_S = 300.0

AUGMENT_FIELDS: tuple[str, ...] = (
    "website",
    "url_pattern",
    "source_directory",
    "lga_code",
    "region",
    "contact_email",
    "postal_address",
    "phone",
)

SEED_RELATIVE_FILES = ("data/seed_councils/QLD.json", "data/seed_councils/WA.json")

_ID_SUFFIX_RE = re.compile(r"^CNCL-([A-Z]{2,3})-(\d{3})$", re.I)


@dataclass
class LoaderState:
    """In-memory snapshots to avoid duplicate round trips per council row."""

    council_ids_used: set[str] = field(default_factory=set)
    by_natural_key: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    max_suffix_by_state: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_supabase(cls, sb: Any) -> LoaderState:
        res = sb.schema("shared").table("ref_councils").select("*").execute()
        st = cls()
        for row in res.data or []:
            if not isinstance(row, dict):
                continue
            cid = str(row.get("council_id") or "").strip()
            if cid:
                st.council_ids_used.add(cid)
            sc = _norm_optional_str(row.get("state_code"))
            name = _norm_optional_str(row.get("council_name"))
            if sc and name:
                st.by_natural_key[(sc, name)] = dict(row)

            if cid:
                m = _ID_SUFFIX_RE.match(cid)
                if m:
                    state_pref = m.group(1).upper()
                    suffix = int(m.group(2))
                    cur = st.max_suffix_by_state.get(state_pref, 0)
                    st.max_suffix_by_state[state_pref] = max(cur, suffix)
        return st

    def next_council_id(self, state_code: str, log: logging.Logger) -> str:
        mx = self.max_suffix_by_state.get(state_code, 0) + 1
        cand = f"CNCL-{state_code}-{mx:03d}"
        while cand in self.council_ids_used:
            mx += 1
            cand = f"CNCL-{state_code}-{mx:03d}"
        self.max_suffix_by_state[state_code] = mx
        return cand

    def allocate_council_id(
        self, preferred: str | None, state_code: str, log: logging.Logger
    ) -> str:
        if preferred and preferred not in self.council_ids_used:
            return preferred
        if preferred:
            log.warning(
                "Seed council_id %s already present - allocating fresh id for state %s",
                preferred,
                state_code,
            )
        nid = self.next_council_id(state_code, log)
        return nid

    def register_allocated_id(self, state_code: str, council_id: str) -> None:
        self.council_ids_used.add(council_id)
        m = _ID_SUFFIX_RE.match(council_id)
        if m and m.group(1).upper() == state_code.upper():
            suf = int(m.group(2))
            cur = self.max_suffix_by_state.get(state_code, 0)
            self.max_suffix_by_state[state_code] = max(cur, suf)


def _setup_logging() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("load_council_seeds")
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
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return str(int(val)) if val.is_integer() else str(val)
    if not isinstance(val, str):
        val = str(val)
    stripped = val.strip()
    return stripped if stripped else None


def _seed_value_for_augment(rec: dict[str, Any], key: str) -> str | None:
    return _norm_optional_str(rec.get(key))


def _load_seed_records(log: logging.Logger, path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        log.error("Seed file missing: %s", path)
        raise SystemExit(1)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit(f"{path}: expected JSON array")

    councils: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if "_metadata" in item and "council_name" not in item:
            continue
        if _norm_optional_str(item.get("council_name")) is None:
            continue
        councils.append(item)
    log.info("Loaded %s council records from %s", len(councils), path.name)
    return councils


def _build_insert_payload(
    lst: LoaderState,
    rec: dict[str, Any],
    today_iso: str,
    log: logging.Logger,
) -> dict[str, Any]:
    state_code = _norm_optional_str(rec.get("state_code"))
    council_name = _norm_optional_str(rec.get("council_name"))
    if not state_code:
        raise ValueError("missing state_code")
    if not council_name:
        raise ValueError("missing council_name")

    preferred = _norm_optional_str(rec.get("council_id"))
    council_id = lst.allocate_council_id(preferred, state_code, log)
    lst.register_allocated_id(state_code, council_id)

    payload: dict[str, Any] = {
        "council_id": council_id,
        "council_name": council_name,
        "state_code": state_code,
        "aligned_destination_ids": [],
        "is_active": True,
        "scraped_date": today_iso,
    }
    for key in AUGMENT_FIELDS:
        payload[key] = _seed_value_for_augment(rec, key)
    lst.by_natural_key[(state_code, council_name)] = dict(payload)
    return payload


def _merged_update_for_existing(existing: dict[str, Any], rec: dict[str, Any], today_iso: str) -> tuple[dict[str, Any], bool]:
    patch: dict[str, Any] = {"is_active": True, "scraped_date": today_iso}
    filled_any = False
    for key in AUGMENT_FIELDS:
        seed_val = _seed_value_for_augment(rec, key)
        if seed_val is None:
            continue
        if existing.get(key) is not None:
            continue
        patch[key] = seed_val
        filled_any = True
    return patch, filled_any


def _refresh_alignment(sb: Any, log: logging.Logger) -> None:
    try:
        sb.schema("shared").rpc("refresh_ref_councils_alignment", params={}).execute()
    except Exception as e:  # noqa: BLE001
        log.warning(
            "refresh_ref_councils_alignment not applied (callable may be broken upstream): %s",
            e,
        )


def main() -> None:
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

    sb = create_client(url, key)
    today_iso = date.today().isoformat()

    lst = LoaderState.from_supabase(sb)

    records: list[dict[str, Any]] = []
    for rel in SEED_RELATIVE_FILES:
        records.extend(_load_seed_records(log, _ROOT / rel))

    inserted = 0
    updated_filled_nulls = 0
    skipped_already_complete = 0
    errors = 0

    for rec in records:
        if time.monotonic() > deadline:
            log.error(
                "Hard runtime limit (%.0fs) exceeded - stopping early",
                RUNTIME_LIMIT_S,
            )
            break
        try:
            state_code = _norm_optional_str(rec.get("state_code"))
            council_name = _norm_optional_str(rec.get("council_name"))
            if not state_code or not council_name:
                log.warning(
                    "Skipping row with missing state_code or council_name: %s",
                    rec,
                )
                continue

            nk = (state_code, council_name)
            existing = lst.by_natural_key.get(nk)

            if not existing:
                payload = _build_insert_payload(lst, rec, today_iso, log)
                sb.schema("shared").table("ref_councils").insert(payload).execute()
                inserted += 1
                log.info("%s | %s | inserted", state_code, council_name)
            else:
                existing_row = dict(existing)
                patch, filled_any = _merged_update_for_existing(
                    existing_row, rec, today_iso
                )
                council_id = existing_row.get("council_id")
                if not council_id:
                    log.error(
                        "Existing row missing council_id for %s | %s",
                        state_code,
                        council_name,
                    )
                    errors += 1
                    continue
                sb.schema("shared").table("ref_councils").update(patch).eq(
                    "council_id", council_id
                ).execute()
                merged = dict(existing_row)
                for k, v in patch.items():
                    merged[k] = v
                lst.by_natural_key[nk] = merged
                if filled_any:
                    updated_filled_nulls += 1
                    log.info("%s | %s | updated_filled_nulls", state_code, council_name)
                else:
                    skipped_already_complete += 1
                    log.info(
                        "%s | %s | skipped_already_complete",
                        state_code,
                        council_name,
                    )
        except Exception:
            errors += 1
            log.exception(
                "Record failed (state=%s name=%s)",
                rec.get("state_code"),
                rec.get("council_name"),
            )

    _refresh_alignment(sb, log)
    elapsed = time.monotonic() - t0

    log.info(
        (
            "Summary | inserted=%s | updated_filled_nulls=%s | "
            "skipped_already_complete=%s | errors=%s | runtime_s=%.2f"
        ),
        inserted,
        updated_filled_nulls,
        skipped_already_complete,
        errors,
        elapsed,
    )
    print(
        "\n--- load_council_seeds summary ---\n"
        f"inserted:                  {inserted}\n"
        f"updated_filled_nulls:      {updated_filled_nulls}\n"
        f"skipped_already_complete: {skipped_already_complete}\n"
        f"errors:                    {errors}\n"
        f"runtime_s:               {elapsed:.2f}\n"
        f"log file:                  {LOG_PATH}\n"
    )


if __name__ == "__main__":
    main()
