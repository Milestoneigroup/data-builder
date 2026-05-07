"""Vendor table metadata, directory category routing, match/insert, and augmentation."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from ._framework import (
    FUZZY_MATCH_MIN,
    norm_name,
    normalise_au_state,
    token_sort_ratio,
)

DirectoryKey = Literal["easy_weddings", "hello_may"]

# Easy Weddings ``{TypePath}/`` segment (pagination: ``/{TypePath}/{n}/`` for n > 1 — see celebrant scraper).
EW_VENDOR_TYPE_TO_PATH: dict[str, str] = {
    "venues": "WeddingVenues",
    "celebrants": "MarriageCelebrant",
    "photographers": "WeddingPhotography",
}

DIRECTORY_CATEGORY_MAP: dict[str, dict[str, str]] = {
    "easy_weddings": {
        "WeddingVenues": "venues",
        "MarriageCelebrant": "celebrants",
        "WeddingPhotography": "photographers",
    },
    "hello_may": {
        "venues": "venues",
        "celebrant": "celebrants",
        "photographers": "photographers",
        "cinematographers": "photographers",
        "destination-wedding": "venues",
        "luxe-stays": "venues",
    },
}

DIR_CODE_FOR_DIRECTORY: dict[DirectoryKey, str] = {
    "easy_weddings": "EWDIR",
    "hello_may": "HMDIR",
}

SOURCE_SUFFIX = "2026_05"
ACTIVE_SIGNAL_SUFFIX = "listing_2026_05"


@dataclass(frozen=True)
class VendorTable:
    table_name: str
    natural_key_prefix: str
    name_columns: tuple[str, ...]
    pk_column: str
    insert_name_field: str
    source_field: str


VENDOR_TABLES: dict[str, VendorTable] = {
    "venues": VendorTable(
        table_name="venues",
        natural_key_prefix="VEN",
        name_columns=("name",),
        pk_column="id",
        insert_name_field="name",
        source_field="data_source",
    ),
    "celebrants": VendorTable(
        table_name="celebrants",
        natural_key_prefix="CEL",
        name_columns=("name", "full_name", "ag_display_name"),
        pk_column="celebrant_id",
        insert_name_field="full_name",
        source_field="data_source",
    ),
    "photographers": VendorTable(
        table_name="photographers",
        natural_key_prefix="PHO",
        name_columns=("business_name",),
        pk_column="photographer_id",
        insert_name_field="business_name",
        source_field="data_source_primary",
    ),
}


def vendor_table_from_ew_segment(segment: str) -> VendorTable | None:
    key = DIRECTORY_CATEGORY_MAP["easy_weddings"].get(segment)
    if not key:
        return None
    return VENDOR_TABLES.get(key)


def vendor_table_from_hm_category(category_slug: str) -> VendorTable | None:
    key = DIRECTORY_CATEGORY_MAP["hello_may"].get(category_slug.strip().lower())
    if not key:
        return None
    return VENDOR_TABLES.get(key)


def primary_source_value(directory: DirectoryKey) -> str:
    return f"{directory}_{SOURCE_SUFFIX}"


def active_signal_value(directory: DirectoryKey) -> str:
    return f"{directory}_{ACTIVE_SIGNAL_SUFFIX}"


def is_nullish(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def best_name_match_score(name: str, row: dict[str, Any], vt: VendorTable) -> int:
    best = 0
    nn = norm_name(name)
    for col in vt.name_columns:
        v = row.get(col)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        best = max(best, token_sort_ratio(nn, norm_name(s)))
    return best


class VendorRouter:
    """Exact + fuzzy matching, NULL-only augmentation, append-only inserts."""

    def __init__(
        self,
        sb: Any,
        *,
        dry_run: bool,
        logger: logging.Logger,
    ) -> None:
        self.sb = sb
        self.dry_run = dry_run
        self.log = logger
        self._row_cache: dict[tuple[str, str | None], list[dict[str, Any]]] = {}
        self._synth_seq_fallback: dict[tuple[str, str], int] = {}

    def invalidate_cache_for(self, table_name: str) -> None:
        keys = [k for k in self._row_cache if k[0] == table_name]
        for k in keys:
            del self._row_cache[k]

    def _load_rows(self, vt: VendorTable, state: str | None) -> list[dict[str, Any]]:
        key = (vt.table_name, state)
        if key in self._row_cache:
            return self._row_cache[key]

        rows: list[dict[str, Any]] = []
        page = 0
        page_size = 1000
        while True:
            q = self.sb.table(vt.table_name).select("*")
            if state:
                q = q.eq("state", state)
            start = page * page_size
            end = start + page_size - 1
            res = q.range(start, end).execute()
            batch = list(res.data or [])
            rows.extend(batch)
            if len(batch) < page_size:
                break
            page += 1
            if page > 500:
                self.log.warning(
                    "Stopping %s prefetch at %s rows for state=%r (safety cap).",
                    vt.table_name,
                    len(rows),
                    state,
                )
                break

        self._row_cache[key] = rows
        return rows

    def next_source_directory_synthetic_id(
        self,
        vt: VendorTable,
        directory: DirectoryKey,
    ) -> str:
        dir_code = DIR_CODE_FOR_DIRECTORY[directory]
        pattern = f"{vt.natural_key_prefix}-{dir_code}-%"
        try:
            res = (
                self.sb.table(vt.table_name)
                .select("source_directory_synthetic_id")
                .not_.is_("source_directory_synthetic_id", "null")
                .like("source_directory_synthetic_id", pattern)
                .order("source_directory_synthetic_id", desc=True)
                .limit(1)
                .execute()
            )
            data = res.data or []
        except Exception as exc:  # noqa: BLE001
            self.log.warning(
                "Could not read source_directory_synthetic_id (migration applied?): %s",
                exc,
            )
            return self._next_synth_fallback(vt, dir_code)

        seq = 1
        if data and data[0].get("source_directory_synthetic_id"):
            last = str(data[0]["source_directory_synthetic_id"])
            m = re.match(
                rf"^{re.escape(vt.natural_key_prefix)}-{re.escape(dir_code)}-(\d+)$",
                last,
                re.I,
            )
            if m:
                seq = int(m.group(1)) + 1

        return f"{vt.natural_key_prefix}-{dir_code}-{seq:06d}"

    def _next_synth_fallback(self, vt: VendorTable, dir_code: str) -> str:
        key = (vt.table_name, dir_code)
        self._synth_seq_fallback[key] = self._synth_seq_fallback.get(key, 0) + 1
        n = self._synth_seq_fallback[key]
        return f"{vt.natural_key_prefix}-{dir_code}-{n:06d}"

    def find_match_row(
        self,
        vt: VendorTable,
        *,
        name: str,
        state: str | None,
    ) -> tuple[dict[str, Any] | None, Literal["exact", "fuzzy"] | None]:
        if not name or not norm_name(name):
            return None, None
        candidates = self._load_rows(vt, state)

        nt = norm_name(name)
        for row in candidates:
            rs = row.get("state")
            if state and rs and str(rs).strip().upper() != str(state).strip().upper():
                continue
            for col in vt.name_columns:
                cell = row.get(col)
                if cell is None:
                    continue
                s = str(cell).strip()
                if not s:
                    continue
                if norm_name(s) == nt:
                    return row, "exact"

        best_row: dict[str, Any] | None = None
        best_score = -1
        for row in candidates:
            rs = row.get("state")
            if state and rs is not None:
                if str(rs).strip().upper() != str(state).strip().upper():
                    continue
            if not state and rs not in (None, ""):
                continue
            sc = best_name_match_score(name, row, vt)
            if sc > best_score:
                best_score = sc
                best_row = row

        if best_row is not None and best_score >= FUZZY_MATCH_MIN:
            self.log.info(
                "Fuzzy match (%s >= %s): %r -> pk=%s",
                best_score,
                FUZZY_MATCH_MIN,
                name,
                best_row.get(vt.pk_column),
            )
            return best_row, "fuzzy"
        return None, None

    def merge_null_only(
        self,
        existing: dict[str, Any],
        patch: dict[str, Any],
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, v in patch.items():
            if v is None:
                continue
            cur = existing.get(k)
            if is_nullish(cur):
                out[k] = v
        return out

    def hello_may_social_patch(
        self,
        vt: VendorTable,
        *,
        existing: dict[str, Any],
        website: str | None,
        instagram: str | None,
        facebook: str | None,
        tiktok: str | None,
        pinterest: str | None,
    ) -> dict[str, Any]:
        patch: dict[str, Any] = {}

        def add(col: str, val: str | None) -> None:
            if not val:
                return
            if is_nullish(existing.get(col)):
                patch[col] = val

        add("website", website)

        if vt.table_name == "celebrants":
            add("instagram_handle_or_url", instagram)
        elif vt.table_name == "venues":
            add("instagram_url", instagram)
            if instagram and is_nullish(existing.get("instagram_handle")):
                tail = instagram.rstrip("/").split("/")[-1]
                if tail:
                    patch["instagram_handle"] = tail[:200]
        else:
            add("instagram_url", instagram)
            if instagram and is_nullish(existing.get("instagram_handle")):
                tail = instagram.rstrip("/").split("/")[-1]
                if tail:
                    patch["instagram_handle"] = tail[:200]

        add("facebook_url", facebook)
        add("tiktok_url", tiktok)
        add("pinterest_url", pinterest)
        return patch

    def insert_new_vendor(
        self,
        vt: VendorTable,
        directory: DirectoryKey,
        *,
        name: str,
        state: str | None,
        suburb: str | None,
        postcode: str | None,
        directory_patch: dict[str, Any],
    ) -> dict[str, Any]:
        synth = self.next_source_directory_synthetic_id(vt, directory)
        payload: dict[str, Any] = {
            vt.insert_name_field: name,
            vt.source_field: primary_source_value(directory),
            "is_active_market": True,
            "active_signal_sources": active_signal_value(directory),
            "source_directory_synthetic_id": synth,
        }
        if state is not None:
            payload["state"] = state
        if suburb is not None:
            payload["suburb"] = suburb
        if postcode is not None:
            payload["postcode"] = postcode

        if vt.table_name == "celebrants":
            payload["celebrant_id"] = synth
            payload["full_name"] = name
            payload["ag_display_name"] = name
            payload["name"] = name

        elif vt.table_name == "photographers":
            payload["photographer_id"] = synth

        payload.update(directory_patch)

        if self.dry_run:
            self.log.info("DRY RUN insert %s: %s", vt.table_name, payload)
            return {vt.pk_column: None, **payload}

        last_err: BaseException | None = None
        for attempt in range(3):
            try:
                res = self.sb.table(vt.table_name).insert(payload).execute()
                rows = res.data or []
                if not rows:
                    raise RuntimeError("Insert returned no data")
                self.invalidate_cache_for(vt.table_name)
                return dict(rows[0])
            except BaseException as exc:  # noqa: BLE001
                last_err = exc
                err_s = str(exc).lower()
                if "duplicate" not in err_s and "23505" not in err_s:
                    break
                synth = self.next_source_directory_synthetic_id(vt, directory)
                payload["source_directory_synthetic_id"] = synth
                if vt.table_name == "celebrants":
                    payload["celebrant_id"] = synth
                if vt.table_name == "photographers":
                    payload["photographer_id"] = synth
        if last_err:
            raise last_err
        raise RuntimeError("insert_new_vendor failed")

    def upsert_augment(self, vt: VendorTable, existing: dict[str, Any], patch: dict[str, Any]) -> None:
        pk = existing.get(vt.pk_column)
        if pk is None:
            return
        merged = self.merge_null_only(existing, patch)
        if not merged:
            self.log.debug("No NULL fields to augment on %s pk=%s", vt.table_name, pk)
            return
        merged["updated_at"] = datetime.now(timezone.utc).isoformat()
        if self.dry_run:
            self.log.info("DRY RUN update %s pk=%s: %s", vt.table_name, pk, merged)
            return
        self.sb.table(vt.table_name).update(merged).eq(vt.pk_column, pk).execute()
        self.invalidate_cache_for(vt.table_name)


def match_or_insert(
    router: VendorRouter,
    vt: VendorTable,
    directory: DirectoryKey,
    vendor_record: dict[str, Any],
    *,
    directory_patch: dict[str, Any],
) -> tuple[Literal["exact", "fuzzy", "inserted"], dict[str, Any]]:
    """Match existing row (NULL-only augment) or insert a synthetic directory row."""
    name = str(vendor_record.get("name") or "").strip()
    state = vendor_record.get("state")
    st = normalise_au_state(state)
    suburb = vendor_record.get("suburb")
    pc = vendor_record.get("postcode")

    matched, how = router.find_match_row(vt, name=name, state=st)
    if matched is not None and how:
        merged_patch = router.merge_null_only(matched, directory_patch)
        router.upsert_augment(vt, matched, merged_patch)
        return how, matched

    row = router.insert_new_vendor(
        vt,
        directory,
        name=name,
        state=st,
        suburb=str(suburb).strip() if suburb not in (None, "") else None,
        postcode=str(pc).strip() if pc not in (None, "") else None,
        directory_patch=directory_patch,
    )
    return "inserted", row
