"""Bulk-import chain groups and venues from CSV v3 (no Google Places calls).

Loads ``data/chain_seeds/_csv_input/chain_groups_v3.csv`` and
``chain_venues_v3.csv``, upserts ``public.venue_groups``, matches or inserts
``public.venues``, and creates ``public.venue_group_membership`` rows.

``public.venues`` rows touched by this job use::

    data_source = 'chain_seed_csv_v3'

(Schema uses ``data_source``, not ``source``. Membership ``confidence`` /
``matched_via`` values must satisfy existing CHECK constraints — human-readable
tiers are echoed in ``notes``.)

Run::

    cd C:\\...\\data-builder
    python -m scrapers.run_bulk_chain_mapper
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scrapers.map_venues_to_groups import (  # noqa: E402
    fetch_all_venues,
    find_venue_match,
    load_env,
)
from scrapers._utils.csv_chain_loader import (  # noqa: E402
    iter_chain_seeds_from_csv,
    load_groups_by_slug,
    load_venues_grouped,
)

RUNTIME_LIMIT_S = 60 * 60
MAX_CONSECUTIVE_INSERT_FAILS = 5
MAX_INSERTS_PER_GROUP = 100

CHAIN_DATA_SOURCE = "chain_seed_csv_v3"

LOG = logging.getLogger("bulk_chain_mapper")

# Membership columns: CHECK (confidence in confirmed, likely, unconfirmed)
# and matched_via in exact_name_match, fuzzy_name_suburb, manual, newly_added


def _confidence_for_match(label: str) -> str:
    if label == "exact":
        return "confirmed"
    if label == "strong":
        return "likely"
    if label == "fuzzy":
        return "unconfirmed"
    raise ValueError(f"Unexpected match label: {label}")


def _matched_via_for_match(label: str) -> str:
    if label == "exact":
        return "exact_name_match"
    if label in ("strong", "fuzzy"):
        return "fuzzy_name_suburb"
    raise ValueError(f"Unexpected match label: {label}")


def _setup_file_log() -> Path:
    logs_dir = _ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"bulk_mapper_{stamp}.log"
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s"))
    root = logging.getLogger()
    root.addHandler(fh)
    root.setLevel(logging.INFO)
    return log_path


def _fetch_existing_group(sb: Any, group_slug: str) -> Optional[dict[str, Any]]:
    r = (
        sb.table("venue_groups")
        .select("*")
        .eq("group_slug", group_slug)
        .limit(1)
        .execute()
    )
    rows = r.data or []
    return rows[0] if rows else None


def upsert_group_fill_nulls_only(sb: Any, payload: dict[str, Any]) -> str:
    """
    Insert a new row or update an existing one only where stored values are NULL
    and the CSV provides a non-empty value.
    """
    slug = payload["group_slug"]
    existing = _fetch_existing_group(sb, slug)
    now = datetime.now(timezone.utc).isoformat()

    if not existing:
        ins = {**payload, "created_at": now, "updated_at": now}
        sb.table("venue_groups").insert(ins).execute()
        sel = (
            sb.table("venue_groups")
            .select("group_id")
            .eq("group_slug", slug)
            .limit(1)
            .execute()
        )
        rows = sel.data or []
        if not rows:
            raise RuntimeError(f"Insert venue_groups failed for slug={slug}")
        return str(rows[0]["group_id"])

    gid = str(existing["group_id"])
    updates: dict[str, Any] = {"updated_at": now}
    for key, new_val in payload.items():
        if key in ("group_slug",):
            continue
        old_val = existing.get(key)
        empty_old = old_val is None or (isinstance(old_val, str) and not str(old_val).strip())
        if empty_old and new_val is not None:
            if isinstance(new_val, str) and not new_val.strip():
                continue
            updates[key] = new_val

    if len(updates) > 1:
        sb.table("venue_groups").update(updates).eq("group_id", gid).execute()
    return gid


def _find_chain_duplicate_id(
    sb: Any,
    venue_name: str,
    state: str,
    suburb_hint: Optional[str],
) -> Optional[str]:
    """Idempotency: existing CSV-sourced row with same normalised name + suburb + state."""
    state_u = (state or "").strip()
    name_l = venue_name.strip().lower()
    sub_l = (suburb_hint or "").strip().lower()
    r = (
        sb.table("venues")
        .select("id,name,suburb,state")
        .eq("data_source", CHAIN_DATA_SOURCE)
        .eq("state", state_u)
        .execute()
    )
    for row in r.data or []:
        n = str(row.get("name") or "").strip().lower()
        s = str(row.get("suburb") or "").strip().lower()
        if n == name_l and s == sub_l:
            return str(row["id"])
    return None


def insert_chain_venue(
    sb: Any,
    *,
    venue_name: str,
    suburb_hint: Optional[str],
    state: str,
    address_hint: Optional[str],
) -> Optional[str]:
    now = datetime.now(timezone.utc).isoformat()
    payload: dict[str, Any] = {
        "name": venue_name,
        "suburb": (suburb_hint or "").strip() or None,
        "state": state,
        "address": (address_hint or "").strip() or None,
        "data_source": CHAIN_DATA_SOURCE,
        "enrichment_status": "chain_csv_seed",
        "created_at": now,
        "updated_at": now,
    }
    sb.table("venues").insert(payload).execute()
    sel = (
        sb.table("venues")
        .select("id")
        .eq("data_source", CHAIN_DATA_SOURCE)
        .eq("name", venue_name)
        .eq("state", state)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = sel.data or []
    if not rows:
        return None
    return str(rows[0]["id"])


def membership_exists(sb: Any, venue_id: str, group_id: str) -> bool:
    r = (
        sb.table("venue_group_membership")
        .select("membership_id")
        .eq("venue_id", venue_id)
        .eq("group_id", group_id)
        .limit(1)
        .execute()
    )
    return bool(r.data)


def insert_membership(
    sb: Any,
    *,
    venue_id: str,
    group_id: str,
    source_url: Optional[str],
    confidence: str,
    matched_via: str,
    match_score: Optional[int],
    notes: Optional[str],
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    row: dict[str, Any] = {
        "venue_id": venue_id,
        "group_id": group_id,
        "source": CHAIN_DATA_SOURCE,
        "source_url": source_url,
        "confidence": confidence,
        "matched_via": matched_via,
        "match_score": match_score,
        "notes": notes,
        "matched_at": now,
    }
    sb.table("venue_group_membership").insert(row).execute()


def main() -> int:
    parser = argparse.ArgumentParser(description="Bulk chain mapper from CSV v3.")
    parser.add_argument(
        "--groups-csv",
        type=Path,
        default=_ROOT / "data" / "chain_seeds" / "_csv_input" / "chain_groups_v3.csv",
    )
    parser.add_argument(
        "--venues-csv",
        type=Path,
        default=_ROOT / "data" / "chain_seeds" / "_csv_input" / "chain_venues_v3.csv",
    )
    args = parser.parse_args()

    load_env()
    log_path = _setup_file_log()
    LOG.info("Log file: %s", log_path)

    started = time.monotonic()
    from supabase import create_client  # noqa: PLC0415

    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
        return 1

    sb = create_client(url, key)

    groups_path = args.groups_csv.expanduser()
    venues_path = args.venues_csv.expanduser()
    if not groups_path.is_file() or not venues_path.is_file():
        LOG.error("Missing CSV: groups=%s venues=%s", groups_path, venues_path)
        return 1

    # §3-C: parse validation
    _gmap = load_groups_by_slug(groups_path)
    _vgroups = load_venues_grouped(venues_path)
    LOG.info("CSV groups=%s venue rows across %s slugs", len(_gmap), len(_vgroups))

    all_venues: list[dict[str, Any]] = fetch_all_venues(sb)
    LOG.info("Loaded %s venue rows for fuzzy matching", len(all_venues))

    total_matched = 0
    total_inserted = 0
    total_memberships = 0
    total_skipped_membership = 0
    consecutive_insert_fails = 0
    groups_processed = 0

    for seed in iter_chain_seeds_from_csv(groups_path, venues_path):
        if time.monotonic() - started > RUNTIME_LIMIT_S:
            LOG.error("Hard stop: exceeded %s s runtime", RUNTIME_LIMIT_S)
            break

        g = seed["group"]
        group_slug = g["group_slug"]
        venues = seed["venues"]
        group_weddings_url = g.get("group_weddings_url") or ""

        gid = upsert_group_fill_nulls_only(sb, g)
        groups_processed += 1

        n_matched = n_inserted = n_member = 0
        inserted_this_group = 0

        for v in venues:
            if time.monotonic() - started > RUNTIME_LIMIT_S:
                LOG.error("Hard stop mid-group: runtime limit")
                break

            venue_name = str(v.get("name") or "")
            state = str(v.get("state") or "")
            suburb_hint = v.get("suburb")
            address_hint = v.get("address_hint")
            evidence_url = v.get("evidence_url")
            notes_csv = v.get("notes")

            source_url = (evidence_url or "").strip() or (
                (group_weddings_url or "").strip() or None
            )

            try:
                score_m = 0
                label_m = "none"
                from_chain_dup = False
                vid: Optional[str] = None

                vm, score_m, label_m = find_venue_match(v, all_venues)

                if vm:
                    vid = vm
                    consecutive_insert_fails = 0
                    total_matched += 1
                    n_matched += 1
                else:
                    dup_id = _find_chain_duplicate_id(sb, venue_name, state, suburb_hint)
                    if dup_id:
                        vid = dup_id
                        from_chain_dup = True
                        consecutive_insert_fails = 0
                    else:
                        if inserted_this_group >= MAX_INSERTS_PER_GROUP:
                            raise RuntimeError(
                                f"Abort: more than {MAX_INSERTS_PER_GROUP} inserts for group {group_slug}"
                            )
                        new_id = insert_chain_venue(
                            sb,
                            venue_name=venue_name,
                            suburb_hint=suburb_hint,
                            state=state,
                            address_hint=address_hint,
                        )
                        if not new_id:
                            consecutive_insert_fails += 1
                            LOG.error(
                                "INSERT failed for venue=%r group=%s (consecutive=%s)",
                                venue_name,
                                group_slug,
                                consecutive_insert_fails,
                            )
                            if consecutive_insert_fails >= MAX_CONSECUTIVE_INSERT_FAILS:
                                raise RuntimeError("Abort: too many consecutive insert failures")
                            continue

                        consecutive_insert_fails = 0
                        vid = new_id
                        inserted_this_group += 1
                        total_inserted += 1
                        n_inserted += 1
                        all_venues.append(
                            {
                                "id": vid,
                                "name": venue_name,
                                "suburb": suburb_hint,
                                "state": state,
                                "address": address_hint or None,
                                "place_id": None,
                                "postcode": None,
                            }
                        )

                if not vid:
                    LOG.warning("No venue_id after processing seed line group=%s name=%r", group_slug, venue_name)
                    continue

                # Membership
                if membership_exists(sb, vid, gid):
                    total_skipped_membership += 1
                    continue

                notes_parts: list[str] = []
                if notes_csv:
                    notes_parts.append(str(notes_csv))
                if label_m in ("exact", "strong", "fuzzy"):
                    notes_parts.append(
                        f"Bulk CSV v3 match tier={label_m} match_score={score_m} "
                        f"(seed=name+suburb+state+address_hint)."
                    )
                elif from_chain_dup:
                    notes_parts.append(
                        "Bulk CSV v3: idempotent reuse of existing chain_seed_csv_v3 venue row "
                        "(same name, state, suburb as seed; no duplicate insert)."
                    )
                else:
                    notes_parts.append(
                        "Bulk CSV v3: new venue row (no fuzzy match to existing public.venues row)."
                    )

                notes_val = "\n".join(notes_parts) if notes_parts else None

                if label_m in ("exact", "strong", "fuzzy"):
                    insert_membership(
                        sb,
                        venue_id=vid,
                        group_id=gid,
                        source_url=source_url,
                        confidence=_confidence_for_match(label_m),
                        matched_via=_matched_via_for_match(label_m),
                        match_score=score_m,
                        notes=notes_val,
                    )
                else:
                    insert_membership(
                        sb,
                        venue_id=vid,
                        group_id=gid,
                        source_url=source_url,
                        confidence="confirmed",
                        matched_via="newly_added",
                        match_score=None,
                        notes=notes_val,
                    )

                total_memberships += 1
                n_member += 1

            except Exception as e:
                LOG.exception(
                    "Venue error group=%s venue=%r: %s",
                    group_slug,
                    venue_name,
                    e,
                )
                traceback.print_exc(file=sys.stderr)
                continue

        LOG.info(
            "Group %s: matched=%s inserted=%s memberships_created_this_group=%s",
            group_slug,
            n_matched,
            n_inserted,
            n_member,
        )

    LOG.info(
        "Finished: groups_processed=%s total_matched=%s total_inserted=%s "
        "total_memberships=%s skipped_existing_memberships=%s",
        groups_processed,
        total_matched,
        total_inserted,
        total_memberships,
        total_skipped_membership,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
