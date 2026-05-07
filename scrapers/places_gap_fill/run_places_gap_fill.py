"""CLI + Railway worker entry-point for Tier 1 Places gap-fill enrichment."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

import httpx

from ._framework import (
    BudgetTracker,
    places_api_key,
    setup_logging,
    supabase_clients,
)
from .celebrants import run_celebrants_gap_fill
from .photographers import run_photographers_gap_fill
from .venues import run_venues_gap_fill


def _run_batches(
    vendor_type: str,
    *,
    limit: int,
    max_budget_usd: float,
    dry_run: bool,
    log: logging.Logger,
) -> tuple[list[dict[str, Any]], BudgetTracker]:
    places_api_key()

    tracker = BudgetTracker(max_budget_usd)
    agg: list[dict[str, Any]] = []

    sb, _ = supabase_clients()
    log.info(
        "run start vendor_type=%s limit=%s max_budget_usd=%s dry_run=%s tracker=%s",
        vendor_type,
        limit,
        max_budget_usd,
        dry_run,
        tracker.report(),
    )

    with httpx.Client() as http:
        if vendor_type in ("venues", "all"):
            spent_before = float(tracker.spent_usd)
            summ = run_venues_gap_fill(sb, http, tracker, limit=limit, dry_run=dry_run, log=log)
            summ["_incremental_spend_usd"] = float(tracker.spent_usd) - spent_before
            agg.append(summ)
        if vendor_type in ("celebrants", "all"):
            spent_before = float(tracker.spent_usd)
            summ = run_celebrants_gap_fill(sb, http, tracker, limit=limit, dry_run=dry_run, log=log)
            summ["_incremental_spend_usd"] = float(tracker.spent_usd) - spent_before
            agg.append(summ)
        if vendor_type in ("photographers", "all"):
            spent_before = float(tracker.spent_usd)
            summ = run_photographers_gap_fill(sb, http, tracker, limit=limit, dry_run=dry_run, log=log)
            summ["_incremental_spend_usd"] = float(tracker.spent_usd) - spent_before
            agg.append(summ)

    for s in agg:
        vt = str(s.get("processed_type") or "unknown")
        hi = int(s.get("enriched_high") or 0)
        low = int(s.get("enriched_low_website") or 0)
        sk = int(s.get("skipped") or 0)
        touched = int(s.get("vendors_touched") or 0)
        tqv = int(s.get("total_query_variations") or 0)
        avg_q = (tqv / touched) if touched else 0.0
        inc_spend = float(s.pop("_incremental_spend_usd", tracker.spent_usd))
        log.info(
            "%s complete: %s enriched (high), %s website-only (low), %s skipped, "
            "spend $%.2f, avg query variations: %.2f",
            vt,
            hi,
            low,
            sk,
            inc_spend,
            avg_q,
        )

    return agg, tracker


def _cron_schedule() -> None:
    """Fire on the fifteenth calendar day each month at 02:00 UTC (Railway daemon)."""

    from zoneinfo import ZoneInfo

    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    tz = ZoneInfo("UTC")

    scheduler = BlockingScheduler(timezone=tz)
    trig = CronTrigger(day=15, hour=2, minute=0, second=0, timezone=tz)

    def scheduled_job() -> None:
        jlog, _ = setup_logging()
        jlog.info("scheduled_gap_fill invoking vendor_type=all budget_usd=50")
        summaries, tracker = _run_batches(
            "all",
            limit=9999,
            max_budget_usd=50.0,
            dry_run=False,
            log=jlog,
        )
        jlog.info(
            "scheduled_gap_fill summaries=%s spend_report=%s",
            summaries,
            tracker.report(),
        )

    scheduler.add_job(scheduled_job, trig)
    scheduler.start()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tier 1 Google Places gap enrichment (Railway-compatible worker).",
    )
    parser.add_argument(
        "--vendor-type",
        choices=("venues", "celebrants", "photographers", "all"),
        default="venues",
        help="Which cohort to hydrate this run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=9999,
        help="Maximum vendors fetched from Supabase REST per cohort (pagination applies).",
    )
    parser.add_argument(
        "--max-budget-usd",
        type=float,
        default=100.0,
        dest="budget",
        help="Hard stop when estimated USD would exceed this cap.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run Text Search → Details extraction only; omit Supabase updates.",
    )
    parser.add_argument(
        "--schedule",
        action="store_true",
        help=(
            "Blocking daemon: trigger automatically on each fifteenth calendar day "
            "at 02:00 UTC with USD 50 per sweep."
        ),
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    log, path = setup_logging()

    if args.schedule:
        log.info("scheduler mode Railway — log=%s", path)
        try:
            _cron_schedule()
        except KeyboardInterrupt:  # pragma: no cover - operator CTRL+C locally
            log.warning("scheduler interrupted manually")
            return 130
        return 0

    summaries, tracker = _run_batches(
        args.vendor_type,
        limit=args.limit,
        max_budget_usd=args.budget,
        dry_run=args.dry_run,
        log=log,
    )

    msg = {"summaries": summaries, "spent": tracker.report(), "log_path": str(path)}
    log.info("run_complete %s", msg)
    print(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
