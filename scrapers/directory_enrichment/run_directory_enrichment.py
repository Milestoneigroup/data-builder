"""Directory enrichment Service A — CLI and Railway entry point."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pytz
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

load_dotenv(_ROOT / "env.local", override=True)
load_dotenv(_ROOT / ".env.local", override=True)
load_dotenv(_ROOT / ".env", override=True)

from scrapers.directory_enrichment import easy_weddings, hello_may  # noqa: E402
from scrapers.directory_enrichment._framework import (  # noqa: E402
    get_supabase_client,
    setup_logging,
)
from scrapers.directory_enrichment._vendor_router import VendorRouter  # noqa: E402


def _vendor_type_list(raw: str) -> list[str]:
    if raw == "all":
        return ["all"]
    return [raw]


def _run_directory_pass(
    *,
    directory: str,
    vendor_types: list[str],
    limit: int,
    dry_run: bool,
    start_page: int,
    deadline_mono: float | None,
    parent_log: logging.Logger,
) -> None:
    sb = get_supabase_client()
    router = VendorRouter(sb, dry_run=dry_run, logger=parent_log)
    ua = easy_weddings.extract_user_agent()

    if directory in ("easy_weddings", "all"):
        parent_log.info("Starting Easy Weddings pass (limit=%s).", limit)
        cl = easy_weddings.build_httpx_client()
        try:
            stats = easy_weddings.run_easy_weddings(
                client=cl,
                router=router,
                log=parent_log,
                vendor_types=vendor_types,
                limit=limit,
                start_page=start_page,
                dry_run=dry_run,
                deadline_mono=deadline_mono,
                user_agent=ua,
            )
            parent_log.info("Easy Weddings finished: %s", stats)
        finally:
            cl.close()

    if directory in ("hello_may", "all"):
        parent_log.info("Starting Hello May pass (limit=%s).", limit)
        cl = hello_may.build_httpx_client()
        try:
            stats = hello_may.run_hello_may(
                client=cl,
                router=router,
                log=parent_log,
                vendor_types=vendor_types,
                limit=limit,
                dry_run=dry_run,
                deadline_mono=deadline_mono,
                user_agent=hello_may.extract_user_agent(),
            )
            parent_log.info("Hello May finished: %s", stats)
        finally:
            cl.close()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Directory enrichment Service A (Easy Weddings + Hello May)."
    )
    p.add_argument(
        "--directory",
        choices=("easy_weddings", "hello_may", "all"),
        default="all",
        help="Which directory source to run.",
    )
    p.add_argument(
        "--vendor-type",
        choices=("venues", "photographers", "celebrants", "all"),
        default="all",
        dest="vendor_type",
    )
    p.add_argument("--limit", type=int, default=99999, help="Max vendors to process.")
    p.add_argument("--dry-run", action="store_true", help="Parse only; no Supabase writes.")
    p.add_argument(
        "--schedule",
        action="store_true",
        help="Run as APScheduler daemon (weekly cron, UTC).",
    )
    p.add_argument("--start-page", type=int, default=1, help="Easy Weddings pagination start.")
    p.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=1800,
        help="Hard stop for one-shot runs (default 30 minutes). Ignored with --schedule.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    os.chdir(_ROOT)
    args = _parse_args(argv)
    log_path, log = setup_logging()
    log.info("Log file: %s", log_path)

    vt = _vendor_type_list(args.vendor_type)

    if args.schedule:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger

        sched = BlockingScheduler(timezone=pytz.utc)

        def _job_ew() -> None:
            sb = get_supabase_client()
            router = VendorRouter(sb, dry_run=False, logger=log)
            ua = easy_weddings.extract_user_agent()
            cl = easy_weddings.build_httpx_client()
            try:
                easy_weddings.run_easy_weddings(
                    client=cl,
                    router=router,
                    log=log,
                    vendor_types=["all"],
                    limit=args.limit,
                    start_page=1,
                    dry_run=False,
                    deadline_mono=None,
                    user_agent=ua,
                )
            finally:
                cl.close()

        def _job_hm() -> None:
            sb = get_supabase_client()
            router = VendorRouter(sb, dry_run=False, logger=log)
            cl = hello_may.build_httpx_client()
            try:
                hello_may.run_hello_may(
                    client=cl,
                    router=router,
                    log=log,
                    vendor_types=["all"],
                    limit=args.limit,
                    dry_run=False,
                    deadline_mono=None,
                    user_agent=hello_may.extract_user_agent(),
                )
            finally:
                cl.close()

        sched.add_job(
            _job_ew,
            CronTrigger(day_of_week="mon", hour=2, minute=0),
            id="directory_enrichment_easy_weddings_weekly",
        )
        sched.add_job(
            _job_hm,
            CronTrigger(day_of_week="tue", hour=2, minute=0),
            id="directory_enrichment_hello_may_weekly",
        )
        log.info(
            "BlockingScheduler started (UTC): Easy Weddings Mondays 02:00; Hello May Tuesdays 02:00."
        )
        sched.start()
        return 0

    deadline_mono: float | None = None
    if args.max_runtime_seconds and args.max_runtime_seconds > 0:
        deadline_mono = time.monotonic() + float(args.max_runtime_seconds)

    _run_directory_pass(
        directory=args.directory,
        vendor_types=vt,
        limit=args.limit,
        dry_run=args.dry_run,
        start_page=max(1, args.start_page),
        deadline_mono=deadline_mono,
        parent_log=log,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
