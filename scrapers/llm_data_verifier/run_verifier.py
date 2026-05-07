"""CLI entry for the LLM enrichment live verifier (Railway + local)."""

from __future__ import annotations

import argparse
import base64
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

load_dotenv(_ROOT / "env.local", override=True)

from scrapers.llm_data_verifier.verify_vendors import (  # noqa: E402
    iso_date_utc,
    run_vertical,
    write_summary_md,
)


def _setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s\t%(levelname)s\t%(message)s",
    )
    log = logging.getLogger("llm_data_verifier")
    return log


def _emit_base64(paths: list[Path]) -> None:
    """Print outputs for Railway log capture (§17)."""
    for p in paths:
        if not p.is_file():
            continue
        raw = p.read_bytes()
        b64 = base64.b64encode(raw).decode("ascii")
        print(f"===BEGIN OUTPUT FILE {p.name}===", flush=True)
        print(b64, flush=True)
        print(f"===END OUTPUT FILE {p.name}===", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Live web verification for LLM-enriched vendor CSVs.")
    ap.add_argument(
        "--input-dir",
        type=Path,
        default=_ROOT / "scrapers" / "llm_data_verifier" / "inputs",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=_ROOT / "scrapers" / "llm_data_verifier" / "outputs",
    )
    ap.add_argument(
        "--vendor-type",
        choices=("photographers", "celebrants", "all"),
        default="all",
    )
    ap.add_argument("--max-vendors", type=int, default=0, help="0 = no limit")
    ap.add_argument(
        "--start-from",
        default="",
        help="Resume after this vendor_id (row is skipped).",
    )
    ap.add_argument(
        "--skip-rejected-host",
        default="",
        help="Comma-separated hostnames to skip after historic 403 blocks.",
    )
    ap.add_argument(
        "--no-base64-emit",
        action="store_true",
        help="Do not print base64-wrapped outputs (for noisy local runs).",
    )
    args = ap.parse_args()

    log = _setup_logging()
    iso = iso_date_utc()
    skip_hosts: set[str] = {h.strip().lower().removeprefix("www.") for h in args.skip_rejected_host.split(",") if h.strip()}

    photo_in = args.input_dir / "master_photographers_enriched.csv"
    celeb_in = args.input_dir / "master_celebrants_enriched.csv"
    photo_out = args.output_dir / f"master_photographers_VERIFIED_{iso}.csv"
    celeb_out = args.output_dir / f"master_celebrants_VERIFIED_{iso}.csv"
    summary_path = args.output_dir / f"verification_summary_{iso}.md"

    args.output_dir.mkdir(parents=True, exist_ok=True)

    max_v = args.max_vendors if args.max_vendors > 0 else 0
    start_after = args.start_from.strip() or None

    photo_meta: dict = {"processed": 0, "tiers": {}, "output": str(photo_out)}
    celeb_meta: dict = {"processed": 0, "tiers": {}, "output": str(celeb_out)}
    rejected_all: list[dict[str, str]] = []
    ran_photo = False
    ran_celeb = False

    try:
        if args.vendor_type in ("photographers", "all"):
            if not photo_in.is_file():
                log.error("Missing input CSV: %s", photo_in)
                if args.vendor_type == "photographers":
                    return 1
            else:
                _, rej, tiers = run_vertical(
                    input_csv=photo_in,
                    output_csv=photo_out,
                    vertical="photographers",
                    logger=log,
                    skip_hosts=skip_hosts,
                    max_vendors=max_v,
                    start_after=start_after,
                    flush_every=25,
                )
                ran_photo = True
                photo_meta["processed"] = int(sum(tiers.values()))
                photo_meta["tiers"] = dict(tiers)
                rejected_all.extend(rej)

        if args.vendor_type in ("celebrants", "all"):
            if not celeb_in.is_file():
                log.error("Missing input CSV: %s", celeb_in)
                if args.vendor_type == "celebrants":
                    return 1
            else:
                _, rej, tiers = run_vertical(
                    input_csv=celeb_in,
                    output_csv=celeb_out,
                    vertical="celebrants",
                    logger=log,
                    skip_hosts=skip_hosts,
                    max_vendors=max_v,
                    start_after=start_after,
                    flush_every=25,
                )
                ran_celeb = True
                celeb_meta["processed"] = int(sum(tiers.values()))
                celeb_meta["tiers"] = dict(tiers)
                rejected_all.extend(rej)

    except KeyboardInterrupt:
        log.warning("Run halted by operator — partial CSVs may exist under outputs/")
        write_summary_md(
            summary_path,
            iso_date=iso,
            photographers_meta=photo_meta,
            celebrants_meta=celeb_meta,
            rejected=rejected_all,
        )
        return 130

    write_summary_md(
        summary_path,
        iso_date=iso,
        photographers_meta=photo_meta,
        celebrants_meta=celeb_meta,
        rejected=rejected_all,
    )

    if not args.no_base64_emit:
        to_emit = [summary_path]
        if ran_photo and photo_out.is_file():
            to_emit.append(photo_out)
        if ran_celeb and celeb_out.is_file():
            to_emit.append(celeb_out)
        _emit_base64(to_emit)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
