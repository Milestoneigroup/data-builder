"""Poll Railway deployment logs until verifier completion markers appear.

Run from repo root::

    python -m scrapers.llm_data_verifier._overnight_watcher

Requires a linked Railway project and authenticated CLI (``railway login``
or ``RAILWAY_TOKEN``). Optional env:

- ``RAILWAY_SERVICE`` — service name (default: ``llm-data-verifier``)
- ``RAILWAY_LOG_LINES`` — lines per fetch (default: 8000)
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_OUT = _ROOT / "scrapers" / "llm_data_verifier" / "outputs"
_LOG_FILE = _OUT / "railway_logs.txt"
_COMPLETION_RE = re.compile(r"===END OUTPUT FILE\s+verification_summary_.*===\s*$")
_POLL_SECONDS = 60
_TIMEOUT_SECONDS = 90 * 60
_MAX_CONSECUTIVE_CLI_ERRORS = 3


def _railway_exe() -> list[str]:
    """Command prefix to invoke Railway CLI (local binary or npx)."""
    direct = shutil.which("railway")
    if direct:
        return [direct]
    npx = shutil.which("npx")
    if npx:
        return [npx, "--yes", "@railway/cli"]
    raise RuntimeError("Neither 'railway' nor 'npx' found on PATH")


def _fetch_logs(service: str, lines: int) -> tuple[int, str]:
    prefix = _railway_exe()
    cmd = [
        *prefix,
        "logs",
        "--json",
        "--lines",
        str(lines),
        "--latest",
        "--service",
        service,
    ]
    p = subprocess.run(
        cmd,
        cwd=_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )
    err = (p.stderr or "").strip()
    out = (p.stdout or "").strip()
    merged = (out + ("\n" + err if err else "")).strip()
    return p.returncode, merged


def _append_snapshot(blob: str, poll_index: int) -> None:
    _OUT.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    with _LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(f"\n\n----- poll {poll_index} @ {ts} UTC -----\n\n")
        fh.write(blob)
        fh.write("\n")


def main() -> int:
    service = (os.environ.get("RAILWAY_SERVICE") or "llm-data-verifier").strip()
    lines = int(os.environ.get("RAILWAY_LOG_LINES") or "8000")

    t_start = time.monotonic()
    seen_messages: set[str] = set()
    all_text_parts: list[str] = []
    consec_errors = 0
    poll_index = 0

    while True:
        poll_index += 1
        elapsed = time.monotonic() - t_start
        if elapsed > _TIMEOUT_SECONDS:
            return 1

        try:
            code, out = _fetch_logs(service, lines)
        except (RuntimeError, subprocess.TimeoutExpired, OSError) as e:
            consec_errors += 1
            _append_snapshot(f"[watcher] fetch error: {e!r}", poll_index)
            if consec_errors >= _MAX_CONSECUTIVE_CLI_ERRORS:
                return 2
            time.sleep(_POLL_SECONDS)
            continue

        if code != 0:
            consec_errors += 1
            _append_snapshot(f"[watcher] railway exit {code}:\n{out}", poll_index)
            if consec_errors >= _MAX_CONSECUTIVE_CLI_ERRORS:
                return 2
            time.sleep(_POLL_SECONDS)
            continue

        consec_errors = 0

        extracted_lines: list[str] = []
        for raw_line in out.splitlines():
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                obj = json.loads(raw_line)
                msg = obj.get("message")
                if msg is None:
                    msg = raw_line
                else:
                    msg = str(msg)
            except json.JSONDecodeError:
                msg = raw_line
            if msg not in seen_messages:
                seen_messages.add(msg)
                extracted_lines.append(msg)

        if extracted_lines:
            blob = "\n".join(extracted_lines)
            all_text_parts.append(blob)
            _append_snapshot(blob, poll_index)

        combined = "\n".join(all_text_parts)
        if _COMPLETION_RE.search(combined) or "===END OUTPUT FILE verification_summary_" in combined:
            return 0

        time.sleep(_POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
