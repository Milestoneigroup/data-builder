"""Shared helpers for influencer directory loaders and scrapers."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse


def norm_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u
    try:
        p = urlparse(u)
        scheme = (p.scheme or "https").lower()
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "").rstrip("/") or ""
        return f"{scheme}://{host}{path}"
    except Exception:  # noqa: BLE001
        return u.lower().rstrip("/")


def dedupe_key(url: str) -> str:
    return norm_url(url)


def root_domain(url: str) -> str | None:
    u = norm_url(url)
    if not u:
        return None
    try:
        host = urlparse(u).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:  # noqa: BLE001
        return None


def states_to_pipe(s: str | None) -> str | None:
    if s is None:
        return None
    t = str(s).strip()
    if not t or t.lower() == "nan":
        return None
    t = t.replace(",", "|").replace(";", "|")
    parts = [p.strip().upper() for p in t.split("|") if p.strip()]
    if not parts:
        return None
    if len(parts) == 1 and parts[0] == "ALL":
        return "NSW|VIC|QLD|WA|SA|TAS|NT|ACT"
    return "|".join(parts)


def is_xlsx_file(path: Path) -> bool:
    try:
        return path.read_bytes()[:2] == b"PK"
    except OSError:
        return False
