"""Read-only Supabase REST fallback when CSV rows lack a usable business name."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import quote

import requests

__all__ = ["lookup_business_name"]


def lookup_business_name(
    vendor_id: str,
    vertical: str,
    *,
    logger: logging.Logger,
    timeout: int = 20,
) -> str:
    """Resolve a display name via PostgREST (no writes). Best-effort only."""
    vid = (vendor_id or "").strip()
    if not vid:
        return ""
    base = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not base or not key:
        return ""

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
    }
    qvid = quote(vid, safe="")

    if vertical == "celebrants":
        url = (
            f"{base}/rest/v1/celebrants"
            f"?celebrant_id=eq.{qvid}&select=full_name,ag_display_name&limit=1"
        )
    elif vertical == "photographers":
        table = (os.getenv("VERIFIER_PHOTOGRAPHERS_TABLE") or "photographers").strip()
        id_col = (os.getenv("VERIFIER_PHOTOGRAPHERS_ID_COLUMN") or "photographer_id").strip()
        url = (
            f"{base}/rest/v1/{quote(table, safe='')}"
            f"?{quote(id_col, safe='')}=eq.{qvid}"
            "&select=display_name,business_name,ag_display_name,full_name&limit=1"
        )
    else:
        return ""

    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            logger.debug("Supabase name lookup HTTP %s for %s", r.status_code, vid)
            return ""
        data: Any = r.json()
        if not isinstance(data, list) or not data:
            return ""
        row = data[0]
        for k in (
            "ag_display_name",
            "full_name",
            "display_name",
            "business_name",
        ):
            v = row.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    except Exception as e:
        logger.debug("Supabase name lookup failed for %s: %s", vid, e)
    return ""
