"""Parse ABS *Tourist Accommodation, Australia* small-area workbooks (STA / legacy STAR-style cubes).

These spreadsheets contain Tourism Region ``(TR)`` sub-totals with monthly room occupancy and
takings-derived rate columns. TR labels align with ASGS Tourism Regions (same geography TRA uses).

This module does **not** hit commercial APIs; it only reads local ``.xls`` paths or bytes.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import pandas as pd

_TR_TOTAL_RE = re.compile(r"^(?P<label>.+?)\s*\(TR\)\s*Total\s*$", re.I)


@dataclass(frozen=True)
class TraMonthlyObservation:
    """One month of metrics for a single tourism region label (pre-code match)."""

    state_code: str
    region_label: str
    obs_year: int
    obs_month: int
    occupancy_pct: float | None
    adr_aud: float | None
    revpar_aud: float | None


def _row0_str(val: object) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()


def _find_block_starts(row4: pd.Series) -> dict[str, int]:
    """Return starting column index for repeated metric blocks on row 4."""
    out: dict[str, int] = {}
    for i, v in enumerate(row4):
        s = _row0_str(v)
        if not s:
            continue
        if "Room occupancy rate" in s and "Room occupancy rate" not in out:
            out["Room occupancy rate"] = i
        if "Average takings per room night occupied" in s and "adr" not in out:
            out["adr"] = i
        if "Average takings per room night available" in s and "revpar" not in out:
            out["revpar"] = i
    return out


def _parse_month_cells(row5: pd.Series, start_col: int) -> list[tuple[int, int]]:
    """Map three leading month columns to (year, month) using row 5 labels like ``July 2015``."""
    labels = [_row0_str(row5.iloc[start_col + j]) for j in range(3)]
    out: list[tuple[int, int]] = []
    for lab in labels:
        m = re.search(r"([A-Za-z]+)\s+(\d{4})", lab)
        if not m:
            out.append((0, 0))
            continue
        from calendar import month_abbr, month_name

        mon_s, y_s = m.group(1), m.group(2)
        mi = None
        for i, name in enumerate(month_name):
            if name.lower().startswith(mon_s.lower()):
                mi = i
                break
        if mi is None:
            for i, ab in enumerate(month_abbr):
                if ab and ab.lower() == mon_s[:3].lower():
                    mi = i
                    break
        if mi is None:
            out.append((0, 0))
        else:
            out.append((int(y_s), mi))
    return out


def _float_cell(row: pd.Series, idx: int) -> float | None:
    if idx >= len(row):
        return None
    v = row.iloc[idx]
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def parse_sta_workbook(
    source: str | Path | bytes | BinaryIO,
    *,
    state_code: str,
    sheet_names: tuple[str, ...] = ("Table_10", "Table_11", "Table_12", "Table_13"),
) -> list[TraMonthlyObservation]:
    """Parse one state's STA ``.xls`` workbook and return monthly TR-level observations."""
    if isinstance(source, (str, Path)):
        xl = pd.ExcelFile(source)
    elif isinstance(source, bytes):
        xl = pd.ExcelFile(io.BytesIO(source))
    else:
        xl = pd.ExcelFile(source)

    observations: list[TraMonthlyObservation] = []
    for sn in sheet_names:
        if sn not in xl.sheet_names:
            continue
        df = pd.read_excel(xl, sheet_name=sn, header=None)
        if df.shape[0] < 8 or df.shape[1] < 20:
            continue
        title = _row0_str(df.iat[3, 0])
        if "tourism region" not in title.lower():
            continue
        blocks = _find_block_starts(df.iloc[4])
        occ_i = blocks.get("Room occupancy rate")
        adr_i = blocks.get("adr")
        rev_i = blocks.get("revpar")
        if occ_i is None:
            continue
        months = _parse_month_cells(df.iloc[5], occ_i)
        if any(y == 0 for y, _ in months):
            continue

        for ri in range(7, len(df)):
            label_cell = df.iat[ri, 0]
            m = _TR_TOTAL_RE.match(_row0_str(label_cell))
            if not m:
                continue
            region_label = m.group("label").strip()
            row = df.iloc[ri]
            for j in range(3):
                y, mo = months[j]
                if y == 0 or mo == 0:
                    continue
                occ = _float_cell(row, occ_i + j)
                adr = _float_cell(row, adr_i + j) if adr_i is not None else None
                rev = _float_cell(row, rev_i + j) if rev_i is not None else None
                observations.append(
                    TraMonthlyObservation(
                        state_code=state_code,
                        region_label=region_label,
                        obs_year=y,
                        obs_month=mo,
                        occupancy_pct=occ,
                        adr_aud=adr,
                        revpar_aud=rev,
                    )
                )
    return observations
