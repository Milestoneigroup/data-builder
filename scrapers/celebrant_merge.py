"""Merge award-winner celebrants (Layer 2) into AG register rows (Layer 1).

- ``data/ag_register_raw.csv`` — ~10,740 rows from the AG scraper.
- ``data/celebrants_au_v1.csv`` — 27 award celebrants (do not modify this file).
- Fuzzy name match: ``thefuzz.token_sort_ratio``; score **≥ 85** = merge.
- ``data/celebrants_merged.csv`` — output.

Run: ``python -m scrapers.celebrant_merge``
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from thefuzz import fuzz

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

AG_PATH = _ROOT / "data" / "ag_register_raw.csv"
AW_PATH = _ROOT / "data" / "celebrants_au_v1.csv"
OUT_PATH = _ROOT / "data" / "celebrants_merged.csv"

SENT = "VERIFY_REQUIRED"


def _norm_name(s: str) -> str:
    return " ".join(str(s or "").lower().split())


def _blank(val) -> bool:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return True
    t = str(val).strip()
    return t == "" or t == SENT or t.lower() == "nan"


def _best_ag_match(award_name: str, ag: pd.DataFrame) -> tuple[int, int]:
    best_i = -1
    best = 0
    an = _norm_name(award_name)
    for i, row in ag.iterrows():
        s = fuzz.token_sort_ratio(an, _norm_name(str(row.get("full_name", ""))))
        if s > best:
            best, best_i = s, int(i) if not isinstance(i, int) else i
    return best_i, best


def run() -> None:
    if not AG_PATH.is_file():
        print(f"ERROR: {AG_PATH} not found. Run ag_register first.", file=sys.stderr)
        sys.exit(1)
    ag = pd.read_csv(AG_PATH, dtype=str, keep_default_na=False)
    if not AW_PATH.is_file():
        print(f"Warning: {AW_PATH} missing — writing AG copy to {OUT_PATH} (no award merge).")
        ag.to_csv(OUT_PATH, index=False)
        print("0 matched, 0 unmatched (no award file)")
        return
    aw = pd.read_csv(AW_PATH, dtype=str, keep_default_na=False)
    out = ag.copy()
    # Normalise column keys for award rows (lowercase)
    aw_renamed = {c: c.strip() for c in aw.columns}
    aw = aw.rename(columns=aw_renamed)
    matched = 0
    unmatched: list[pd.Series] = []
    used_ag_indices: set[int] = set()
    for _, award in aw.iterrows():
        an = award.get("full_name", award.iloc[0] if len(award) else "")
        if _blank(an):
            continue
        bi, sc = _best_ag_match(str(an), out)
        if sc >= 85 and bi >= 0 and bi not in used_ag_indices:
            used_ag_indices.add(bi)
            for col in out.columns:
                if col in award.index and not _blank(award.get(col, "")):
                    out.at[bi, col] = str(award.get(col, "")).strip()
            if "merge_fuzzy_score" in out.columns:
                out.at[bi, "merge_fuzzy_score"] = str(sc)
            if "is_standalone_award_entry" in out.columns:
                out.at[bi, "is_standalone_award_entry"] = SENT
            if "data_quality_score" in out.columns and _blank(out.at[bi, "data_quality_score"]):
                out.at[bi, "data_quality_score"] = "85"
            matched += 1
        else:
            u = award.to_dict()
            u["_fuzzy_best"] = sc
            unmatched.append(u)
    aw_seq = 0
    for u in unmatched:
        aw_seq += 1
        new_id = f"CEL-AWARD-{aw_seq:05d}"
        row = {c: SENT for c in out.columns}
        row["celebrant_id"] = new_id
        row["data_source"] = "AWARD_STANDALONE"
        row["is_standalone_award_entry"] = "true"
        bsc = u.pop("_fuzzy_best", 0)
        row["merge_fuzzy_score"] = str(bsc) if bsc is not None else SENT
        for c, val in u.items():
            if c in row and not _blank(val):
                row[c] = str(val).strip()
        out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"{matched} matched, {len(unmatched)} unmatched award winners")
    print(f"Wrote {len(out)} rows to {OUT_PATH}")


if __name__ == "__main__":
    run()
