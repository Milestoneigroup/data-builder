"""Build data/seed_weather_test_cells.json from shared.ref_destinations.

Geographic rules match SILO expansion handoff. Run from repo root:
  python scripts/build_seed_weather_expansion.py

Requires .env.local with SUPABASE_* (same as scrapers).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

for _p in (_ROOT / ".env", _ROOT / ".env.local", _ROOT / "env.local"):
    if _p.is_file():
        load_dotenv(_p, override=True, encoding="utf-8")

from data_builder.config import get_settings  # noqa: E402
from supabase import create_client  # noqa: E402

SEED_PATH = _ROOT / "data" / "seed_weather_test_cells.json"

LOCKED: list[dict[str, object]] = [
    {"coverage_label": "Greater Sydney Metro", "requested_lat": -33.815, "requested_lng": 151.001},
    {"coverage_label": "Greater Melbourne Metro", "requested_lat": -37.8136, "requested_lng": 144.9631},
    {"coverage_label": "Hunter Valley Wine Country", "requested_lat": -32.7796, "requested_lng": 151.29},
    {"coverage_label": "Yarra Valley & Dandenong Ranges", "requested_lat": -37.65, "requested_lng": 145.55},
    {"coverage_label": "Margaret River & South West WA", "requested_lat": -33.9556, "requested_lng": 115.0736},
    {"coverage_label": "Sunshine Coast & Noosa", "requested_lat": -26.4, "requested_lng": 153.05},
    {"coverage_label": "Whitsundays", "requested_lat": -20.27, "requested_lng": 148.72},
    {"coverage_label": "Hobart & Southern Tasmania", "requested_lat": -42.8821, "requested_lng": 147.3272},
    {"coverage_label": "Barossa Valley", "requested_lat": -34.5333, "requested_lng": 138.95},
    {"coverage_label": "Byron Region (Northern Rivers)", "requested_lat": -28.75, "requested_lng": 153.45},
]

WA_EXCLUDED_H1_FRAGMENTS = (
    "broome and the kimberley",
    "coral coast (geraldton",
    "esperance and goldfields",
)

QLD_INLAND_RE = re.compile(
    r"mount\s+isa|longreach|charleville|winton|birdsville|\broma\b|"
    r"barcaldine|quilpie|thargomindah|blackall|cloncurry|julia\s+creek|"
    r"mckinlay|hughenden|richmond\s+ql|torrens\s+creek|camooweal",
    re.I,
)

NSW_FAR_WEST_RE = re.compile(
    r"broken\s+hill|white\s+cliffs|wilcannia|menindee|\bbourke\b|"
    r"far\s+west|tibooburra|lightning\s+ridge",
    re.I,
)


def silo_grid_key(lat: float, lng: float) -> tuple[float, float]:
    step = 0.05
    rlat = round(lat / step) * step
    rlng = round(lng / step) * step
    return (round(rlat, 6), round(rlng, 6))


def fetch_all(sb: object) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = 0
    page = 1000
    tbl = sb.schema("shared").table("ref_destinations")
    while True:
        r = (
            tbl.select(
                "destination_id,destination_name,hierarchy_level,"
                "parent_destination_id,state_code,lat,lng"
            )
            .eq("is_active", True)
            .range(start, start + page - 1)
            .execute()
        )
        batch = r.data or []
        rows.extend(batch)
        if len(batch) < page:
            break
        start += page
    return rows


def root_wa_excluded_ids(rows: list[dict[str, object]]) -> set[str]:
    out: set[str] = set()
    for row in rows:
        if int(row.get("hierarchy_level") or 0) != 1:
            continue
        if (row.get("state_code") or "") != "WA":
            continue
        name = (row.get("destination_name") or "").lower()
        if any(frag in name for frag in WA_EXCLUDED_H1_FRAGMENTS):
            did = row.get("destination_id")
            if isinstance(did, str):
                out.add(did)
    return out


def ancestors(by_id: dict[str, dict[str, object]], did: str) -> list[dict[str, object]]:
    chain: list[dict[str, object]] = []
    cur: str | None = did
    for _ in range(12):
        row = by_id.get(cur or "")
        if not row:
            break
        chain.append(row)
        pid = row.get("parent_destination_id")
        if not pid or str(pid) == "NOT_APPLICABLE":
            break
        cur = str(pid)
    return chain


def under_any_root(by_id: dict[str, dict[str, object]], did: str, roots: set[str]) -> bool:
    for a in ancestors(by_id, did):
        aid = a.get("destination_id")
        if isinstance(aid, str) and aid in roots:
            return True
    return False


def nt_exclude_non_top_end_h2(by_id: dict[str, dict[str, object]], row: dict[str, object]) -> bool:
    if (row.get("state_code") or "") != "NT":
        return False
    if int(row.get("hierarchy_level") or 0) <= 1:
        return False
    for a in ancestors(by_id, str(row.get("destination_id") or "")):
        n = (a.get("destination_name") or "").lower()
        if "darwin and top end" in n:
            return False
        if "alice springs" in n or "uluru" in n or "red centre" in n:
            return True
    return False


def geo_exclude(
    row: dict[str, object],
    by_id: dict[str, dict[str, object]],
    wa_excluded_roots: set[str],
) -> bool:
    name = row.get("destination_name") or ""
    did = str(row.get("destination_id") or "")

    if under_any_root(by_id, did, wa_excluded_roots):
        return True
    if QLD_INLAND_RE.search(name):
        return True
    if NSW_FAR_WEST_RE.search(name):
        return True
    if nt_exclude_non_top_end_h2(by_id, row):
        return True
    return False


def state_for_output_label(label: str, rows: list[dict[str, object]]) -> str:
    t = label.strip()
    for r in rows:
        if str(r.get("destination_name") or "").strip() == t:
            return str(r.get("state_code") or "ZZ")
    # Seed labels that differ from DB naming
    manual = {
        "Greater Sydney Metro": "NSW",
        "Greater Melbourne Metro": "VIC",
        "Hunter Valley Wine Country": "NSW",
        "Yarra Valley & Dandenong Ranges": "VIC",
        "Margaret River & South West WA": "WA",
        "Sunshine Coast & Noosa": "QLD",
        "Whitsundays": "QLD",
        "Hobart & Southern Tasmania": "TAS",
        "Barossa Valley": "SA",
        "Byron Region (Northern Rivers)": "NSW",
    }
    return manual.get(t, "ZZ")


def main() -> None:
    settings = get_settings()
    url = (settings.supabase_url or "").strip()
    key = (settings.supabase_service_role_key or settings.supabase_key or "").strip()
    if not url or not key:
        raise SystemExit("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required")

    sb = create_client(url, key)
    rows = fetch_all(sb)
    by_id = {str(r["destination_id"]): r for r in rows if r.get("destination_id")}
    wa_excl = root_wa_excluded_ids(rows)

    # Cell value: hierarchy (-1=locked), label, lat, lng, locked
    cells: dict[tuple[float, float], tuple[int, str, float, float, bool]] = {}
    for e in LOCKED:
        la, lo = float(e["requested_lat"]), float(e["requested_lng"])
        k = silo_grid_key(la, lo)
        cells[k] = (-1, str(e["coverage_label"]), la, lo, True)

    def add_candidate(h_level: int, label: str, lat: float, lng: float) -> None:
        k = silo_grid_key(lat, lng)
        cur = cells.get(k)
        if cur is not None:
            _ch, _lab, _la, _lo, locked = cur
            if locked:
                return
            if h_level > _ch:
                return
            if h_level == _ch and label >= _lab:
                return
        cells[k] = (h_level, label, lat, lng, False)

    h1_rows = [r for r in rows if int(r.get("hierarchy_level") or 0) == 1]
    h1_rows.sort(key=lambda r: ((r.get("state_code") or ""), (r.get("destination_name") or "")))
    for r in h1_rows:
        if geo_exclude(r, by_id, wa_excl):
            continue
        lat, lng = r.get("lat"), r.get("lng")
        if lat is None or lng is None:
            continue
        add_candidate(1, str(r.get("destination_name") or "").strip(), float(lat), float(lng))

    # Re-assert locked labels/coords before counting toward P2 cap
    for e in LOCKED:
        la, lo = float(e["requested_lat"]), float(e["requested_lng"])
        k = silo_grid_key(la, lo)
        cells[k] = (-1, str(e["coverage_label"]), la, lo, True)

    TARGET_CAP = 100
    MIN_FLOOR = 80

    def try_add_new_grid_only(h_level: int, label: str, lat: float, lng: float) -> bool:
        """Add only if grid empty and below TARGET_CAP (never overwrites)."""
        if len(cells) >= TARGET_CAP:
            return False
        k = silo_grid_key(lat, lng)
        if k in cells:
            return False
        cells[k] = (h_level, label, lat, lng, False)
        return True

    h2_rows = [r for r in rows if int(r.get("hierarchy_level") or 0) == 2]
    h2_rows.sort(key=lambda r: ((r.get("state_code") or ""), (r.get("destination_name") or "")))

    def p2_pass(r: dict[str, object], parent_states: set[str]) -> bool:
        if geo_exclude(r, by_id, wa_excl):
            return False
        parent = by_id.get(str(r.get("parent_destination_id") or ""))
        if not parent:
            return False
        if int(parent.get("hierarchy_level") or 0) != 1:
            return False
        st = str(parent.get("state_code") or "")
        if st not in parent_states:
            return False
        if geo_exclude(parent, by_id, wa_excl):
            return False
        return True

    for r in h2_rows:
        if not p2_pass(r, {"NSW", "VIC", "QLD", "WA"}):
            continue
        lat, lng = r.get("lat"), r.get("lng")
        if lat is None or lng is None:
            continue
        try_add_new_grid_only(2, str(r.get("destination_name") or "").strip(), float(lat), float(lng))

    if len(cells) < MIN_FLOOR:
        for r in h2_rows:
            if len(cells) >= TARGET_CAP:
                break
            if not p2_pass(r, {"SA", "TAS", "ACT"}):
                continue
            lat, lng = r.get("lat"), r.get("lng")
            if lat is None or lng is None:
                continue
            try_add_new_grid_only(2, str(r.get("destination_name") or "").strip(), float(lat), float(lng))

    for e in LOCKED:
        la, lo = float(e["requested_lat"]), float(e["requested_lng"])
        k = silo_grid_key(la, lo)
        cells[k] = (-1, str(e["coverage_label"]), la, lo, True)

    out = [
        {"coverage_label": lab, "requested_lat": la, "requested_lng": lo}
        for _k, (_h, lab, la, lo, _lk) in cells.items()
    ]
    out.sort(key=lambda d: (state_for_output_label(str(d["coverage_label"]), rows), str(d["coverage_label"])))

    n = len(out)
    print("unique_cells", n)
    if n < 80 or n > 120:
        print("WARNING: outside 80–120 band; review required.")
        sys.exit(2)

    SEED_PATH.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    print("wrote", SEED_PATH)


if __name__ == "__main__":
    main()
