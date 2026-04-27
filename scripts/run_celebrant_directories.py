import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.chdir(sys.path[0])

from scrapers.celebrant_active_enrichment import WL_CITY_URLS, run_step1, run_step2

if __name__ == "__main__":
    run_step1(
        ew_pages=15,
        skip_ew=True,
        skip_afcc=True,
        skip_mycelebrantapp=True,
        wedlockers_city_urls=list(WL_CITY_URLS),
        request_delay_s=3.0,
        directory_browser_headers=True,
    )
    _, _, n_up = run_step2(
        output_file="data/celebrants_master_v3.csv",
        upsert_active_to_supabase=True,
    )
    print(f"Upserted {n_up} active celebrants to Supabase")
    print("DONE — celebrants_master_v3.csv written")
    raise SystemExit(0)
