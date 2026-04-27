import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.chdir(sys.path[0])

from scrapers.celebrant_active_enrichment import run_step1, run_step2

if __name__ == "__main__":
    run_step1(
        ew_pages=15,
        skip_ew=True,
        skip_afcc=True,
        skip_mycelebrantapp=True,
    )
    run_step2(output_file="data/celebrants_master_v3.csv")
    print("DONE — celebrants_master_v3.csv written")
