"""
Logistics-only mini pipeline
============================
Reads DailyShipping&LogisticsReport.xls, runs compute_logistics(),
writes cache/latest/cases_logistics.csv + logistics_summary.csv,
then pushes to GitHub only if the data actually changed.

Run manually : py pipeline_logistics_only.py
Scheduled    : every 1-2 minutes via Task Scheduler (run_logistics.bat)
"""

import logging
import subprocess
from datetime import date
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [logistics] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
with open(BASE_DIR / "config.yaml") as f:
    CFG = yaml.safe_load(f)

CACHE_DIR  = BASE_DIR / CFG["output"]["local_cache_dir"]
LATEST_DIR = CACHE_DIR / "latest"
LATEST_DIR.mkdir(parents=True, exist_ok=True)

WATCH_FOLDER = Path(CFG["data_source"]["csv"]["watch_folder"])


def main():
    from mt_reports_parser import load_shipping_logistics_report
    from pipeline_logistics import compute_logistics

    log.info("Loading shipping & logistics report from %s", WATCH_FOLDER)
    shipping_df = load_shipping_logistics_report(WATCH_FOLDER)

    if shipping_df.empty:
        log.warning("No data returned from load_shipping_logistics_report — aborting")
        return

    log.info("Loaded %d cases — running compute_logistics()", len(shipping_df))
    compute_logistics(
        cases_df=shipping_df,
        base_dir=BASE_DIR,
        cache_dir=CACHE_DIR,
        latest_dir=LATEST_DIR,
    )
    log.info("cases_logistics.csv + logistics_summary.csv written")

    # ── Push to GitHub only if files changed ────────────────────────────────
    repo = str(BASE_DIR)
    subprocess.run(["git", "add", "cache/latest/cases_logistics.csv",
                    "cache/latest/logistics_summary.csv"], cwd=repo, check=True)

    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo)
    if diff.returncode != 0:
        # Stamp _data_version.py so Streamlit Cloud redeploys
        ver_file = BASE_DIR / "_data_version.py"
        from datetime import datetime
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ver_file.write_text(
            "# Auto-generated — do not edit manually.\n"
            "# Changing this file forces Streamlit Cloud to redeploy.\n"
            f'DATA_VERSION = "{ts}"\n'
        )
        subprocess.run(["git", "add", "_data_version.py"], cwd=repo, check=True)
        subprocess.run(
            ["git", "commit", "-m", f"Logistics update {ts}"],
            cwd=repo, check=True,
        )
        subprocess.run(["git", "push", "origin", "main"], cwd=repo, check=True)
        log.info("Pushed to GitHub — Streamlit will redeploy")
    else:
        log.info("No logistics changes — skipping push")


if __name__ == "__main__":
    main()
