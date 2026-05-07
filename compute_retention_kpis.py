"""
Artistic Dental Studio — Retention KPI Computer
================================================
Reads cache/case_history.csv + every retention-period file in the configured
folder, computes per-period status, and writes three CSVs to cache/latest/:

    retention_periods.csv   — one row per period (id, title, year, totals, %)
    retention_doctors.csv   — one row per (period × doctor) with status + flags
    retention_master.csv    — one row per unique doctor, latest period status

Run after pipeline.py:
    py pipeline.py
    py compute_retention_kpis.py

Or chain in git_push.bat:
    py pipeline.py
    py compute_retention_kpis.py
    git_push.bat
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path

import yaml
import retention as ret


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('retention_pipeline.log'),
    ],
)
log = logging.getLogger("retention_kpis")

BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "cache"
LATEST_DIR = CACHE_DIR / "latest"
LATEST_DIR.mkdir(parents=True, exist_ok=True)

# Where retention-period XLS/XLSX/CSV files live.
# Override by adding `retention_folder: "C:/path"` to config.yaml under data_source.
DEFAULT_RETENTION_FOLDER = BASE_DIR / "retention_periods"


def main():
    # Optional config override
    cfg_path = BASE_DIR / "config.yaml"
    ret_folder = DEFAULT_RETENTION_FOLDER
    if cfg_path.exists():
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        custom = (cfg.get("data_source", {}).get("retention_folder")
                  or cfg.get("retention_folder"))
        if custom:
            ret_folder = Path(custom)

    if not ret_folder.exists():
        log.error("Retention folder not found: %s", ret_folder)
        log.error("Create the folder and drop your retention XLS/XLSX/CSV files in it,")
        log.error("or set retention_folder: in config.yaml")
        sys.exit(1)

    case_path = CACHE_DIR / "case_history.csv"
    if not case_path.exists():
        log.warning("case_history.csv not found — retention will compute from XLS")
        log.warning("checkmarks only. Run import_case_history.py first for full math.")

    log.info("=" * 60)
    log.info("Retention KPI computation starting")

    log.info("Loading retention periods from %s", ret_folder)
    periods = ret.load_all_periods(ret_folder)
    log.info("  Loaded %d periods", len(periods))

    log.info("Loading case history from %s", case_path)
    cases_df = ret.load_case_history(case_path)
    log.info("  Loaded %d cases (%d unique doctors)",
             len(cases_df),
             cases_df["account_id"].nunique() if not cases_df.empty else 0)

    log.info("Computing retention status...")
    periods_df, doctors_df, master_df = ret.compute_retention(periods, cases_df)

    # Write outputs
    activity_df = ret.compute_doctor_activity(cases_df)
    out_files = {
        "retention_periods.csv": periods_df,
        "retention_doctors.csv": doctors_df,
        "retention_master.csv":  master_df,
        "doctor_activity.csv":   activity_df,
    }
    for name, df in out_files.items():
        path = LATEST_DIR / name
        df.to_csv(path, index=False)
        log.info("  Wrote %s (%d rows)", path, len(df))

    log.info("=" * 60)
    log.info("Retention KPI computation complete.")
    log.info("Periods: %d | Doctor-period rows: %d | Master doctors: %d",
             len(periods_df), len(doctors_df), len(master_df))

    if not periods_df.empty:
        log.info("Quick summary:")
        for _, p in periods_df.iterrows():
            log.info("  %s  %s  %d tracked  ->  %d retained / %d at_risk / %d lost  (%.1f%%)",
                     p["period_id"], p["m3_label"], p["total"],
                     p["retained"], p["at_risk"], p["lost"], p["retained_pct"])


if __name__ == "__main__":
    main()
