"""
Artistic Dental Studio — Active Accounts Fix
=============================================
Re-computes active_accounts_30d from live_exports/Active_30_day.csv (the
nightly Gmail-pulled Magic Touch tabular export with Cases_* column headers)
and patches the count back into kpi_gauges.csv so the executive dashboard
shows it.

This is a direct read of pandas-friendly columns:
    Cases_CaseNumber, Cases_CustomerID, Cases_DateIn, Cases_TotalCharge, ...

Run after pipeline.py + compute_retention_kpis.py:
    py pipeline.py
    py compute_retention_kpis.py
    py compute_active_accounts.py
"""

from __future__ import annotations

import sys
import time
import logging
from pathlib import Path

import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('retention_pipeline.log')],
)
log = logging.getLogger("active_accounts")

BASE_DIR = Path(__file__).parent
LIVE_DIR = BASE_DIR / "live_exports"          # legacy drop, no longer refreshed
LATEST_DIR = BASE_DIR / "cache" / "latest"

# Where the MagicTouch sync task actually lands exports (config.yaml watch_folder).
try:
    import yaml
    _cfg = yaml.safe_load((BASE_DIR / "config.yaml").read_text())
    WATCH_DIR = Path(str(_cfg["data_source"]["watch_folder"]))
except Exception:
    WATCH_DIR = Path(r"C:\MT_Reports_Local")


def _read_with_fallback(path: Path) -> pd.DataFrame:
    """Read CSV trying utf-8 then latin-1 (Magic Touch case exports use latin-1)."""
    last_err = None
    for enc in ('utf-8-sig', 'utf-8', 'latin-1', 'cp1252'):
        try:
            df = pd.read_csv(path, encoding=enc, low_memory=False)
            log.info("  Decoded %s as %s (%d rows, %d columns)",
                     path.name, enc, len(df), len(df.columns))
            return df
        except UnicodeDecodeError as e:
            last_err = e
            continue
    raise IOError(f"Could not decode {path}: {last_err}")


def main():
    # The live MagicTouch sync writes to C:\MT_Reports_Local (config.yaml
    # watch_folder); live_exports/ is a legacy drop that nothing refreshes any
    # more — its copy had gone two months stale (Jun 11), which is why this
    # computed "0 cases in the last 30 days" and zeroed the KPI. Prefer whichever
    # copy is actually newer so a stale leftover can't win.
    candidates = [p for p in (WATCH_DIR / "Active_30_day.csv",
                              LIVE_DIR / "Active_30_day.csv") if p.exists()]
    if not candidates:
        log.error("Active_30_day.csv not found in %s or %s — skipping",
                  WATCH_DIR, LIVE_DIR)
        sys.exit(0)
    src = max(candidates, key=lambda p: p.stat().st_mtime)
    age_h = (time.time() - src.stat().st_mtime) / 3600
    log.info("Using %s (%.1fh old)", src, age_h)
    if age_h > 24:
        log.warning("Source export is %.1f hours old — active-account numbers "
                    "may be stale.", age_h)

    log.info("Loading %s (Magic Touch Cases_* tabular format)", src.name)
    df = _read_with_fallback(src)

    # Two export layouts land in this same filename: the wide Magic Touch
    # "Cases_*" tabular dump (~104 cols) and a narrower report (~27 cols) whose
    # columns are unprefixed (CustomerID / DateIn). Accept either.
    def _col(*names):
        for n in names:
            if n in df.columns:
                return n
        return None

    cust_col = _col('Cases_CustomerID', 'CustomerID')
    date_col = _col('Cases_DateIn', 'DateIn')
    if not cust_col or not date_col:
        log.error("Could not find customer/date columns. Have: %s",
                  list(df.columns)[:20])
        sys.exit(1)
    log.info("  using columns: %s / %s", cust_col, date_col)

    # Parse dates
    df['__date_in'] = pd.to_datetime(df[date_col], errors='coerce')
    df = df.dropna(subset=['__date_in', cust_col])
    df = df[df[cust_col].astype(str).str.strip().str.len() >= 3]
    log.info("  %d rows with valid date_in + account_id", len(df))

    # Filter to last 30 days
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=30)
    df_30d = df[df['__date_in'] >= cutoff].copy()
    log.info("  %d cases in last 30 days (since %s)", len(df_30d), cutoff.date())

    # Coerce charge to numeric
    charge_col = _col('Cases_TotalCharge', 'TotalCharge')
    if charge_col:
        df_30d['__charge'] = pd.to_numeric(df_30d[charge_col],
                                           errors='coerce').fillna(0)
    else:
        df_30d['__charge'] = 0

    # Doctor name (best-effort — different reports use different fields)
    name_col = _col('Cases_DoctorName', 'Cases_DoctorFullName',
                    'Cases_PracticeName', 'DoctorName', 'Practice')
    if name_col:
        df_30d['__doctor'] = df_30d[name_col].astype(str)
    else:
        df_30d['__doctor'] = ''

    # Aggregate by account
    active = (df_30d
              .groupby(cust_col, as_index=False)
              .agg(cases=('__date_in', 'count'),
                   doctor_name=('__doctor', 'first'),
                   revenue=('__charge', 'sum'),
                   last_case=('__date_in', 'max'))
              .rename(columns={cust_col: 'account_id'})
              .sort_values('revenue', ascending=False))
    log.info("  %d unique active accounts in last 30 days", len(active))

    # SANITY GUARD — refuse to publish an empty result over good data.
    # The source export is rewritten by the sync task on its own schedule; a run
    # that reads it mid-swap (or reads a layout whose date column parses to
    # nothing) computes 0 active accounts and used to overwrite the real numbers,
    # zeroing the Executive KPI (372 -> 0) and deleting 374 rows. Stale data beats
    # wrong data: bail out and let the next cycle retry.
    gauges_path = LATEST_DIR / "kpi_gauges.csv"
    prev_count = 0
    if gauges_path.exists():
        _g = pd.read_csv(gauges_path)
        if not _g.empty:
            prev_count = int(_g.iloc[0].get('active_accounts_30d', 0) or 0)
    if len(active) == 0 and prev_count > 0:
        log.error("Computed 0 active accounts but the last good value was %d — "
                  "refusing to overwrite. Source export is likely stale or was "
                  "read mid-write; leaving existing data in place.", prev_count)
        sys.exit(1)
    if prev_count and len(active) < prev_count * 0.5:
        log.warning("Active accounts dropped sharply (%d -> %d) — writing anyway, "
                    "but check the source export.", prev_count, len(active))

    # Write the per-account CSV
    out = LATEST_DIR / "active_accounts_30d.csv"
    active.to_csv(out, index=False)
    log.info("  Wrote %s", out)

    # Patch kpi_gauges.csv so the top KPI card on the exec dashboard updates
    if gauges_path.exists():
        gauges = pd.read_csv(gauges_path)
        if not gauges.empty:
            old_count = int(gauges.iloc[0].get('active_accounts_30d', 0))
            new_count = len(active)
            gauges.loc[0, 'active_accounts_30d'] = new_count
            gauges.to_csv(gauges_path, index=False)
            log.info("  kpi_gauges.csv: active_accounts_30d %d -> %d",
                     old_count, new_count)
    else:
        log.warning("kpi_gauges.csv not found - top KPI card won't update")

    log.info("Active accounts fix complete.")


if __name__ == "__main__":
    main()
