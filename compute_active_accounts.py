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
LIVE_DIR = BASE_DIR / "live_exports"
LATEST_DIR = BASE_DIR / "cache" / "latest"


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
    src = LIVE_DIR / "Active_30_day.csv"
    if not src.exists():
        log.error("Active_30_day.csv not found at %s — skipping", src)
        sys.exit(0)

    log.info("Loading %s (Magic Touch Cases_* tabular format)", src.name)
    df = _read_with_fallback(src)

    # Required columns
    required = {'Cases_CustomerID', 'Cases_DateIn'}
    missing = required - set(df.columns)
    if missing:
        log.error("Missing required columns: %s", missing)
        log.error("Got columns starting with 'Cases_': %s",
                  [c for c in df.columns if c.startswith('Cases_')][:10])
        sys.exit(1)

    # Parse dates
    df['__date_in'] = pd.to_datetime(df['Cases_DateIn'], errors='coerce')
    df = df.dropna(subset=['__date_in', 'Cases_CustomerID'])
    df = df[df['Cases_CustomerID'].astype(str).str.strip().str.len() >= 3]
    log.info("  %d rows with valid date_in + account_id", len(df))

    # Filter to last 30 days
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=30)
    df_30d = df[df['__date_in'] >= cutoff].copy()
    log.info("  %d cases in last 30 days (since %s)", len(df_30d), cutoff.date())

    # Coerce charge to numeric
    if 'Cases_TotalCharge' in df_30d.columns:
        df_30d['__charge'] = pd.to_numeric(df_30d['Cases_TotalCharge'],
                                           errors='coerce').fillna(0)
    else:
        df_30d['__charge'] = 0

    # Doctor name (best-effort — different reports use different fields)
    name_col = None
    for c in ('Cases_DoctorName', 'Cases_DoctorFullName', 'Cases_PracticeName'):
        if c in df_30d.columns:
            name_col = c
            break
    if name_col:
        df_30d['__doctor'] = df_30d[name_col].astype(str)
    else:
        df_30d['__doctor'] = ''

    # Aggregate by account
    active = (df_30d
              .groupby('Cases_CustomerID', as_index=False)
              .agg(cases=('__date_in', 'count'),
                   doctor_name=('__doctor', 'first'),
                   revenue=('__charge', 'sum'),
                   last_case=('__date_in', 'max'))
              .rename(columns={'Cases_CustomerID': 'account_id'})
              .sort_values('revenue', ascending=False))
    log.info("  %d unique active accounts in last 30 days", len(active))

    # Write the per-account CSV
    out = LATEST_DIR / "active_accounts_30d.csv"
    active.to_csv(out, index=False)
    log.info("  Wrote %s", out)

    # Patch kpi_gauges.csv so the top KPI card on the exec dashboard updates
    gauges_path = LATEST_DIR / "kpi_gauges.csv"
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
