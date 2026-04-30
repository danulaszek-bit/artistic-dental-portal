"""
Artistic Dental Studio — Automated Data Pipeline
=================================================
Phase 1 : Reads CSV / Excel exports from Magic Touch & QuickBooks
Phase 2 : Swap DATA_SOURCE to "odbc" in config.yaml once Magic Touch
          confirms ODBC access — no other code changes needed.

Run manually   : python pipeline.py
Run as service : python pipeline.py --daemon   (loops on schedule)
"""

import os
import sys
import pickle
import logging
import argparse
import hashlib
from datetime import datetime, date
from pathlib import Path

import yaml
import schedule
import time
import pandas as pd
import numpy as np

# Google Drive
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ],
)
log = logging.getLogger("dental_pipeline")

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
with open(BASE_DIR / "config.yaml") as f:
    CFG = yaml.safe_load(f)

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
CACHE_DIR = BASE_DIR / CFG["output"]["local_cache_dir"]
CACHE_DIR.mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  DATA INGESTION
# ══════════════════════════════════════════════════════════════════════════════

def load_data() -> dict[str, pd.DataFrame]:
    """
    Route to the correct loader based on config.yaml  data_source.mode.
    Returns a dict of DataFrames keyed by table name.
    """
    mode = CFG["data_source"]["mode"]
    if mode == "csv":
        return _load_from_csv()
    elif mode == "odbc":
        return _load_from_odbc()
    else:
        raise ValueError(f"Unknown data_source.mode: {mode}")


# ── Phase 1: CSV / Excel ───────────────────────────────────────────────────────

def _load_from_csv() -> dict[str, pd.DataFrame]:
    """Read Magic Touch CSV exports + QuickBooks Excel file."""
    src = CFG["data_source"]["csv"]
    folder = Path(src["watch_folder"])
    enc = src.get("encoding", "utf-8-sig")

    log.info("Loading from CSV/Excel exports in %s", folder)

    def read_csv(filename, **kwargs):
        path = folder / filename
        if not path.exists():
            log.warning("File not found: %s — using empty frame", path)
            return pd.DataFrame()
        return pd.read_csv(path, encoding=enc, **kwargs)

    # ── Orders (core production table) ──────────────────────────────────────
    orders = read_csv(src["orders_file"], parse_dates=["order_date", "ship_date"])
    orders = _clean_orders(orders)

    # ── Accounts (practice / doctor info) ────────────────────────────────────
    accounts = read_csv(src["accounts_file"])
    accounts = _clean_accounts(accounts)

    # ── Implant pipeline (cases in progress) ─────────────────────────────────
    implants = read_csv(src["implants_file"], parse_dates=["rx_date", "due_date"])
    implants = _clean_implants(implants)

    # ── QuickBooks financials (monthly P&L) ───────────────────────────────────
    qb_path = folder / src["quickbooks_file"]
    if qb_path.exists():
        qb = pd.read_excel(qb_path, sheet_name="P&L", parse_dates=["period"])
        qb = _clean_quickbooks(qb)
    else:
        log.warning("QuickBooks file not found — using empty frame")
        qb = pd.DataFrame()

    return {"orders": orders, "accounts": accounts, "implants": implants, "financials": qb}


def _clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise orders table regardless of source."""
    if df.empty:
        return df
    required = {"order_id", "account_id", "product_type", "revenue", "cogs",
                 "order_date", "status"}
    missing = required - set(df.columns)
    if missing:
        log.warning("Orders missing columns: %s", missing)

    df["revenue"] = pd.to_numeric(df.get("revenue", 0), errors="coerce").fillna(0)
    df["cogs"] = pd.to_numeric(df.get("cogs", 0), errors="coerce").fillna(0)
    df["gross_profit"] = df["revenue"] - df["cogs"]
    df["margin_pct"] = np.where(df["revenue"] > 0,
                                df["gross_profit"] / df["revenue"] * 100, 0)
    df["is_remake"] = df.get("status", "").str.lower().str.contains("remake", na=False)
    df["year"] = pd.to_datetime(df.get("order_date"), errors="coerce").dt.year
    df["month"] = pd.to_datetime(df.get("order_date"), errors="coerce").dt.month
    df["yearmonth"] = pd.to_datetime(df.get("order_date"), errors="coerce").dt.to_period("M")
    return df


def _clean_accounts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _clean_implants(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["days_in_progress"] = (
        pd.Timestamp.today() - pd.to_datetime(df.get("rx_date"), errors="coerce")
    ).dt.days
    df["overdue"] = df["days_in_progress"] > CFG["targets"]["implant_turnaround_days"]
    return df


def _clean_quickbooks(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


# ── Phase 2: ODBC (swap in when Magic Touch confirms) ─────────────────────────

def _load_from_odbc() -> dict[str, pd.DataFrame]:
    """
    Direct database connection via ODBC.
    Requires pyodbc installed and a DSN configured in Windows ODBC Data Sources.
    """
    try:
        import pyodbc
    except ImportError:
        log.error("pyodbc not installed. Run: pip install pyodbc")
        sys.exit(1)

    src = CFG["data_source"]["odbc"]
    conn_str = (
        f"DSN={src['dsn']};"
        if src.get("dsn")
        else (
            f"DRIVER={src['driver']};"
            f"SERVER={src['server']};"
            f"DATABASE={src['database']};"
            f"Trusted_Connection={src.get('trusted_connection','yes')};"
        )
    )
    log.info("Connecting via ODBC to %s", src.get("dsn") or src.get("server"))
    conn = pyodbc.connect(conn_str, timeout=10)

    # ── Adjust table/column names to match Magic Touch schema ───────────────
    queries = {
        "orders": """
            SELECT OrderID       AS order_id,
                   AccountID     AS account_id,
                   ProductType   AS product_type,
                   Revenue       AS revenue,
                   COGS          AS cogs,
                   OrderDate     AS order_date,
                   ShipDate      AS ship_date,
                   Status        AS status
            FROM   dbo.Orders
            WHERE  OrderDate >= DATEADD(year, -2, GETDATE())
        """,
        "accounts": """
            SELECT AccountID     AS account_id,
                   AccountName   AS account_name,
                   DoctorName    AS doctor_name,
                   Phone         AS phone
            FROM   dbo.Accounts
        """,
        "implants": """
            SELECT CaseID        AS case_id,
                   AccountID     AS account_id,
                   RxDate        AS rx_date,
                   DueDate       AS due_date,
                   Stage         AS stage,
                   ProductType   AS product_type
            FROM   dbo.ImplantPipeline
            WHERE  Status = 'InProgress'
        """,
    }

    tables = {}
    for name, sql in queries.items():
        log.info("Querying %s ...", name)
        tables[name] = pd.read_sql(sql, conn)

    conn.close()

    tables["orders"] = _clean_orders(tables["orders"])
    tables["accounts"] = _clean_accounts(tables["accounts"])
    tables["implants"] = _clean_implants(tables["implants"])
    tables["financials"] = pd.DataFrame()   # add QB ODBC query if needed
    return tables


# ══════════════════════════════════════════════════════════════════════════════
#  ANALYTICS / KPI COMPUTATION
# ══════════════════════════════════════════════════════════════════════════════

def compute_kpis(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Build all analytics tables used by the dashboard.
    All analysis logic lives here — the dashboard just reads these outputs.
    """
    orders = tables.get("orders", pd.DataFrame())
    accounts = tables.get("accounts", pd.DataFrame())
    implants = tables.get("implants", pd.DataFrame())
    financials = tables.get("financials", pd.DataFrame())

    kpis = {}

    if orders.empty:
        log.warning("Orders table is empty — KPIs will be blank")
        return kpis

    # ── 1. Profitability by account ────────────────────────────────────────
    prof = (
        orders.groupby("account_id")
        .agg(
            revenue=("revenue", "sum"),
            cogs=("cogs", "sum"),
            orders=("order_id", "count"),
        )
        .assign(gross_profit=lambda d: d.revenue - d.cogs,
                margin_pct=lambda d: (d.gross_profit / d.revenue * 100).round(1))
        .sort_values("gross_profit", ascending=False)
        .reset_index()
    )
    if not accounts.empty and "account_id" in accounts.columns:
        prof = prof.merge(
            accounts[["account_id", "account_name"]].drop_duplicates("account_id"),
            on="account_id", how="left"
        )
    kpis["profitability"] = prof

    # ── 2. Remake trends (monthly rate) ───────────────────────────────────
    remake = (
        orders.groupby("yearmonth")
        .agg(total=("order_id", "count"), remakes=("is_remake", "sum"))
        .assign(remake_rate=lambda d: (d.remakes / d.total * 100).round(2))
        .reset_index()
    )
    remake["yearmonth"] = remake["yearmonth"].astype(str)
    kpis["remake_trends"] = remake

    # ── 3. Top-20% accounts (Pareto) ──────────────────────────────────────
    total_rev = prof["revenue"].sum()
    prof_sorted = prof.sort_values("revenue", ascending=False).copy()
    prof_sorted["cum_revenue"] = prof_sorted["revenue"].cumsum()
    prof_sorted["cum_pct"] = prof_sorted["cum_revenue"] / total_rev
    threshold = CFG["targets"]["pareto_threshold"]
    kpis["pareto_accounts"] = prof_sorted[prof_sorted["cum_pct"].shift(1, fill_value=0) < threshold]

    # ── 4. YTD vs prior-year + growth target ──────────────────────────────
    current_year = date.today().year
    prior_year = current_year - 1
    ytd_month = date.today().month

    ytd_current = orders[
        (orders["year"] == current_year) & (orders["month"] <= ytd_month)
    ]["revenue"].sum()

    ytd_prior = orders[
        (orders["year"] == prior_year) & (orders["month"] <= ytd_month)
    ]["revenue"].sum()

    growth_target = CFG["targets"]["annual_growth_pct"]
    ytd_target = ytd_prior * (1 + growth_target / 100)
    actual_growth = ((ytd_current - ytd_prior) / ytd_prior * 100) if ytd_prior else 0

    kpis["ytd_summary"] = pd.DataFrame([{
        "current_year": current_year,
        "ytd_current": ytd_current,
        "ytd_prior": ytd_prior,
        "ytd_target": ytd_target,
        "target_growth_pct": growth_target,
        "actual_growth_pct": round(actual_growth, 2),
        "on_track": ytd_current >= ytd_target,
    }])

    # Monthly trend for sparkline
    monthly = (
        orders[orders["year"].isin([prior_year, current_year])]
        .groupby(["year", "month"])["revenue"]
        .sum()
        .reset_index()
    )
    kpis["monthly_revenue"] = monthly

    # ── 5. Implant pipeline ────────────────────────────────────────────────
    kpis["implant_pipeline"] = implants if not implants.empty else pd.DataFrame()

    # ── 6. Top-level KPI gauges ───────────────────────────────────────────
    remake_rate = remake["remake_rate"].iloc[-1] if not remake.empty else 0
    kpis["kpi_gauges"] = pd.DataFrame([{
        "ytd_revenue": ytd_current,
        "ytd_prior_revenue": ytd_prior,
        "actual_growth_pct": actual_growth,
        "target_growth_pct": growth_target,
        "remake_rate": remake_rate,
        "remake_alert_pct": CFG["targets"]["remake_alert_pct"],
        "avg_margin_pct": round(prof["margin_pct"].mean(), 1),
        "implant_overdue": int(implants["overdue"].sum()) if not implants.empty else 0,
        "total_implants_open": len(implants),
        "pareto_account_count": len(kpis["pareto_accounts"]),
        "total_account_count": len(prof),
    }])

    return kpis


# ══════════════════════════════════════════════════════════════════════════════
#  GOOGLE DRIVE UPLOAD
# ══════════════════════════════════════════════════════════════════════════════

def get_drive_service():
    """Authenticate and return a Google Drive API service object."""
    creds = None
    token_path = BASE_DIR / CFG["google_drive"]["token_file"]
    creds_path = BASE_DIR / CFG["google_drive"]["credentials_file"]

    if token_path.exists():
        with open(token_path, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "wb") as f:
            pickle.dump(creds, f)

    return build("drive", "v3", credentials=creds)


def upload_to_drive(service, local_path: Path, drive_folder_id: str) -> str:
    """Upload or overwrite a file in Google Drive. Returns the file ID."""
    filename = local_path.name

    # Check if file already exists (to update rather than duplicate)
    query = (f"name='{filename}' and '{drive_folder_id}' in parents "
             f"and trashed=false")
    results = service.files().list(q=query, fields="files(id,name)").execute()
    existing = results.get("files", [])

    media = MediaFileUpload(str(local_path), resumable=True)
    if existing:
        file_id = existing[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        log.info("Updated  Drive file: %s  (id=%s)", filename, file_id)
    else:
        meta = {"name": filename, "parents": [drive_folder_id]}
        res = service.files().create(body=meta, media_body=media,
                                     fields="id").execute()
        file_id = res["id"]
        log.info("Uploaded Drive file: %s  (id=%s)", filename, file_id)
    return file_id


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN PIPELINE JOB
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    log.info("=" * 60)
    log.info("Pipeline run started  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 1. Ingest
    tables = load_data()

    # 2. Compute KPIs
    kpis = compute_kpis(tables)

    # 3. Save locally as Parquet (fast, compact, type-safe)
    today_str = date.today().strftime("%Y-%m-%d")
    snapshot_name = CFG["output"]["processed_filename"].format(date=today_str)
    snapshot_path = CACHE_DIR / snapshot_name

    with pd.ExcelWriter(str(snapshot_path).replace(".parquet", ".xlsx")) as writer:
        for name, df in {**tables, **kpis}.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, sheet_name=name[:31], index=False)

    # Also save the "latest" file that the dashboard reads
    latest_path = CACHE_DIR / "latest_kpis.xlsx"
    with pd.ExcelWriter(str(latest_path)) as writer:
        for name, df in kpis.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_excel(writer, sheet_name=name[:31], index=False)

    log.info("Saved local snapshot: %s", snapshot_path)

    # 4. Upload both files to Google Drive
    try:
        service = get_drive_service()
        folder_id = CFG["google_drive"]["folder_id"]
        upload_to_drive(service, latest_path, folder_id)
        upload_to_drive(service, snapshot_path.with_suffix(".xlsx"), folder_id)
        log.info("Google Drive upload complete.")
    except Exception as exc:
        log.error("Drive upload failed (data still saved locally): %s", exc)

    log.info("Pipeline run complete.")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Artistic Dental Pipeline")
    parser.add_argument("--daemon", action="store_true",
                        help="Run on schedule (daemon mode)")
    parser.add_argument("--now", action="store_true",
                        help="Run once immediately (default)")
    args = parser.parse_args()

    if args.daemon:
        run_time = CFG["schedule"]["run_time"]
        log.info("Daemon mode: scheduled daily at %s", run_time)
        schedule.every().day.at(run_time).do(run_pipeline)
        run_pipeline()   # run once immediately on start
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_pipeline()


if __name__ == "__main__":
    main()
