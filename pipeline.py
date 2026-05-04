"""
Artistic Dental Studio — Automated Data Pipeline
=================================================
Built around Magic Touch Sales_Data.csv export format.
Pre-aggregated by customer + product with YTD, LY, LTD, quarterly breakdowns.

Run manually   : python pipeline.py
Run as service : python pipeline.py --daemon
"""

import sys
import pickle
import logging
import argparse
from datetime import datetime, date
from pathlib import Path

import yaml
import schedule
import time
import pandas as pd
import numpy as np

# Google Drive
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

# ── Gmail attachment downloader ────────────────────────────────────────────────
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/gmail.readonly",
]

def download_gmail_attachments(dest_folder: Path) -> list[str]:
    """
    Check the MagicTouchExports Gmail label and download any CSV attachments
    to dest_folder. Returns list of downloaded filenames.
    """
    import base64
    from googleapiclient.discovery import build as gbuild

    token_path  = BASE_DIR / CFG["google_drive"]["token_file"]
    creds_path  = BASE_DIR / CFG["google_drive"]["credentials_file"]

    creds = None
    if token_path.exists():
        with open(token_path, "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "wb") as f:
            pickle.dump(creds, f)

    gmail = gbuild("gmail", "v1", credentials=creds)
    downloaded = []

    # Search for unread emails in MagicTouchExports label from last 24 hours
    query = "label:MagicTouchExports has:attachment newer_than:1d"
    results = gmail.users().messages().list(userId="me", q=query).execute()
    messages = results.get("messages", [])

    if not messages:
        log.info("Gmail: no new Magic Touch exports found")
        return downloaded

    log.info("Gmail: found %d new export email(s)", len(messages))

    for msg_ref in messages:
        msg = gmail.users().messages().get(
            userId="me", id=msg_ref["id"], format="full"
        ).execute()

        subject = next(
            (h["value"] for h in msg["payload"]["headers"] if h["name"] == "Subject"),
            "Unknown"
        )

        parts = msg.get("payload", {}).get("parts", [])
        for part in parts:
            filename = part.get("filename", "")
            if not filename or not filename.lower().endswith(".csv"):
                continue

            att_id = part["body"].get("attachmentId")
            if not att_id:
                continue

            att = gmail.users().messages().attachments().get(
                userId="me", messageId=msg_ref["id"], id=att_id
            ).execute()

            data = base64.urlsafe_b64decode(att["data"])

            # Map Magic Touch report names to our standard filenames
            save_name = _map_report_filename(filename, subject)
            save_path = dest_folder / save_name

            with open(save_path, "wb") as f:
                f.write(data)

            log.info("Gmail: downloaded %s → %s", filename, save_name)
            downloaded.append(save_name)

    return downloaded


def _map_report_filename(filename: str, subject: str) -> str:
    """Map Magic Touch report filenames to our standard names."""
    fname = filename.lower()
    subj  = subject.lower()

    if "allcasesbydate" in fname or "allcasesbydate" in subj:
        return "Active_30_day.csv"
    if "salesdata" in fname or "sales" in subj:
        return "Sales_Data.csv"
    if "remake" in fname or "remake" in subj:
        return "Remakes.csv"
    if "wip" in fname or "wip" in subj or "opencase" in fname:
        return "WIP.csv"

    # Default — keep original name
    return filename


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

SCOPES = GMAIL_SCOPES
CACHE_DIR = BASE_DIR / CFG["output"]["local_cache_dir"]
CACHE_DIR.mkdir(exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
#  DATA INGESTION — Magic Touch Sales_Data.csv
# ══════════════════════════════════════════════════════════════════════════════

def load_data() -> dict[str, pd.DataFrame]:
    mode = CFG["data_source"]["mode"]
    if mode == "csv":
        return _load_from_csv()
    elif mode == "odbc":
        return _load_from_odbc()
    else:
        raise ValueError(f"Unknown data_source.mode: {mode}")


def _load_from_csv() -> dict[str, pd.DataFrame]:
    """
    Load Magic Touch Sales_Data.csv — pre-aggregated by customer+product.
    Columns include YTD, LY, LTD, MTD, quarterly sales + remakes.
    """
    src = CFG["data_source"]["csv"]
    folder = Path(src["watch_folder"])
    enc = src.get("encoding", "utf-8-sig")

    log.info("Loading Magic Touch data from %s", folder)

    sales_file = src.get("sales_file", "Sales_Data.csv")
    sales_path = folder / sales_file

    if not sales_path.exists():
        log.warning("File not found: %s", sales_path)
        return {"sales": pd.DataFrame()}

    df = pd.read_csv(sales_path, encoding=enc)
    log.info("Raw file: %d rows loaded", len(df))
    df = _clean_sales(df)
    log.info("Clean: %d rows, %d unique customers",
             len(df), df["account_id"].nunique())

    tables = {"sales": df}
    tables.update(_load_case_files(folder))
    return tables


def _load_case_files(folder: Path) -> dict[str, pd.DataFrame]:
    """Load Active_30_day.csv and Remakes.csv from Magic Touch Advanced Export."""
    enc = "latin-1"
    tables = {}

    # Active cases (last 30 days) — also used for WIP
    active_path = folder / "Active_30_day.csv"
    if active_path.exists():
        df = pd.read_csv(active_path, encoding=enc, low_memory=False)
        df = _clean_cases(df)
        tables["active_cases"] = df
        log.info("Active cases: %d rows", len(df))
    else:
        log.warning("Active_30_day.csv not found")
        tables["active_cases"] = pd.DataFrame()

    # Remakes (last 30 days)
    remake_path = folder / "Remakes.csv"
    if remake_path.exists():
        df = pd.read_csv(remake_path, encoding=enc, low_memory=False)
        df = _clean_cases(df)
        tables["remakes"] = df
        log.info("Remakes: %d rows", len(df))
    else:
        log.warning("Remakes.csv not found")
        tables["remakes"] = pd.DataFrame()

    # WIP — dedicated export from Magic Touch
    wip_path = folder / "WIP.csv"
    if wip_path.exists():
        df = pd.read_csv(wip_path, encoding="utf-8-sig", low_memory=False)
        df = _clean_cases(df)
        # Filter to true open WIP statuses only
        open_statuses = ["In Production", "On Hold", "Submitted",
                         "Outsourced", "Sent for TryIn"]
        df = df[df["status"].isin(open_statuses)].copy()
        tables["wip"] = df
        log.info("WIP: %d open cases, $%.0f total value",
                 len(df), df["total_charge"].sum() if "total_charge" in df.columns else 0)
    else:
        # Derive WIP from active cases if WIP.csv not available
        if not tables["active_cases"].empty:
            invoiced = ["Invoiced", "Invoiced TryIn", "Cancelled"]
            wip = tables["active_cases"][
                ~tables["active_cases"]["status"].isin(invoiced)
            ].copy()
            tables["wip"] = wip
            log.info("WIP (derived): %d open cases", len(wip))
        else:
            tables["wip"] = pd.DataFrame()

    return tables


def _clean_cases(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise Magic Touch case export columns."""
    if df.empty:
        return df
    rename = {
        "Cases_CaseNumber":    "case_number",
        "Cases_CustomerID":    "account_id",
        "Cases_DateIn":        "date_in",
        "Cases_DueDate":       "due_date",
        "Cases_Status":        "status",
        "Cases_TotalCharge":   "total_charge",
        "Cases_DoctorName":    "doctor_name",
        "Cases_Remake":        "remake_reason_code",
        "Cases_RemakeReason":  "remake_reason",
        "Cases_Rush":          "rush",
        "Cases_SalesPerson":   "sales_person",
        "Cases_Department":    "department",
        "Cases_LabName":       "lab_name",
        "Cases_PanNumber":     "pan_number",
        "Cases_PatientFullName": "patient_name",
        "Cases_ShipDate":      "ship_date",
        "Cases_InvoiceDate":   "invoice_date",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    if "total_charge" in df.columns:
        df["total_charge"] = pd.to_numeric(df["total_charge"], errors="coerce").fillna(0)
    if "date_in" in df.columns:
        df["date_in"] = pd.to_datetime(df["date_in"], errors="coerce")
    if "due_date" in df.columns:
        df["due_date"] = pd.to_datetime(df["due_date"], errors="coerce")
    if "due_date" in df.columns and "date_in" in df.columns:
        today = pd.Timestamp.today()
        df["days_in_lab"] = (today - df["date_in"]).dt.days
        df["overdue"] = df["due_date"] < today
    return df


def _clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Rename Magic Touch columns and coerce numerics."""
    if df.empty:
        return df

    rename = {
        "SalesData_CustomerID":   "account_id",
        "SalesData_ProductGroup": "product_group",
        "SalesData_ProductID":    "product_id",
        "SalesData_ProductType":  "product_type",
        "SalesData_Department":   "department",
        "SalesData_YTDSales":     "ytd_sales",
        "SalesData_LYSales":      "ly_sales",
        "SalesData_LTDSales":     "ltd_sales",
        "SalesData_MTDSales":     "mtd_sales",
        "SalesData_LMSales":      "lm_sales",
        "SalesData_Q1Sales":      "q1_sales",
        "SalesData_Q2Sales":      "q2_sales",
        "SalesData_Q3Sales":      "q3_sales",
        "SalesData_Q4Sales":      "q4_sales",
        "SalesData_YTDRemake":    "ytd_remake",
        "SalesData_LYRemake":     "ly_remake",
        "SalesData_LTDRemake":    "ltd_remake",
        "SalesData_MTDRemake":    "mtd_remake",
        "SalesData_LMRemake":     "lm_remake",
        "SalesData_Q1Remake":     "q1_remake",
        "SalesData_Q2Remake":     "q2_remake",
        "SalesData_Q3Remake":     "q3_remake",
        "SalesData_Q4Remake":     "q4_remake",
        "SalesData_YTDCredit":    "ytd_credit",
        "SalesData_LYCredit":     "ly_credit",
        "SalesData_LTDCredit":    "ltd_credit",
        "SalesData_MTDCredit":    "mtd_credit",
        "SalesData_LMCredit":     "lm_credit",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    num_cols = [c for c in df.columns if any(
        c.startswith(p) for p in ["ytd_", "ly_", "ltd_", "mtd_", "lm_",
                                   "q1_", "q2_", "q3_", "q4_"])]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["is_implant"] = df["department"].str.upper().str.contains(
        "IMPLANT", na=False)

    return df


# ── Phase 2: ODBC ─────────────────────────────────────────────────────────────
def _load_from_odbc() -> dict[str, pd.DataFrame]:
    try:
        import pyodbc
    except ImportError:
        log.error("pyodbc not installed. Run: pip install pyodbc")
        sys.exit(1)

    src = CFG["data_source"]["odbc"]
    conn_str = (f"DSN={src['dsn']};" if src.get("dsn") else
                f"DRIVER={src['driver']};SERVER={src['server']};"
                f"DATABASE={src['database']};"
                f"Trusted_Connection={src.get('trusted_connection','yes')};")
    log.info("Connecting via ODBC ...")
    conn = pyodbc.connect(conn_str, timeout=10)
    sql = """
        SELECT CustomerID AS account_id, ProductType AS product_type,
               Department AS department,
               YTDSales AS ytd_sales, LYSales AS ly_sales,
               LTDSales AS ltd_sales, MTDSales AS mtd_sales,
               Q1Sales AS q1_sales, Q2Sales AS q2_sales,
               Q3Sales AS q3_sales, Q4Sales AS q4_sales,
               YTDRemake AS ytd_remake, LYRemake AS ly_remake,
               Q1Remake AS q1_remake, Q2Remake AS q2_remake,
               Q3Remake AS q3_remake, Q4Remake AS q4_remake
        FROM   dbo.SalesData
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return {"sales": _clean_sales(df)}


# ══════════════════════════════════════════════════════════════════════════════
#  KPI COMPUTATION
# ══════════════════════════════════════════════════════════════════════════════

def compute_kpis(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    df = tables.get("sales", pd.DataFrame())
    kpis = {}

    if df.empty:
        log.warning("Sales table empty — all KPIs will be blank")
        return kpis

    # ── 1. Profitability ranking by account ───────────────────────────────
    prof = (
        df.groupby("account_id")
        .agg(
            ltd_sales=("ltd_sales",   "sum"),
            ytd_sales=("ytd_sales",   "sum"),
            ly_sales=("ly_sales",     "sum"),
            ytd_remake=("ytd_remake", "sum"),
            product_lines=("product_type", "nunique"),
        )
        .reset_index()
        .sort_values("ltd_sales", ascending=False)
    )
    prof["yoy_growth_pct"] = np.where(
        prof["ly_sales"] > 0,
        (prof["ytd_sales"] - prof["ly_sales"]) / prof["ly_sales"] * 100,
        0
    ).round(1)
    prof["remake_rate_pct"] = np.where(
        prof["ytd_sales"] > 0,
        prof["ytd_remake"] / prof["ytd_sales"] * 100,
        0
    ).round(2)
    kpis["profitability"] = prof

    # ── 2. Pareto — active accounts only (ytd or ly sales > 0) ───────────
    active_prof = prof[(prof["ytd_sales"] > 0) | (prof["ly_sales"] > 0)].copy()
    log.info("Active accounts (ytd or ly sales > 0): %d of %d total",
             len(active_prof), len(prof))

    total_ltd = active_prof["ltd_sales"].sum()
    prof_sorted = active_prof.sort_values("ltd_sales", ascending=False).copy()
    prof_sorted["cum_ltd"] = prof_sorted["ltd_sales"].cumsum()
    prof_sorted["cum_pct"] = prof_sorted["cum_ltd"] / total_ltd
    threshold = CFG["targets"]["pareto_threshold"]
    kpis["pareto_accounts"] = prof_sorted[
        prof_sorted["cum_pct"].shift(1, fill_value=0) < threshold
    ].copy()

    # ── 3. YTD vs prior year + 7% growth target ───────────────────────────
    ytd_total = df["ytd_sales"].sum()
    ly_total  = df["ly_sales"].sum()
    ltd_total = df["ltd_sales"].sum()
    mtd_sales = df["mtd_sales"].sum()
    ytd_remake = df["ytd_remake"].sum()
    mtd_remake = df["mtd_remake"].sum()

    growth_target = CFG["targets"]["annual_growth_pct"]
    # Prorate LY to same number of months for a fair YTD comparison
    # (LY column = full prior year; YTD = current year to date)
    current_month  = date.today().month
    ly_prorated    = ly_total * (current_month / 12)
    ytd_target     = ly_prorated * (1 + growth_target / 100)
    actual_growth  = ((ytd_total - ly_prorated) / ly_prorated * 100) if ly_prorated else 0

    kpis["ytd_summary"] = pd.DataFrame([{
        "current_year":      date.today().year,
        "ytd_current":       ytd_total,
        "ytd_prior":         ly_prorated,    # prorated to same months
        "ly_full_year":      ly_total,
        "ltd_total":         ltd_total,
        "ytd_target":        ytd_target,
        "target_growth_pct": growth_target,
        "actual_growth_pct": round(actual_growth, 2),
        "on_track":          ytd_total >= ytd_target,
        "months_elapsed":    current_month,
    }])

    # ── 4. Quarterly trend ────────────────────────────────────────────────
    quarters = []
    for q in ["q1", "q2", "q3", "q4"]:
        cy_sales  = df[f"{q}_sales"].sum()
        cy_remake = df[f"{q}_remake"].sum()
        ly_approx = ly_total / 4
        quarters.append({
            "quarter":       q.upper(),
            "cy_sales":      cy_sales,
            "ly_approx":     ly_approx,
            "target":        ly_approx * (1 + growth_target / 100),
            "remake_amount": cy_remake,
            "remake_rate":   round(cy_remake / cy_sales * 100, 2) if cy_sales else 0,
        })
    kpis["quarterly_trend"] = pd.DataFrame(quarters)

    # ── 5. Remake trends ──────────────────────────────────────────────────
    remake_rows = []
    for q in ["q1", "q2", "q3", "q4"]:
        s = df[f"{q}_sales"].sum()
        r = df[f"{q}_remake"].sum()
        remake_rows.append({
            "yearmonth":   q.upper(),
            "total":       s,
            "remakes":     r,
            "remake_rate": round(r / s * 100, 2) if s > 0 else 0,
        })
    remake_rows.append({
        "yearmonth":   "YTD",
        "total":       ytd_total,
        "remakes":     ytd_remake,
        "remake_rate": round(ytd_remake / ytd_total * 100, 2) if ytd_total else 0,
    })
    kpis["remake_trends"] = pd.DataFrame(remake_rows)

    # ── 6. Implant summary ────────────────────────────────────────────────
    implants = df[df["is_implant"]].copy()
    impl_summary = (
        implants.groupby("account_id")
        .agg(
            ytd_implant_sales=("ytd_sales",   "sum"),
            ly_implant_sales=("ly_sales",     "sum"),
            ytd_implant_remakes=("ytd_remake", "sum"),
        )
        .reset_index()
        .sort_values("ytd_implant_sales", ascending=False)
    )
    kpis["implant_pipeline"] = impl_summary

    # ── 7. Department mix ─────────────────────────────────────────────────
    dept = (
        df.groupby("department")
        .agg(ytd_sales=("ytd_sales", "sum"), ly_sales=("ly_sales", "sum"))
        .reset_index()
        .sort_values("ytd_sales", ascending=False)
    )
    dept = dept[dept["department"].notna() & (dept["ytd_sales"] > 0)]
    kpis["department_mix"] = dept

    # ── 7b. WIP Summary ──────────────────────────────────────
    wip = tables.get("wip", pd.DataFrame())
    if not wip.empty and "total_charge" in wip.columns:
        wip_value = wip["total_charge"].sum()
        wip_count = len(wip)
        wip_overdue = int(wip["overdue"].sum()) if "overdue" in wip.columns else 0
        wip_by_status = wip.groupby("status")["total_charge"].agg(["sum","count"]).reset_index()
        wip_by_status.columns = ["status","value","count"]
        kpis["wip_summary"] = wip_by_status
        kpis["wip_detail"] = wip[[c for c in ["case_number","account_id","doctor_name",
                                                "date_in","due_date","status","total_charge",
                                                "days_in_lab","overdue","rush"] 
                                    if c in wip.columns]].copy()
    else:
        wip_value = wip_count = wip_overdue = 0
        kpis["wip_summary"] = pd.DataFrame()
        kpis["wip_detail"] = pd.DataFrame()

    # ── 7c. Active Accounts (last 30 days) ────────────────────
    active = tables.get("active_cases", pd.DataFrame())
    if not active.empty:
        # Ensure account_id column exists
        if "account_id" not in active.columns and "Cases_CustomerID" in active.columns:
            active["account_id"] = active["Cases_CustomerID"]
        if "total_charge" not in active.columns and "Cases_TotalCharge" in active.columns:
            active["total_charge"] = pd.to_numeric(active["Cases_TotalCharge"], errors="coerce").fillna(0)
        if "account_id" in active.columns:
            # All cases with any status count as active accounts
            active_accounts = active[active["account_id"].notna()].groupby("account_id").agg(
                cases=("account_id","count"),
                revenue=("total_charge","sum"),
            ).reset_index().sort_values("revenue", ascending=False)
            kpis["active_accounts_30d"] = active_accounts
            log.info("Active accounts 30d: %d unique accounts", len(active_accounts))
        else:
            kpis["active_accounts_30d"] = pd.DataFrame()
    else:
        kpis["active_accounts_30d"] = pd.DataFrame()

    # ── 7d. Remakes detail ────────────────────────────────────
    remakes_df = tables.get("remakes", pd.DataFrame())
    if not remakes_df.empty:
        kpis["remakes_detail"] = remakes_df[[c for c in ["case_number","account_id",
                                                            "doctor_name","date_in",
                                                            "remake_reason","total_charge",
                                                            "status"] 
                                               if c in remakes_df.columns]].copy()
        remake_by_reason = remakes_df.groupby("remake_reason").size().reset_index(name="count")
        kpis["remake_by_reason"] = remake_by_reason
    else:
        kpis["remakes_detail"] = pd.DataFrame()
        kpis["remake_by_reason"] = pd.DataFrame()

    # ── 8. KPI gauges ─────────────────────────────────────────────────────
    overall_remake = (ytd_remake / ytd_total * 100) if ytd_total else 0
    kpis["kpi_gauges"] = pd.DataFrame([{
        "ytd_revenue":          ytd_total,
        "ytd_prior_revenue":    ly_total,
        "ltd_revenue":          ltd_total,
        "actual_growth_pct":    round(actual_growth, 2),
        "target_growth_pct":    growth_target,
        "remake_rate":          round(overall_remake, 2),
        "remake_alert_pct":     CFG["targets"]["remake_alert_pct"],
        "avg_margin_pct":       0,
        "implant_ytd_sales":    implants["ytd_sales"].sum(),
        "implant_accounts":     impl_summary["account_id"].nunique(),
        "pareto_account_count": len(kpis["pareto_accounts"]),
        "total_account_count":  len(active_prof),
        "all_account_count":    len(prof),
        "mtd_revenue":          mtd_sales,
        "mtd_remake_rate":      round(mtd_remake / mtd_sales * 100, 2) if mtd_sales else 0,
        "mtd_projected_month":  round(mtd_sales / date.today().day * __import__('calendar').monthrange(date.today().year, date.today().month)[1], 2) if mtd_sales and date.today().day > 0 else 0,
        "mtd_days_elapsed":     date.today().day,
        "mtd_days_in_month":    __import__('calendar').monthrange(date.today().year, date.today().month)[1],
        "wip_value":            wip_value,
        "wip_count":            wip_count,
        "wip_overdue":          wip_overdue,
        "active_accounts_30d":  len(kpis.get("active_accounts_30d", pd.DataFrame())),
        "remakes_30d":          len(remakes_df) if not remakes_df.empty else 0,
    }])

    return kpis


# ══════════════════════════════════════════════════════════════════════════════
#  GOOGLE DRIVE
# ══════════════════════════════════════════════════════════════════════════════

def get_drive_service():
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
    filename = local_path.name
    query    = (f"name='{filename}' and '{drive_folder_id}' in parents "
                f"and trashed=false")
    existing = service.files().list(q=query, fields="files(id)").execute().get("files", [])
    media    = MediaFileUpload(str(local_path), resumable=True)

    if existing:
        file_id = existing[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        log.info("Updated  Drive: %s", filename)
    else:
        meta    = {"name": filename, "parents": [drive_folder_id]}
        file_id = service.files().create(body=meta, media_body=media,
                                          fields="id").execute()["id"]
        log.info("Uploaded Drive: %s", filename)
    return file_id


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline():
    log.info("=" * 60)
    log.info("Pipeline started  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Step 0: Pull fresh exports from Gmail before loading
    live_exports = BASE_DIR / CFG["data_source"]["csv"]["watch_folder"].replace("C:/ArtisticDentalPortal/", "")
    watch_folder = Path(CFG["data_source"]["csv"]["watch_folder"])
    try:
        downloaded = download_gmail_attachments(watch_folder)
        if downloaded:
            log.info("Gmail: pulled %d file(s): %s", len(downloaded), downloaded)
        else:
            log.info("Gmail: no new files — using existing exports")
    except Exception as exc:
        log.error("Gmail download failed (using existing files): %s", exc)

    tables = load_data()
    kpis   = compute_kpis(tables)

    today_str    = date.today().strftime("%Y-%m-%d")
    latest_dir   = CACHE_DIR / "latest"
    snapshot_dir = CACHE_DIR / f"snapshot_{today_str}"
    latest_dir.mkdir(exist_ok=True)
    snapshot_dir.mkdir(exist_ok=True)

    # Save each KPI table as its own CSV — no Excel license needed
    for name, df in kpis.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.to_csv(latest_dir / f"{name}.csv", index=False)
            df.to_csv(snapshot_dir / f"{name}.csv", index=False)
    raw = tables.get("sales", pd.DataFrame())
    if not raw.empty:
        raw.to_csv(latest_dir / "raw_sales.csv", index=False)

    log.info("Saved local cache: %s", latest_dir)

    try:
        service   = get_drive_service()
        folder_id = CFG["google_drive"]["folder_id"]
        for csv_file in latest_dir.glob("*.csv"):
            upload_to_drive(service, csv_file, folder_id)
        log.info("Google Drive upload complete.")
    except Exception as exc:
        log.error("Drive upload failed (data saved locally): %s", exc)

    # Auto-push fresh CSV files to GitHub
    try:
        import subprocess
        repo_dir = str(BASE_DIR)
        subprocess.run(["git", "stash"], cwd=repo_dir)
        subprocess.run(["git", "pull", "origin", "main", "--rebase"], cwd=repo_dir)
        subprocess.run(["git", "stash", "pop"], cwd=repo_dir)
        subprocess.run(["git", "add", "cache/latest/"], cwd=repo_dir, check=True)
        result = subprocess.run(["git", "commit", "-m", f"Auto-update data {date.today()}"], cwd=repo_dir)
        if result.returncode == 0:
            subprocess.run(["git", "push", "origin", "main"], cwd=repo_dir, check=True)
            log.info("GitHub push complete.")
        else:
            log.info("Nothing new to push to GitHub.")
    except Exception as exc:
        log.error("GitHub push failed (data still saved locally): %s", exc)

    log.info("Pipeline run complete.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--daemon", action="store_true")
    args = parser.parse_args()

    if args.daemon:
        run_time = CFG["schedule"]["run_time"]
        log.info("Daemon mode: daily at %s", run_time)
        schedule.every().day.at(run_time).do(run_pipeline)
        run_pipeline()
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        run_pipeline()


if __name__ == "__main__":
    main()
