"""
mt_reports_parser.py
====================
Parsers for all Magic Touch Job Management exports landing in C:\\MT_Reports_Local.
Synced every 3 minutes from \\\\2019servermts01\\reports2 via sync_mt_reports.bat.

All public functions accept a Path (folder) and return clean pandas DataFrames
with standardized column names. PHI columns (patient_first, patient_last) are
kept for internal pipeline use — strip before any public-facing output.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

# ── Helpers ───────────────────────────────────────────────────────────────────

def _money(v) -> float:
    """Strip $, commas; coerce to float."""
    try:
        return float(re.sub(r"[$,]", "", str(v)).strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _enc(path: Path, fallback: str = "utf-8-sig") -> str:
    """Detect encoding: try utf-8-sig, fall back to latin-1."""
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            path.read_text(encoding=enc)
            return enc
        except UnicodeDecodeError:
            continue
    return fallback


def _hhmm_to_decimal(s) -> float:
    """Convert 'HH:MM' or 'H:MM' to decimal hours."""
    s = str(s).strip()
    if ":" in s:
        parts = s.split(":")
        try:
            return int(parts[0]) + int(parts[1]) / 60
        except (ValueError, IndexError):
            return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════════
#  1. Sales_Data.csv  — invoice-line level
# ═══════════════════════════════════════════════════════════════════════════════

def load_sales_data(folder: Path) -> pd.DataFrame:
    """
    Clean tabular CSV, row 0 = headers.
    Returns one row per invoiced product line with money cols as floats.
    Strips PHI before returning (PatientFirst / PatientLast dropped).
    """
    path = folder / "Sales_Data.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python")
    df.columns = [c.strip() for c in df.columns]

    # Standardise column names
    rename = {
        "CaseNumber":    "case_number",
        "CustomerID":    "account_id",
        "TaxItem":       "tax_item",
        "InvoiceDate":   "invoice_date",
        "CustomerName":  "customer_name",
        "PracticeName":  "practice_name",
        "Address1":      "address1",
        "Address2":      "address2",
        "City":          "city",
        "State":         "state",
        "ZipCode":       "zip_code",
        "OfficePhone":   "office_phone",
        "PatientFirst":  "patient_first",
        "PatientLast":   "patient_last",
        "ProductID":     "product_id",
        "Description":   "description",
        "Quantity":      "quantity",
        "UnitPrice":     "unit_price",
        "SalesDiscount": "sales_discount",
        "RemakeDiscount":"remake_discount",
        "TaxAmount":     "tax_amount",
        "InvoiceTotal":  "invoice_total",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Money → float
    for col in ("unit_price", "sales_discount", "remake_discount",
                "tax_amount", "invoice_total"):
        if col in df.columns:
            df[col] = df[col].apply(_money)

    # Dates
    if "invoice_date" in df.columns:
        df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    # Flag remakes: RemakeDiscount > 0
    if "remake_discount" in df.columns:
        df["is_remake"] = df["remake_discount"] > 0

    # Drop rows with no account_id
    if "account_id" in df.columns:
        df = df[df["account_id"].str.strip() != ""].copy()

    return df


def aggregate_sales_for_kpis(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert invoice-line Sales_Data into a pre-aggregated per-account DataFrame
    with ytd_sales, ly_sales, mtd_sales, q1–q4_sales, ytd_remake, etc.
    This preserves compatibility with compute_kpis() in pipeline.py.
    """
    if sales_df.empty:
        return pd.DataFrame()

    today = pd.Timestamp.today()
    yr    = today.year

    df = sales_df.copy()
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    df = df.dropna(subset=["invoice_date", "account_id"])

    # InvoiceTotal is already net of SalesDiscount (Magic Touch subtracts it
    # before writing the column). Do NOT subtract sales_discount again here —
    # that would double-count every discount and understate revenue.
    df["net_sales"] = df["invoice_total"].fillna(0)

    # Period flags
    df["is_ytd"] = df["invoice_date"].dt.year == yr
    df["is_ly"]  = df["invoice_date"].dt.year == yr - 1
    df["is_mtd"] = df["is_ytd"] & (df["invoice_date"].dt.month == today.month)
    df["is_lm"]  = (
        (df["invoice_date"].dt.year == (today.replace(day=1) - pd.Timedelta(days=1)).year) &
        (df["invoice_date"].dt.month == (today.replace(day=1) - pd.Timedelta(days=1)).month)
    )
    for q, months in enumerate([(1,2,3),(4,5,6),(7,8,9),(10,11,12)], 1):
        df[f"is_q{q}"] = df["is_ytd"] & df["invoice_date"].dt.month.isin(months)
    df["is_remake"] = df.get("remake_discount", pd.Series(0, index=df.index)).fillna(0) > 0

    def _agg(mask):
        return df[mask].groupby("account_id")["net_sales"].sum()

    ytd   = _agg(df["is_ytd"]).rename("ytd_sales")
    ly    = _agg(df["is_ly"]).rename("ly_sales")
    mtd   = _agg(df["is_mtd"]).rename("mtd_sales")
    lm    = _agg(df["is_lm"]).rename("lm_sales")
    q1    = _agg(df["is_q1"]).rename("q1_sales")
    q2    = _agg(df["is_q2"]).rename("q2_sales")
    q3    = _agg(df["is_q3"]).rename("q3_sales")
    q4    = _agg(df["is_q4"]).rename("q4_sales")

    ytd_rem = _agg(df["is_ytd"] & df["is_remake"]).rename("ytd_remake")
    ly_rem  = _agg(df["is_ly"]  & df["is_remake"]).rename("ly_remake")
    mtd_rem = _agg(df["is_mtd"] & df["is_remake"]).rename("mtd_remake")
    lm_rem  = _agg(df["is_lm"]  & df["is_remake"]).rename("lm_remake")
    q1_rem  = _agg(df["is_q1"]  & df["is_remake"]).rename("q1_remake")
    q2_rem  = _agg(df["is_q2"]  & df["is_remake"]).rename("q2_remake")
    q3_rem  = _agg(df["is_q3"]  & df["is_remake"]).rename("q3_remake")
    q4_rem  = _agg(df["is_q4"]  & df["is_remake"]).rename("q4_remake")

    # Lifetime
    ltd = df.groupby("account_id")["net_sales"].sum().rename("ltd_sales")

    # Product type + department (most recent product line per account)
    meta = (
        df.sort_values("invoice_date")
          .groupby("account_id")
          .last()[["product_id", "description"]]
          .rename(columns={"product_id": "product_id", "description": "product_type"})
    )
    if "department" not in df.columns:
        meta["department"] = ""
        meta["product_group"] = ""
    else:
        meta["department"] = df.sort_values("invoice_date").groupby("account_id").last()["department"]
        meta["product_group"] = ""

    out = pd.concat([ltd, ytd, ly, mtd, lm, q1, q2, q3, q4,
                     ytd_rem, ly_rem, mtd_rem, lm_rem,
                     q1_rem, q2_rem, q3_rem, q4_rem, meta], axis=1).reset_index()

    num_cols = [c for c in out.columns if c.endswith(("_sales","_remake"))]
    for c in num_cols:
        out[c] = out[c].fillna(0)

    out["is_implant"] = out.get("department", "").str.upper().str.contains("IMPLANT", na=False)
    return out


def compute_lytd_from_summary(portal_root: Path, through_date: "date | None" = None) -> float:
    """
    Read historical/Sales_2025.csv (Sales Summary By Date format) and return
    the Total Invoiced sum through `through_date` (same calendar date last year).

    The Sales Summary By Date CSV has a fixed layout:
      col 24 = date (YYYY-MM-DD), col 37 = Total Invoiced for that date.

    Returns 0.0 if the file is missing or unparseable.
    """
    from datetime import date as _date
    path = portal_root / "historical" / "Sales_2025.csv"
    if not path.exists():
        return 0.0
    if through_date is None:
        today = _date.today()
        through_date = today.replace(year=today.year - 1)
    cutoff = pd.Timestamp(through_date)
    try:
        df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                         on_bad_lines="skip", engine="python")
        DATE_COL, INV_COL = 24, 37
        if df.shape[1] <= INV_COL:
            return 0.0
        total = 0.0
        for _, row in df.iterrows():
            dt = pd.to_datetime(str(row.iloc[DATE_COL]).strip(), errors="coerce")
            if pd.notna(dt) and dt <= cutoff:
                total += _money(str(row.iloc[INV_COL]))
        return total
    except Exception:
        return 0.0


def compute_ly_same_month(portal_root: Path, month: "int | None" = None) -> float:
    """
    Return Total Invoiced for the same calendar month last year from
    historical/Sales_2025.csv.  Defaults to the current calendar month.
    """
    from datetime import date as _date
    if month is None:
        month = _date.today().month
    path = portal_root / "historical" / "Sales_2025.csv"
    if not path.exists():
        return 0.0
    try:
        df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                         on_bad_lines="skip", engine="python")
        DATE_COL, INV_COL = 24, 37
        if df.shape[1] <= INV_COL:
            return 0.0
        total = 0.0
        for _, row in df.iterrows():
            dt = pd.to_datetime(str(row.iloc[DATE_COL]).strip(), errors="coerce")
            if pd.notna(dt) and dt.month == month:
                total += _money(str(row.iloc[INV_COL]))
        return total
    except Exception:
        return 0.0


def load_ly_from_legacy_sales(portal_root: Path) -> pd.DataFrame:
    """
    Load last-year (2025) sales totals per account for LY KPI fields.

    Priority order:
      1. historical/Sales_2025.csv — invoice-line format (same as current
         MT_Reports_Local/Sales_Data.csv), full year 2025. Drop this file in
         C:\\ArtisticDentalPortal\\historical\\ once and it never needs updating.
      2. live_exports/Sales_Data.csv — old AHK pre-aggregated format with
         SalesData_LYSales columns. Used as fallback until the 2025 file exists.

    Returns a DataFrame with columns: account_id, ly_sales, ly_remake.
    Returns empty DataFrame if neither source is available.
    """
    # Option 1: dedicated 2025 invoice-line export (same format as Sales_Data.csv)
    hist_path = portal_root / "historical" / "Sales_2025.csv"
    if hist_path.exists():
        try:
            enc = _enc(hist_path)
            df = pd.read_csv(hist_path, encoding=enc, low_memory=False)
            id_col  = next((c for c in df.columns if c.strip().lower() in
                            ("customerid", "customer id", "account_id")), None)
            tot_col = next((c for c in df.columns if c.strip().lower() in
                            ("invoicetotal", "invoice total", "invoiceamount")), None)
            rem_col = next((c for c in df.columns if "remakedisc" in c.lower() or
                            "remake_disc" in c.lower()), None)
            if id_col and tot_col:
                df = df.rename(columns={id_col: "account_id", tot_col: "net_sales"})
                df["net_sales"] = (df["net_sales"].astype(str)
                    .str.replace(r"[$,]", "", regex=True)
                    .pipe(pd.to_numeric, errors="coerce").fillna(0))
                df["is_remake"] = False
                if rem_col:
                    df["is_remake"] = (df[rem_col].astype(str)
                        .str.replace(r"[$,]", "", regex=True)
                        .pipe(pd.to_numeric, errors="coerce").fillna(0) > 0)
                grp = df.groupby("account_id")
                out = pd.DataFrame({
                    "ly_sales":  grp["net_sales"].sum(),
                    "ly_remake": grp.apply(
                        lambda g: g.loc[g["is_remake"], "net_sales"].sum()
                    ),
                }).reset_index()
                return out
        except Exception:
            pass  # fall through to legacy

    # Option 2: legacy AHK pre-aggregated export
    legacy_path = portal_root / "live_exports" / "Sales_Data.csv"
    if not legacy_path.exists():
        return pd.DataFrame()
    try:
        enc = _enc(legacy_path)
        df = pd.read_csv(legacy_path, encoding=enc, low_memory=False)
        col_map = {
            "SalesData_CustomerID": "account_id",
            "SalesData_LYSales":    "ly_sales",
            "SalesData_LYRemake":   "ly_remake",
        }
        if any(c not in df.columns for c in col_map):
            return pd.DataFrame()
        df = df.rename(columns=col_map)[["account_id", "ly_sales", "ly_remake"]].copy()
        for col in ("ly_sales", "ly_remake"):
            df[col] = (df[col].astype(str).str.replace(r"[$,]", "", regex=True)
                .pipe(pd.to_numeric, errors="coerce").fillna(0))
        return df.groupby("account_id").agg(
            {"ly_sales": "sum", "ly_remake": "sum"}
        ).reset_index()
    except Exception:
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
#  2. Active_30_day.csv  — shipped/invoiced cases rolling ~30 days
# ═══════════════════════════════════════════════════════════════════════════════

def load_active_30_day(folder: Path) -> pd.DataFrame:
    """
    Clean tabular CSV, row 0 = headers.
    One row per case-product line. Returns with standardised column names.
    """
    path = folder / "Active_30_day.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python",
                     encoding=_enc(path))
    df.columns = [c.strip() for c in df.columns]

    rename = {
        "CaseNumber":     "case_number",
        "CustomerID":     "account_id",
        "Status":         "status",
        "PanNumber":      "pan_number",
        "RxNumber":       "rx_number",
        "DateIn":         "date_in",
        "ShipDate":       "ship_date",
        "DueDate":        "due_date",
        "InvoiceDate":    "invoice_date",
        "PatientFirst":   "patient_first",
        "PatientLast":    "patient_last",
        "FirstName":      "doctor_first",
        "LastName":       "doctor_last",
        "DoctorName":     "doctor_name",
        "Route":          "route",
        "ProductID":      "product_id",
        "Quantity":       "quantity",
        "TotalCharge":    "total_charge",
        "Description":    "description",
        "Type":           "product_type",
        "Group":          "product_group",
        "SubGroup":       "product_subgroup",
        "Category":       "product_category",
        "Dept.":          "department",
        "LabName":        "lab_name",
        "Practice":       "practice",
        "PatientApptDate":"patient_appt_date",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    for col in ("date_in", "ship_date", "due_date", "invoice_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "total_charge" in df.columns:
        df["total_charge"] = df["total_charge"].apply(_money)
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  3. tech_productivity.csv  — task-level technician performance
# ═══════════════════════════════════════════════════════════════════════════════

def load_tech_productivity(folder: Path) -> pd.DataFrame:
    """
    Clean tabular CSV, row 0 = headers, one row per task completed.

    Quirk: The first technician block (code 740 / SCHEDULE CASE) has blank
    Technician/Technician Name/Technician Department columns; their code and
    name spill into the Task/Task Description columns instead. We detect and
    fix this so every row has a valid tech_code.
    """
    path = folder / "tech_productivity.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python")
    df.columns = [c.strip() for c in df.columns]

    rename = {
        "Technician":            "tech_code",
        "Technician Name":       "tech_name",
        "Technician Department": "tech_dept",
        "Task":                  "task_code",
        "Task Description":      "task_desc",
        "Case #":                "case_number",
        "Product":               "product_id",
        "Product Description":   "product_desc",
        "Completion Date":       "completion_date",
        "Accepted":              "accepted",
        "Rejected":              "rejected",
        "Paid":                  "paid",
        "Is Remake":             "is_remake",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Fix blank-tech rows: when tech_code is blank but task_code looks like a
    # numeric tech code, the data shifted right by 3 columns on export.
    if "tech_code" in df.columns and "task_code" in df.columns:
        blank_tech = df["tech_code"].str.strip() == ""
        # task_code contains the tech code when it's numeric (e.g. "740")
        spilled = blank_tech & df["task_code"].str.match(r"^\d+$")
        if spilled.any():
            df.loc[spilled, "tech_code"]  = df.loc[spilled, "task_code"]
            df.loc[spilled, "tech_name"]  = df.loc[spilled, "task_desc"]
            df.loc[spilled, "task_code"]  = df.loc[spilled, "case_number"]
            df.loc[spilled, "task_desc"]  = df.loc[spilled, "product_id"]
            df.loc[spilled, "case_number"]= df.loc[spilled, "product_desc"]
            df.loc[spilled, "product_id"] = df.loc[spilled, "completion_date"]
            df.loc[spilled, "product_desc"]= df.loc[spilled, "accepted"]
            df.loc[spilled, "completion_date"] = df.loc[spilled, "rejected"]
            df.loc[spilled, "accepted"]   = df.loc[spilled, "paid"]
            df.loc[spilled, "rejected"]   = df.loc[spilled, "is_remake"]
            df.loc[spilled, ["paid", "is_remake", "tech_dept"]] = ""

    # Propagate tech_code / tech_name / tech_dept forward within each group
    # (the report only prints these on the first row of each tech block)
    for col in ("tech_code", "tech_name", "tech_dept"):
        if col in df.columns:
            df[col] = df[col].replace("", pd.NA).ffill()

    if "completion_date" in df.columns:
        df["completion_date"] = pd.to_datetime(df["completion_date"], errors="coerce")
    for col in ("accepted", "rejected"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Drop header-only rows (case_number blank after propagation)
    if "case_number" in df.columns:
        df = df[df["case_number"].str.strip() != ""].copy()

    return df


# ═══════════════════════════════════════════════════════════════════════════════
#  4. WIP_cases.csv  — open cases by ship date (hierarchical)
# ═══════════════════════════════════════════════════════════════════════════════

def load_wip_cases(folder: Path) -> pd.DataFrame:
    """
    Tabular CSV — row 0 = report title (contains embedded newlines in one
    quoted field), row 1 = column headers, rows 2+ = data.
    Uses csv.reader so the multiline title field is consumed as one record.
    """
    import csv as _csv
    path = folder / "WIP_cases.csv"
    if not path.exists():
        return pd.DataFrame()

    enc = _enc(path)
    with open(path, newline="", encoding=enc, errors="replace") as f:
        records = list(_csv.reader(f))

    # Locate the header record: first record whose col 0 is a known column name
    header_idx = None
    for i, rec in enumerate(records):
        if rec and str(rec[0]).strip().lower() in ("shipdate", "ship date", "case #"):
            header_idx = i
            break
    if header_idx is None:
        header_idx = 1 if len(records) > 1 else 0

    headers      = [c.strip() for c in records[header_idx]]
    data_records = records[header_idx + 1:]

    def _get(vals, col_name):
        try:
            return vals[headers.index(col_name)] if col_name in headers else ""
        except (ValueError, IndexError):
            return ""

    rows = []
    for rec in data_records:
        if not rec:
            continue
        vals = [str(v).strip() for v in rec]
        while len(vals) < len(headers):
            vals.append("")
        case_num = _get(vals, "Case #")
        if not case_num.isdigit():
            continue
        rows.append({
            "ship_date":     _get(vals, "ShipDate"),
            "case_number":   case_num,
            "pan_number":    _get(vals, "Pan #"),
            "account_id":    _get(vals, "Customer ID"),
            "customer_name": _get(vals, "Customer Name"),
            "doctor_name":   _get(vals, "Doctor Name"),
            "patient_name":  _get(vals, "Patient Name"),  # PHI
            "date_in":       _get(vals, "Date In"),
            "due_date":      _get(vals, "DueDate"),
            "total_charge":  _money(_get(vals, "Total Charge")),
        })

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    for col in ("ship_date", "date_in", "due_date"):
        out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


# ═══════════════════════════════════════════════════════════════════════════════
#  5. case_location.csv  — current location of every open case (hierarchical)
# ═══════════════════════════════════════════════════════════════════════════════

def load_case_location(folder: Path) -> pd.DataFrame:
    """
    Hierarchical report with a 2-row header (title + column names, the latter
    containing quoted multiline fields).  Uses csv.reader to consume those
    records cleanly, then skips to the data section.

    Layout after headers:
      Location group rows : col 0 starts with 'Location:'
      Data rows           : col 0 = 'CUSTID: Customer Name', col 1 = case# (int)
                            col 2=age_days, col3=date_in, col4=due_date,
                            col5=ship_date, col6=days_at_loc, col7=last_scan_date,
                            col8=tech, col9=patient_name(PHI), col11=pan#, col12=status
    """
    import csv as _csv
    path = folder / "case_location.csv"
    if not path.exists():
        return pd.DataFrame()

    enc = _enc(path)
    with open(path, newline="", encoding=enc, errors="replace") as f:
        records = list(_csv.reader(f))

    # Skip title record (row 0) and column-header record (row 1)
    data_records = records[2:]

    rows = []
    current_location = ""

    for rec in data_records:
        vals = [str(v).strip() for v in rec]
        c0 = vals[0] if vals else ""

        # Location group header: "Location: CADOUT"
        if c0.startswith("Location:"):
            current_location = c0.replace("Location:", "").strip()
            continue

        # Skip lab header, totals, empty rows
        if not c0 or c0.startswith("Lab:") or c0.startswith("Total"):
            continue

        # Data row: col 1 must be a numeric case number
        case_num = vals[1].strip() if len(vals) > 1 else ""
        if not case_num.isdigit():
            continue

        # col 0 = "CUSTID: Customer Name"
        if ":" in c0:
            acct_id, cust_name = c0.split(":", 1)
            acct_id   = acct_id.strip()
            cust_name = cust_name.strip()
        else:
            acct_id   = c0
            cust_name = ""

        rows.append({
            "account_id":     acct_id,
            "customer_name":  cust_name,
            "case_number":    case_num,
            "age_days":       vals[2]  if len(vals) > 2  else "",
            "date_in":        vals[3]  if len(vals) > 3  else "",
            "due_date":       vals[4]  if len(vals) > 4  else "",
            "ship_date":      vals[5]  if len(vals) > 5  else "",
            "days_at_loc":    vals[6]  if len(vals) > 6  else "",
            "last_scan_date": vals[7]  if len(vals) > 7  else "",
            "tech_code":      vals[8]  if len(vals) > 8  else "",
            "patient_name":   vals[9]  if len(vals) > 9  else "",  # PHI
            "pan_number":     vals[11] if len(vals) > 11 else "",
            "status":         vals[12] if len(vals) > 12 else "",
            "last_location":  current_location,
        })

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    for col in ("date_in", "due_date", "ship_date", "last_scan_date"):
        out[col] = pd.to_datetime(out[col], errors="coerce")
    if "age_days" in out.columns:
        out["age_days"] = pd.to_numeric(out["age_days"], errors="coerce").fillna(0).astype(int)
    return out


# ═══════════════════════════════════════════════════════════════════════════════
#  6. build_unified_wip()  — joins WIP_cases + case_location for logistics
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_all_cases_csv(path: Path) -> dict:
    """
    Parse one AllCasesByDateIn-format CSV into a {case_number_str: total_charge_float} dict.

    Two known export formats:
      Standard (11 cols): col 0 = case#, col 10 = TotalCharge
        - Produced by the rolling AHK export (AllCasesByDateIn.csv)
      Flat/historical (40 cols): col 18 = case#, col 28 = TotalCharge
        - Produced when the report is manually exported going back to 2020;
          the report title + header + group summary + case data + grand totals
          are all collapsed into one wide row per case.

    Format is detected from the first data row's column count.
    """
    import csv as _csv, io as _io
    enc = _enc(path)
    with open(path, "rb") as f:
        raw = f.read().replace(b"\x00", b"")
    text = raw.decode(enc, errors="replace")
    rows = list(_csv.reader(_io.StringIO(text)))
    lookup: dict = {}

    # Detect format from the first non-empty row
    first_row = next((r for r in rows if r), [])
    wide_format = len(first_row) >= 30 and first_row[18].strip().isdigit()

    if wide_format:
        # 40-col flat format: case# at col 18, TotalCharge at col 28
        CASE_COL, CHARGE_COL = 18, 28
        for r in rows:
            if len(r) <= CHARGE_COL:
                continue
            case_num = r[CASE_COL].strip()
            if not case_num.isdigit():
                continue
            lookup.setdefault(case_num, _money(r[CHARGE_COL]))
    else:
        # Standard 11-col format: case# at col 0, TotalCharge at col 10
        for r in rows:
            if not r:
                continue
            case_num = r[0].strip()
            if not case_num.isdigit():
                continue
            charge = _money(r[10]) if len(r) > 10 else 0.0
            lookup.setdefault(case_num, charge)

    return lookup


def load_all_cases_charge_lookup(folder: Path) -> dict:
    """
    Build a {case_number_str: total_charge_float} lookup from all available sources:

      1. historical/AllCases_charge_lookup.csv  — one-time export going back to lab
                                                   inception (run manually, widest coverage)
      2. AllCasesByDateIn.csv                   — rolling live export (current date range)

    The historical file is loaded first (broadest coverage). The live file then fills
    in any case numbers not already seen, so recent cases added after the historical
    export was generated are always captured.
    """
    lookup: dict = {}

    # 1. Historical one-time export (widest date range)
    # mt_reports_parser.py lives in the portal root, so historical/ is a sibling.
    portal_root = Path(__file__).parent
    hist_path = portal_root / "historical" / "AllCases_charge_lookup.csv"
    if hist_path.exists():
        hist = _parse_all_cases_csv(hist_path)
        lookup.update(hist)
        import logging as _log
        _log.getLogger(__name__).info(
            "WIP charge lookup: loaded %d cases from historical/AllCases_charge_lookup.csv",
            len(hist)
        )

    # 2. Live rolling export — fills gaps for cases newer than the historical snapshot
    live_path = folder / "AllCasesByDateIn.csv"
    if live_path.exists():
        live = _parse_all_cases_csv(live_path)
        before = len(lookup)
        for k, v in live.items():
            lookup.setdefault(k, v)  # don't overwrite historical with live
        added = len(lookup) - before
        if added:
            import logging as _log
            _log.getLogger(__name__).info(
                "WIP charge lookup: +%d new cases from AllCasesByDateIn.csv (total %d)",
                added, len(lookup)
            )

    return lookup


def build_unified_wip(folder: Path) -> pd.DataFrame:
    """
    Join WIP_cases.csv (open case list) with case_location.csv (location/status)
    on case_number. Outputs a DataFrame with Cases_* column names so that
    pipeline_logistics.py works without modification.

    Charge lookup priority (highest wins, fills gaps downward):
      1. WIP_cases.csv     — "Cases Due Out by Ship Date" (date-filtered, ~200 cases)
      2. AllCasesByDateIn.csv — all cases by intake date (broader, ~13 K rows)
    case_location.csv is the authoritative open-case list (no charge column).

    Optionally enriches with Active_30_day.csv for Products_Department where
    the case appears in the rolling window.
    """
    wip = load_wip_cases(folder)
    loc = load_case_location(folder)

    if wip.empty and loc.empty:
        return pd.DataFrame()

    # Start from case_location (authoritative for open cases + last location)
    if loc.empty:
        base = wip.copy() if not wip.empty else pd.DataFrame()
    elif wip.empty:
        base = loc.copy()
    else:
        base = loc.merge(
            wip[["case_number", "doctor_name", "customer_name",
                 "total_charge", "pan_number"]].drop_duplicates("case_number"),
            on="case_number", how="left",
            suffixes=("", "_wip"),
        )
        # Fill pan_number from WIP if case_location didn't have it
        if "pan_number_wip" in base.columns:
            base["pan_number"] = base["pan_number"].where(
                base["pan_number"] != "", base["pan_number_wip"]
            )
            base = base.drop(columns=["pan_number_wip"])

    # --- Supplement total_charge from AllCasesByDateIn for cases WIP_cases missed ---
    # WIP_cases is filtered by ship-date window so many open cases have no charge.
    # AllCasesByDateIn covers a wider date range and fills most of the gap.
    if "case_number" in base.columns:
        all_charges = load_all_cases_charge_lookup(folder)
        if all_charges:
            if "total_charge" not in base.columns:
                base["total_charge"] = 0.0
            base["total_charge"] = pd.to_numeric(base["total_charge"], errors="coerce").fillna(0)
            missing_mask = base["total_charge"] == 0
            base.loc[missing_mask, "total_charge"] = (
                base.loc[missing_mask, "case_number"].map(all_charges).fillna(0)
            )
            n_filled = int(missing_mask.sum()) - int((base["total_charge"] == 0).sum())
            if n_filled > 0:
                import logging as _log
                _log.getLogger(__name__).info(
                    "WIP charges: filled %d cases from AllCasesByDateIn "
                    "(total WIP now $%.0f)", n_filled, base["total_charge"].sum()
                )

    # Enrich with Products_Department from Active_30_day where available
    active = load_active_30_day(folder)
    if not active.empty and "case_number" in active.columns and "department" in active.columns:
        dept_map = (
            active.dropna(subset=["department"])
                  .query("department != ''")
                  .drop_duplicates("case_number")
                  .set_index("case_number")["department"]
        )
        base["Products_Department"] = base["case_number"].map(dept_map).fillna("")
    else:
        base["Products_Department"] = ""

    # Rename to Cases_* for pipeline_logistics.py compatibility
    rename = {
        "case_number":  "Cases_CaseNumber",
        "account_id":   "Cases_CustomerID",
        "doctor_name":  "Cases_DoctorName",
        "pan_number":   "Cases_PanNumber",
        "date_in":      "Cases_DateIn",
        "due_date":     "Cases_DueDate",
        "ship_date":    "Cases_ShipDate",
        "status":       "Cases_Status",
        "last_location":"Cases_LastLocation",
        "total_charge": "Cases_TotalCharge",
        "customer_name":"Cases_CustomerName",
        "age_days":     "Cases_AgeDays",
    }
    base = base.rename(columns={k: v for k, v in rename.items() if k in base.columns})

    # Ensure Cases_TotalCharge is numeric
    if "Cases_TotalCharge" in base.columns:
        base["Cases_TotalCharge"] = pd.to_numeric(
            base["Cases_TotalCharge"], errors="coerce"
        ).fillna(0)

    return base


# ═══════════════════════════════════════════════════════════════════════════════
#  7. remake_reasons.csv  — hierarchical 3-col reason/customer/case layout
# ═══════════════════════════════════════════════════════════════════════════════

def load_remake_reasons(folder: Path) -> pd.DataFrame:
    """
    "Remake Reasons Case Detail Report" — grouped-by-reason layout:
      Group header:  ["Case Count: 2","Case Mix-Up"]
      Column header: ["Date In","Invoiced","Case #","Customer","Product ID",
                       "Single","Bridge","Remake","Entered By"]  (repeats per group)
      Detail row:    [date_in, invoiced, case#, account_id, product_id,
                       single, bridge, remake_type, entered_by]
      Footer:        ["Total Remake Cases for Reason ""X"":  N"]  (skipped)

    A case can have multiple product lines under the same reason — this
    returns one row per (reason, case, product) line; aggregate on distinct
    case_number, not row count, to avoid over-counting multi-line cases.

    Returns: remake_reason, date_in, invoiced, case_number, account_id,
    product_id, single, bridge, remake_type (e.g. "Remake 100%",
    "Adjustments", "Flat Remake Discount"), entered_by.
    """
    import csv as _csv
    path = folder / "remake_reasons.csv"
    if not path.exists():
        return pd.DataFrame()

    enc = _enc(path)
    with open(path, newline="", encoding=enc, errors="replace") as f:
        records = list(_csv.reader(f))

    rows = []
    current_reason = ""

    for rec in records:
        vals = [str(v).strip() for v in rec]
        if not vals or not vals[0]:
            continue
        c0 = vals[0]

        # Group header: "Case Count: 2","Case Mix-Up"
        if c0.startswith("Case Count:") and len(vals) >= 2:
            current_reason = vals[1].strip()
            continue

        # Column header / report title / footer rows — skip
        if c0 == "Date In" or c0.startswith("Total Remake Cases") or \
           c0.startswith("Remake Reasons Case Detail Report"):
            continue

        # Detail row: first col is a date like "7/6/2026"
        if current_reason and len(vals) >= 9 and re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", c0):
            case_num = vals[2].strip()
            if not case_num.isdigit():
                continue
            rows.append({
                "remake_reason": current_reason,
                "date_in":       c0,
                "invoiced":      vals[1],
                "case_number":   case_num,
                "account_id":    vals[3],
                "product_id":    vals[4],
                "single":        vals[5],
                "bridge":        vals[6],
                "remake_type":   vals[7],
                "entered_by":    vals[8],
            })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
#  8. Remake by Lab Customer Products.csv  — richest remake data
# ═══════════════════════════════════════════════════════════════════════════════

def load_remake_by_lab(folder: Path) -> pd.DataFrame:
    """
    Each row contains ALL fields concatenated: report header, reason, column
    labels, customer, case, product, units/amount, and subtotals — all on
    every row. We extract the repeating fields by position.

    Columns extracted per row (0-indexed from raw):
      4  = Remake Reason (e.g. 'Remake Reason: Contacts')
      13 = CustomerID
      14 = Customer Name
      15 = Case Number
      16 = Patient Name (PHI)
      17 = Product ID
      18 = Product Description
      19 = Units
      20 = Amount
    Returns one row per case-product with reason, customer, case, product detail.
    """
    path = folder / "Remake by Lab Customer Products.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python")

    rows = []
    for _, row in df.iterrows():
        vals = row.tolist()
        if len(vals) < 21:
            continue
        reason_raw = str(vals[4]).strip()
        if not reason_raw.startswith("Remake Reason:"):
            continue
        case_num = str(vals[15]).strip()
        if not case_num.isdigit():
            continue
        reason = reason_raw.replace("Remake Reason:", "").strip()
        units  = _money(vals[19])
        if units == 0:
            continue
        rows.append({
            "remake_reason":  reason,
            "account_id":     str(vals[13]).strip(),
            "customer_name":  str(vals[14]).strip(),
            "case_number":    case_num,
            "patient_name":   str(vals[16]).strip(),  # PHI
            "product_id":     str(vals[17]).strip(),
            "product_desc":   str(vals[18]).strip(),
            "units":          units,
            "amount":         _money(vals[20]),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
#  9. EmployeeProductivity.xls  — per-employee units / hours / UPH
# ═══════════════════════════════════════════════════════════════════════════════

def load_employee_productivity(folder: Path) -> pd.DataFrame:
    """
    XLS hierarchical report. Structure (even rows only — odd rows are spacers):
      Department header row: Col 0 starts with 'Department:'
      Employee row:          Col 0 = 'CODE - Last, First'
      Time-category rows:    Col 0 = 'Production', 'Holiday', 'Break', etc.

    We extract one row per employee: code, name, department, total_units,
    total_hours (HH:MM string → decimal), uph, production_units,
    production_hours.
    """
    import xlrd  # installed in requirements
    path = folder / "EmployeeProductivity.xls"
    if not path.exists():
        return pd.DataFrame()

    wb = xlrd.open_workbook(str(path))
    sh = wb.sheet_by_index(0)
    _hours_to_decimal = _hhmm_to_decimal

    rows = []
    current_dept = ""
    current_emp  = None

    for i in range(sh.nrows):
        row = [str(sh.cell_value(i, c)).strip() for c in range(sh.ncols)]
        c0  = row[0]

        if c0.startswith("Department:"):
            # Flush previous employee
            if current_emp:
                rows.append(current_emp)
                current_emp = None
            current_dept = c0.replace("Department:", "").strip()
            continue

        # Employee row: "CODE - Last, First" in col 0; data in cols 7 / 11 / 13
        if " - " in c0 and not c0.startswith("Department:"):
            if current_emp:
                rows.append(current_emp)
            code, name = c0.split(" - ", 1)
            current_emp = {
                "tech_code":        code.strip(),
                "tech_name":        name.strip(),
                "department":       current_dept,
                "total_units":      _money(row[7])  if len(row) > 7  else 0.0,
                "total_hours":      _hours_to_decimal(row[11]) if len(row) > 11 else 0.0,
                "uph":              _money(row[13]) if len(row) > 13 else 0.0,
                "production_units": 0.0,
                "production_hours": 0.0,
            }
            continue

        # Time-category sub-rows: category name is in col 1 (col 0 is blank),
        # data is in cols 7 (units) and 11 (hours)
        if current_emp and not c0 and len(row) > 1 and row[1]:
            cat = row[1].strip()
            if cat == "Production":
                current_emp["production_units"] = _money(row[7])  if len(row) > 7  else 0.0
                current_emp["production_hours"] = _hours_to_decimal(row[11]) if len(row) > 11 else 0.0

    if current_emp:
        rows.append(current_emp)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
#  Helper: resolve .xls ↔ .csv  (pick freshest existing file)
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_path(folder: Path, name: str) -> Path:
    """Return the freshest file matching name, trying the alternate extension
    (.xls ↔ .csv) when the primary is absent or older."""
    primary = folder / name
    stem    = Path(name).stem
    ext     = Path(name).suffix.lower()
    alt_ext = ".csv" if ext in (".xls", ".xlsx") else ".xls"
    alt     = folder / (stem + alt_ext)

    p_ok = primary.exists()
    a_ok = alt.exists()

    if p_ok and a_ok:
        return primary if primary.stat().st_mtime >= alt.stat().st_mtime else alt
    if p_ok:
        return primary
    if a_ok:
        return alt
    return primary   # caller handles missing-file


# ═══════════════════════════════════════════════════════════════════════════════
#  10. prod_by_dept  (xls or csv)  — department/product unit counts
# ═══════════════════════════════════════════════════════════════════════════════

def load_prod_by_dept(folder: Path) -> pd.DataFrame:
    """
    Magic Touch "Sales By Product Department" report.
    Hierarchical layout (same structure in both XLS and LibreOffice-converted CSV):
      Row 0-9:  report header / column labels
        Header row contains "Product ID" in col 0
      Department row: col 0 starts with "Department:"
      Product row:    col 0 = "PRODID-Description", col 3 = New Units, col 4 = Rmk Units
      Totals row:     col 0 is blank
    Returns one row per product: department, product_id, description,
    new_units, remake_units, remake_discount, sales_discount, credit_discount,
    metal_charges, total_invoiced, net_sales.
    remake_discount/net_sales are the primary source for department-level
    "dollars remade vs. dollars in sales" — Magic Touch computes Remake Discount
    from its own case-level remake-type weighting, so no separate manual
    100%/50%/adjustment weighting needs to be reconstructed here.
    Picks the freshest of prod_by_dept.xls / prod_by_dept.csv via _resolve_path.
    """
    path = _resolve_path(folder, "prod_by_dept.xls")
    if not path.exists():
        return pd.DataFrame()

    ext = path.suffix.lower()

    # ── Read into a list of string rows ──────────────────────────────────────
    # Try multiple strategies in order: xlrd → openpyxl → read-as-csv → sibling .csv
    raw_rows: list[list[str]] = []

    def _rows_from_csv(p: Path) -> list:
        df = pd.read_csv(p, header=None, dtype=str,
                         keep_default_na=False, on_bad_lines="skip",
                         engine="python")
        return [[str(v).strip() for v in row] for row in df.values.tolist()]

    if ext == ".csv":
        try:
            raw_rows = _rows_from_csv(path)
        except Exception:
            return pd.DataFrame()
    else:
        # 1. xlrd (classic BIFF)
        loaded = False
        try:
            import xlrd
            wb = xlrd.open_workbook(str(path))
            sh = wb.sheet_by_index(0)
            raw_rows = [
                [str(sh.cell_value(i, c)).strip() for c in range(sh.ncols)]
                for i in range(sh.nrows)
            ]
            loaded = True
        except Exception:
            pass

        # 2. openpyxl (xlsx stored as .xls)
        if not loaded:
            try:
                df = pd.read_excel(path, header=None, dtype=str, engine="openpyxl")
                raw_rows = [[str(v).strip() for v in row] for row in df.values.tolist()]
                loaded = True
            except Exception:
                pass

        # 3. File might actually be CSV despite the .xls extension
        if not loaded:
            try:
                raw_rows = _rows_from_csv(path)
                loaded = True
            except Exception:
                pass

        # 4. Fall back to sibling .csv file (same stem, .csv extension)
        if not loaded:
            sibling = path.parent / (path.stem + ".csv")
            if sibling.exists():
                try:
                    raw_rows = _rows_from_csv(sibling)
                    loaded = True
                except Exception:
                    pass

        if not loaded:
            return pd.DataFrame()

    if not raw_rows:
        return pd.DataFrame()

    # ── Parse hierarchical rows ───────────────────────────────────────────────
    results      = []
    current_dept = ""

    for row in raw_rows:
        c0 = row[0] if row else ""

        # Department header
        if c0.startswith("Department:"):
            current_dept = c0.replace("Department:", "").strip()
            continue

        # Skip blank, header, or totals rows
        if not c0 or not current_dept:
            continue
        if any(kw in c0 for kw in ("Product ID", "Invoicing Lab", "Sales By Product")):
            continue

        # Product row: "PRODID-Description" or "PRODID-Description (detail)"
        # New units = col 3, Remake units = col 4
        if len(row) < 5:
            continue
        new_u    = int(_money(row[3])) if row[3] else 0
        remake_u = int(_money(row[4])) if row[4] else 0
        if new_u == 0 and remake_u == 0:
            continue   # totals / blank rows

        # Split product ID from description on first hyphen
        if "-" in c0:
            prod_id, desc = c0.split("-", 1)
        else:
            prod_id, desc = c0, ""

        # Dollar columns (cols 6-12): Credit Discount, Sales Discount,
        # Remake Discount, Metal Charges, Total Tax, Total Invoiced, Net Sales.
        # Guard on row length — some historical exports may be narrower.
        def _col(idx):
            return _money(row[idx]) if len(row) > idx and row[idx] else 0.0

        results.append({
            "department":       current_dept,
            "product_id":       prod_id.strip(),
            "description":      desc.strip(),
            "new_units":        new_u,
            "remake_units":     remake_u,
            "credit_discount":  _col(6),
            "sales_discount":   _col(7),
            "remake_discount":  _col(8),
            "metal_charges":    _col(9),
            "total_invoiced":   _col(11),
            "net_sales":        _col(12),
        })

    return pd.DataFrame(results) if results else pd.DataFrame()


def load_all_cases_daily(folder: Path) -> pd.DataFrame:
    """
    Magic Touch "All Cases by Date In" summary export.
    Each data row has three columns:
      col 0: "Cases In on M/D/YYYY:\t<count>"
      col 1: "Units In on M/D/YYYY:\t<count>"
      col 2: "Total for Date: $<amount>"
    Returns one row per date with: date, cases_in, units_in, total_amount.
    Accepts .csv or .xls — picks the freshest via _resolve_path.
    """
    # Prefer DailyCaseExport.csv — includes WIP + invoiced cases, so it's
    # more current (~yesterday) vs AllCasesByDateIn (invoiced only, ~2-week lag).
    # Pick whichever is fresher if both exist.
    _daily = _resolve_path(folder, "DailyCaseExport.csv")
    _all   = _resolve_path(folder, "AllCasesByDateIn.csv")
    if _daily.exists() and _all.exists():
        path = _daily if _daily.stat().st_mtime >= _all.stat().st_mtime else _all
    elif _daily.exists():
        path = _daily
    elif _all.exists():
        path = _all
    else:
        return pd.DataFrame()

    ext = path.suffix.lower()
    try:
        if ext in (".xls", ".xlsx"):
            raw = pd.read_excel(path, header=None, dtype=str)
        else:
            raw = pd.read_csv(path, header=None, dtype=str,
                              keep_default_na=False, on_bad_lines="skip",
                              engine="python")
    except Exception:
        return pd.DataFrame()

    if raw.empty:
        return pd.DataFrame()

    # Pattern: "Cases In on 1/2/2026:  \t35"
    date_re  = re.compile(r"Cases In on\s+([\d/]+):", re.IGNORECASE)
    count_re = re.compile(r"[:\t\s]+([\d.]+)\s*$")

    rows = []
    for _, row in raw.iterrows():
        c0 = str(row.iloc[0]).strip() if len(row) > 0 else ""
        c1 = str(row.iloc[1]).strip() if len(row) > 1 else ""
        c2 = str(row.iloc[2]).strip() if len(row) > 2 else ""

        m = date_re.search(c0)
        if not m:
            continue

        date_str = m.group(1)
        dt = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(dt):
            continue

        # Extract trailing number from each column
        def _tail_num(s):
            t = s.split("\t")[-1].strip() if "\t" in s else s.split(":")[-1].strip()
            try:
                return float(re.sub(r"[$,]", "", t))
            except (ValueError, TypeError):
                return 0.0

        rows.append({
            "date":         dt,
            "cases_in":     int(_tail_num(c0)),
            "units_in":     _tail_num(c1),
            "total_amount": _tail_num(c2),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def load_internal_remakes(folder: Path, year: int = None, month: int = None):
    """
    Parse InternalRemakesAnalysis.xls (grouped by technician).
    Returns dict of {tech_code: remake_count} for the given year+month.
    If year/month are None, returns counts for all dates.
    """
    import xlrd
    from collections import defaultdict

    KNOWN_CODES = {
        "GAMMA-TECH", "3SHAPE", "EASY DENT", "EvidentQC", "Evident",
        "AJEBLE", "AJEZYM", "ALMSEN", "AREVHE", "ARSORU", "AUKSOR",
        "CARLUC", "CHRICH", "CIAJIA", "CONWSA", "DELEAL", "DENMIA",
        "DIVERI", "EARLMA", "FLOBRE", "FRAHOP", "FREDCH", "GAMMA-TECH",
        "GASICO", "GIBPCEL", "GORMAT", "HOOTAR", "IOVFRE", "KAZERI",
        "KENSCA", "MAGASE", "OHAJOH", "PODBES", "POZDOM", "PRINKE",
        "SALPAL", "SHARSA", "TAPLIZ", "ULASDA", "ULASJE", "VEGANI",
        "VENPAU", "WIKAMA", "WUTWAR", "ZEMBEDR",
    }

    def _parse_tech_line(line):
        part = line.replace("Technician:", "").strip()
        for code in KNOWN_CODES:
            if part.startswith(code + "-") or part.startswith(code + " "):
                return code, part[len(code):].lstrip("- ").strip()
        dash = part.find("-")
        return (part[:dash].strip(), part[dash+1:].strip()) if dash > 0 else (part, part)

    candidates = sorted(folder.glob("InternalRemakesAnalysis*.xls*"))
    if not candidates:
        return {}

    wb  = xlrd.open_workbook(str(candidates[-1]))
    sh  = wb.sheet_by_index(0)
    out = defaultdict(int)
    current_code = None

    for i in range(25, sh.nrows):
        c0 = str(sh.cell_value(i, 0)).strip()
        if c0.startswith("Technician:"):
            current_code, _ = _parse_tech_line(c0)
            continue
        if not current_code or not c0 or not c0.replace(".", "").isdigit():
            continue
        if year is not None or month is not None:
            date_raw = sh.cell_value(i, 12)
            if not date_raw or not isinstance(date_raw, float):
                continue
            try:
                d = xlrd.xldate_as_datetime(date_raw, wb.datemode)
            except Exception:
                continue
            if year is not None and d.year != year:
                continue
            if month is not None and d.month != month:
                continue
        out[current_code] += 1

    return dict(out)


# ── Shipping & Logistics Report parser ───────────────────────────────────────

def load_shipping_logistics_report(folder: Path) -> "pd.DataFrame":
    """
    Parse 'Daily Shipping & Logistics Report (Custom).csv' into a DataFrame
    with Cases_*-column names compatible with pipeline_logistics.compute_logistics().

    The report is exported from Magic Touch in a wide, repeated-header layout:
      cols 0-2  : repeated report header (ignored)
      cols 3-11 : repeated column-label strings (ignored)
      col  12   : ship-date group header (ignored)
      col  13   : Doctor / Customer Name
      col  14   : Case # (numeric)
      col  15   : Pan #
      col  16   : Current WIP Location
      col  17   : Date In
      col  18   : Ship Date
      col  19   : Due Date
      col  20   : Carrier / Route
      col  21   : Delivery Notes

    Deduplicates on Case # keeping the LAST row (most recently seen location).
    Returns empty DataFrame if no file found.
    """
    import pandas as pd

    import logging
    log_lr = logging.getLogger("mt_reports_parser")
    EXCLUDED_DOCTORS = {"MURRPHY LAWSTON", "MURPHY LAWSTON"}

    # ── Prefer new XLS format (DailyShipping&LogisticsReport.xls) ───────────
    xls_path = folder / "DailyShipping&LogisticsReport.xls"
    if xls_path.exists():
        try:
            import xlrd
            wb  = xlrd.open_workbook(str(xls_path))
            ws  = wb.sheet_by_index(0)
            dm  = wb.datemode

            rows_out = []
            for i in range(ws.nrows):
                row = [ws.cell_value(i, c) for c in range(ws.ncols)]
                # Data rows have a numeric case number > 100000 in col 3
                try:
                    case_num = float(row[3])
                    if case_num < 100000:
                        continue
                except (ValueError, TypeError):
                    continue

                def _xls_dt(val):
                    try:
                        return xlrd.xldate_as_datetime(float(val), dm) if val else None
                    except Exception:
                        return None

                due_str = str(row[14]).split()[0] if row[14] else ""
                rows_out.append({
                    "Cases_CaseNumber":   str(int(case_num)),
                    "Cases_DoctorName":   str(row[0]).strip(),
                    "Cases_PanNumber":    str(row[5]).strip(),
                    "Cases_LastLocation": str(row[7]).strip(),
                    "Cases_DateIn":       _xls_dt(row[10]),
                    "Cases_ShipDate":     _xls_dt(row[12]),
                    "Cases_DueDate":      pd.to_datetime(due_str, errors="coerce") if due_str else None,
                    "Cases_Carrier":      str(row[17]).strip(),
                    "Cases_Notes":        str(row[20]).strip(),
                    "Cases_Status":       "In Production",
                    "Cases_TotalCharge":  0.0,
                    "Cases_CustomerID":   "",
                })

            result = pd.DataFrame(rows_out)
            result = result.drop_duplicates(subset=["Cases_CaseNumber"], keep="last").reset_index(drop=True)

            # Drop dummy accounts
            doctor_norm = result["Cases_DoctorName"].str.upper()
            before = len(result)
            result = result[~doctor_norm.isin(EXCLUDED_DOCTORS)].reset_index(drop=True)
            if before - len(result):
                log_lr.info("load_shipping_logistics_report: dropped %d dummy-account rows", before - len(result))

            log_lr.info("load_shipping_logistics_report: %d unique cases from %s", len(result), xls_path.name)
            return result
        except Exception as exc:
            log_lr.warning("load_shipping_logistics_report: XLS parse failed (%s), falling back to CSV", exc)

    # ── Fallback: legacy CSV format ──────────────────────────────────────────
    pattern = "Daily Shipping & Logistics Report (Custom)*.csv"
    candidates = sorted(folder.glob(pattern))
    if not candidates:
        log_lr.warning(
            "load_shipping_logistics_report: no file matching %s in %s", pattern, folder)
        return pd.DataFrame()

    path = candidates[-1]
    df = pd.read_csv(path, header=None, skiprows=3, encoding="latin-1")

    df[14] = pd.to_numeric(df[14], errors="coerce")
    data = df[df[14].notna()].copy()
    data = data.drop_duplicates(subset=[14], keep="last").reset_index(drop=True)

    doctor_norm = data[13].fillna("").astype(str).str.strip().str.upper()
    before = len(data)
    data = data[~doctor_norm.isin(EXCLUDED_DOCTORS)].reset_index(drop=True)
    if before - len(data):
        log_lr.info("load_shipping_logistics_report: dropped %d dummy-account rows", before - len(data))

    due_raw = data[19].astype(str).str.split().str[0]
    result = pd.DataFrame({
        "Cases_CaseNumber":   data[14].astype(int).astype(str),
        "Cases_DoctorName":   data[13].fillna("").astype(str).str.strip(),
        "Cases_PanNumber":    data[15].fillna("").astype(str).str.strip(),
        "Cases_LastLocation": data[16].fillna("").astype(str).str.strip(),
        "Cases_DateIn":       pd.to_datetime(data[17], errors="coerce"),
        "Cases_ShipDate":     pd.to_datetime(data[18], errors="coerce"),
        "Cases_DueDate":      pd.to_datetime(due_raw, errors="coerce"),
        "Cases_Carrier":      data[20].fillna("").astype(str).str.strip(),
        "Cases_Notes":        data[21].fillna("").astype(str).str.strip(),
        "Cases_Status":       "In Production",
        "Cases_TotalCharge":  0.0,
        "Cases_CustomerID":   "",
    })

    log_lr.info("load_shipping_logistics_report: %d unique cases from %s (CSV fallback)", len(result), path.name)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  11. Employees TimeClock Detail (Export).csv  — daily punch segments
# ═══════════════════════════════════════════════════════════════════════════════

def load_timeclock(folder: Path) -> pd.DataFrame:
    """
    Quirky export: every row repeats the 11 column-label strings
    ('LabName','Employee ID',' FirstName','  LastName','Activity','Date In',
    'Time In','Date Out','Time Out','Total Hours','Comments') as literal values
    in columns 0-10 of EVERY row (not just a header row) — real data follows
    positionally in columns 11-21. There is no usable header row to key off.

    Returns one row per punch segment: employee_id, first_name, last_name,
    activity, date_in, time_in, date_out, time_out, hours (decimal), comments.
    `activity` values seen: Production, Non-Production, Break, Vacation,
    Personal, Bereavement — useful both for hours-worked and as a historical
    PTO/absence cross-check.
    """
    path = folder / "Employees TimeClock Detail (Export).csv"
    if not path.exists():
        # Scheduled exports may land under a slightly different name — take
        # the freshest CSV matching the report's stem.
        candidates = sorted(folder.glob("Employees TimeClock*.csv"),
                            key=lambda p: p.stat().st_mtime)
        if not candidates:
            return pd.DataFrame()
        path = candidates[-1]

    df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python", encoding=_enc(path))
    if df.shape[1] < 22:
        return pd.DataFrame()

    out = df.iloc[:, 11:22].copy()
    out.columns = [
        "lab_name", "employee_id", "first_name", "last_name", "activity",
        "date_in", "time_in", "date_out", "time_out", "total_hours", "comments",
    ]
    out = out[out["employee_id"].str.strip() != ""].copy()

    out["hours"] = out["total_hours"].apply(_hhmm_to_decimal)
    out["date_in"]  = pd.to_datetime(out["date_in"],  errors="coerce")
    out["date_out"] = pd.to_datetime(out["date_out"], errors="coerce")

    return out.drop(columns=["lab_name", "total_hours"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  12. Issued (29).csv  — Clixon materials issued, per department/date
# ═══════════════════════════════════════════════════════════════════════════════

def load_materials_issued(folder: Path) -> pd.DataFrame:
    """
    Clean tabular CSV (UTF-8 BOM), real header row. One row per material
    issuance transaction. Per-department `,Total:,` subtotal rows (blank
    Company/Product, "Total:" in the Issue Date column) are dropped — they're
    redundant with summing the detail rows, kept here only as an implicit
    cross-check opportunity, not loaded as data.

    No employee-code column exists in this report — cost is attributed by
    `Issued Dept.` (department) and free-text `Requested By` (name, not a
    tech_code), so this drives department-level, not per-technician, material
    cost tracking.

    Returns: requested_by, vendor, case_number, department, issue_date, qty,
    issued_value.
    """
    path = folder / "Issued (29).csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python", encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]

    rename = {
        "Requested By":  "requested_by",
        "Vendor":        "vendor",
        "Case #":        "case_number",
        "Issued Dept.":  "department",
        "Issue Date":    "issue_date",
        "Qty":           "qty",
        "Issued Value":  "issued_value",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # Drop subtotal rows: Issue Date literally reads "Total:"
    if "issue_date" in df.columns:
        df = df[df["issue_date"].str.strip() != "Total:"].copy()

    # Drop fully blank rows (no department)
    if "department" in df.columns:
        df = df[df["department"].str.strip() != ""].copy()

    if "qty" in df.columns:
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
    if "issued_value" in df.columns:
        df["issued_value"] = df["issued_value"].apply(_money)
    if "issue_date" in df.columns:
        df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")

    keep = [c for c in ("requested_by", "vendor", "case_number", "department",
                        "issue_date", "qty", "issued_value") if c in df.columns]
    return df[keep].reset_index(drop=True)
