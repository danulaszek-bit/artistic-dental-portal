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

    net = df["invoice_total"].fillna(0) - df.get("sales_discount", pd.Series(0, index=df.index)).fillna(0)
    df["net_sales"] = net

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
    Hierarchical report — first 4 rows are header, data starts at row 4.
    Returns one row per open case.
    """
    path = folder / "WIP_cases.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python",
                     encoding=_enc(path))

    rows = []
    for i, row in df.iterrows():
        if i < 4:
            continue
        vals = row.tolist()
        # Data rows have a numeric case number in col 1
        case_num = str(vals[1]).strip() if len(vals) > 1 else ""
        if not case_num.isdigit():
            continue
        rows.append({
            "ship_date":    str(vals[0]).strip() if len(vals) > 0 else "",
            "case_number":  case_num,
            "pan_number":   str(vals[2]).strip() if len(vals) > 2 else "",
            "account_id":   str(vals[3]).strip() if len(vals) > 3 else "",
            "customer_name":str(vals[4]).strip() if len(vals) > 4 else "",
            "doctor_name":  str(vals[5]).strip() if len(vals) > 5 else "",
            "patient_name": str(vals[6]).strip() if len(vals) > 6 else "",  # PHI
            "date_in":      str(vals[7]).strip() if len(vals) > 7 else "",
            "due_date":     str(vals[8]).strip() if len(vals) > 8 else "",
            "total_charge": _money(vals[9]) if len(vals) > 9 else 0.0,
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
    Hierarchical report. Location group headers (Col 0 starts with 'Location:')
    define the LastLocation carried forward onto each case row below them.
    Case rows: Col 0 = 'TechCode: TechName'.
    Returns one row per open case with last_location and status.
    """
    path = folder / "case_location.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python")

    rows = []
    current_location = ""

    for _, row in df.iterrows():
        vals = [str(v).strip() for v in row.tolist()]
        c0 = vals[0] if vals else ""

        # Location header row
        if c0.startswith("Location:"):
            current_location = c0.replace("Location:", "").split(",")[0].strip()
            continue

        # Case data row: col 0 = "CODE: Name" or "CODE : Name"
        if ":" in c0 and len(vals) > 1:
            case_num = str(vals[1]).strip() if len(vals) > 1 else ""
            if not case_num.isdigit():
                continue
            tech_parts = c0.split(":", 1)
            tech_code  = tech_parts[0].strip()
            tech_name  = tech_parts[1].strip() if len(tech_parts) > 1 else ""
            rows.append({
                "case_number":    case_num,
                "tech_code":      tech_code,
                "tech_name":      tech_name,
                "age_days":       str(vals[2]).strip() if len(vals) > 2 else "",
                "date_in":        str(vals[3]).strip() if len(vals) > 3 else "",
                "due_date":       str(vals[4]).strip() if len(vals) > 4 else "",
                "ship_date":      str(vals[5]).strip() if len(vals) > 5 else "",
                "units":          str(vals[6]).strip() if len(vals) > 6 else "",
                "last_scan_date": str(vals[7]).strip() if len(vals) > 7 else "",
                "account_id":     str(vals[8]).strip() if len(vals) > 8 else "",
                "patient_name":   str(vals[9]).strip() if len(vals) > 9 else "",  # PHI
                "pan_number":     str(vals[11]).strip() if len(vals) > 11 else "",
                "status":         str(vals[12]).strip() if len(vals) > 12 else "",
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

def build_unified_wip(folder: Path) -> pd.DataFrame:
    """
    Join WIP_cases.csv (open case list) with case_location.csv (location/status)
    on case_number. Outputs a DataFrame with Cases_* column names so that
    pipeline_logistics.py works without modification.

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
    Hierarchical 3-column layout:
      Reason group header:  ['Remake Reason: Contacts']
      Customer row:         ['FELNIC', 'Nick Feller']
      Case row:             ['451669', 'Thomas Lewis']
    Returns one row per case with remake_reason, account_id, case_number,
    customer_name, patient_name (PHI).
    """
    path = folder / "remake_reasons.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python")

    rows = []
    current_reason   = ""
    current_cust_id  = ""
    current_cust_name= ""

    for _, row in df.iterrows():
        vals = [str(v).strip() for v in row.tolist() if str(v).strip()]
        if not vals:
            continue
        c0 = vals[0]

        # Reason group header
        if c0.startswith("Remake Reason:"):
            current_reason = c0.replace("Remake Reason:", "").strip()
            current_cust_id = current_cust_name = ""
            continue

        # Distinguish customer row (alpha ID) from case row (numeric)
        if len(vals) >= 2:
            if not c0.replace("-","").replace("_","").isdigit():
                # Customer row
                current_cust_id   = c0
                current_cust_name = vals[1]
            elif c0.isdigit():
                # Case row
                rows.append({
                    "remake_reason": current_reason,
                    "account_id":    current_cust_id,
                    "customer_name": current_cust_name,
                    "case_number":   c0,
                    "patient_name":  vals[1],   # PHI
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

    def _hours_to_decimal(s: str) -> float:
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

        # Employee row: "CODE - Last, First"
        if " - " in c0 and not c0.startswith("Department:") and row[1].replace(",","").replace(".","").isdigit():
            if current_emp:
                rows.append(current_emp)
            code, name = c0.split(" - ", 1)
            current_emp = {
                "tech_code":       code.strip(),
                "tech_name":       name.strip(),
                "department":      current_dept,
                "total_units":     _money(row[1]),
                "total_hours":     _hours_to_decimal(row[2]),
                "uph":             _money(row[3]),
                "production_units":0.0,
                "production_hours":0.0,
            }
            continue

        # Time-category sub-rows
        if current_emp and c0 == "Production":
            current_emp["production_units"] = _money(row[1])
            current_emp["production_hours"] = _hours_to_decimal(row[2])

    if current_emp:
        rows.append(current_emp)

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    # Recompute UPH from production hours where available
    out["uph"] = (
        out["production_units"] / out["production_hours"].replace(0, pd.NA)
    ).fillna(out["uph"]).round(2)
    return out


# ═══════════════════════════════════════════════════════════════════════════════
#  10. prod_by_dept.xls  — sales by product department (units + dollars)
# ═══════════════════════════════════════════════════════════════════════════════

def load_prod_by_dept(folder: Path) -> pd.DataFrame:
    """
    XLS hierarchical report. Structure:
      Row 8:  Column headers
      Dept header rows: Col 0 starts with 'Department:'
      Product rows:     Col 0 = 'PRODUCTID-Description'
      Subtotal rows:    Col 0 is numeric — skip

    Column map (0-indexed):
      0  = ProductID-Description  (split on first '-')
      1  = # of Invoices
      2  = New Units
      3  = Remake Units
      4  = Credit Units
      5  = Credit Discount
      6  = Sales Discount
      7  = Remake Discount
      8  = Metal Charges
      9  = Total Tax
      10 = Total Invoiced
      11 = Net Sales
    """
    import xlrd
    path = folder / "prod_by_dept.xls"
    if not path.exists():
        return pd.DataFrame()

    wb = xlrd.open_workbook(str(path))
    sh = wb.sheet_by_index(0)

    rows = []
    current_dept = ""

    for i in range(sh.nrows):
        row = [str(sh.cell_value(i, c)).strip() for c in range(sh.ncols)]
        c0  = row[0]

        if c0.startswith("Department:"):
            current_dept = c0.replace("Department:", "").strip()
            continue

        # Skip subtotal rows (col 0 is pure numeric)
        if c0.replace(",", "").replace(".", "").isdigit():
            continue

        # Product row: must contain a dash and not look like a header
        if "-" not in c0 or c0.startswith("Product") or c0.startswith("Invoicing"):
            continue

        pid, desc = c0.split("-", 1)
        pid, desc = pid.strip(), desc.strip()
        if not pid:
            continue

        rows.append({
            "department":      current_dept,
            "product_id":      pid,
            "description":     desc,
            "num_invoices":    _money(row[1]) if len(row) > 1 else 0,
            "new_units":       _money(row[2]) if len(row) > 2 else 0,
            "remake_units":    _money(row[3]) if len(row) > 3 else 0,
            "credit_units":    _money(row[4]) if len(row) > 4 else 0,
            "credit_discount": _money(row[5]) if len(row) > 5 else 0,
            "sales_discount":  _money(row[6]) if len(row) > 6 else 0,
            "remake_discount": _money(row[7]) if len(row) > 7 else 0,
            "metal_charges":   _money(row[8]) if len(row) > 8 else 0,
            "total_tax":       _money(row[9]) if len(row) > 9 else 0,
            "total_invoiced":  _money(row[10]) if len(row) > 10 else 0,
            "net_sales":       _money(row[11]) if len(row) > 11 else 0,
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
#  11. AllCasesByDateIn.csv  — daily aggregate: cases in / units / revenue
# ═══════════════════════════════════════════════════════════════════════════════

def load_all_cases_daily(folder: Path) -> pd.DataFrame:
    """
    Parses the daily aggregate report. Each data row looks like:
      'Cases In on 1/2/2026:  35'  |  'Units In on 1/2/2026:  56.00'  |  'Total for Date: $5,267.95'
    Returns one row per date: date, cases_in, units_in, total_revenue.
    """
    path = folder / "AllCasesByDateIn.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python")

    rows = []
    for _, row in df.iterrows():
        vals = row.tolist()
        c0 = str(vals[0]).strip() if vals else ""
        if not c0.startswith("Cases In on"):
            continue
        # Extract date and count from col 0
        m0 = re.search(r"Cases In on ([\d/]+).*?(\d+)\s*$", c0)
        if not m0:
            continue
        date_str  = m0.group(1)
        cases_in  = int(m0.group(2))

        units_in  = 0.0
        total_rev = 0.0
        if len(vals) > 1:
            m1 = re.search(r"([\d,.]+)\s*$", str(vals[1]))
            if m1:
                units_in = float(m1.group(1).replace(",", ""))
        if len(vals) > 2:
            m2 = re.search(r"\$([\d,.]+)", str(vals[2]))
            if m2:
                total_rev = float(m2.group(1).replace(",", ""))

        rows.append({
            "date":          pd.to_datetime(date_str, errors="coerce"),
            "cases_in":      cases_in,
            "units_in":      units_in,
            "total_revenue": total_rev,
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
#  12. Remakes.csv  — daily aggregate remake counts
# ═══════════════════════════════════════════════════════════════════════════════

def load_remakes_daily(folder: Path) -> pd.DataFrame:
    """
    Parses daily remake count aggregate:
      'Date In: 4/1/2026'
      'Remake Cases In for 4/1/2026:  10'
    Returns one row per date: date, remake_cases.
    """
    path = folder / "Remakes.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, header=None, dtype=str, keep_default_na=False,
                     on_bad_lines="skip", engine="python")

    rows = []
    current_date = None
    for _, row in df.iterrows():
        vals = row.tolist()
        c0 = str(vals[0]).strip()
        if c0.startswith("Date In:"):
            date_str = c0.replace("Date In:", "").strip()
            current_date = pd.to_datetime(date_str, errors="coerce")
        elif c0.startswith("Remake Cases In for") and current_date is not None:
            m = re.search(r"(\d+)\s*$", c0)
            count = int(m.group(1)) if m else 0
            rows.append({"date": current_date, "remake_cases": count})
            current_date = None

    return pd.DataFrame(rows) if rows else pd.DataFrame()
