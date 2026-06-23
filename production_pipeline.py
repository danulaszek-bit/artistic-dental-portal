"""
Production Manager dashboard — data pipeline
============================================
Reads Magic Touch Job Management exports from C:\\MT_Reports_Local (synced via
robocopy every 3 minutes) and emits cache/latest/production_data.json.

Sources (all via mt_reports_parser):
  prod_by_dept.xls                       -> departments + products + units
  EmployeeProductivity.xls               -> per-employee units / hours / uph
  Remake by Lab Customer Products.csv    -> remake reasons with units + dollars
  AllCasesByDateIn.csv                   -> daily cases-in/units for monthly trend

Run:  py production_pipeline.py
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

from mt_reports_parser import (
    load_prod_by_dept,
    load_employee_productivity,
    load_remake_by_lab,
    load_remake_reasons,
    load_all_cases_daily,
)

BASE = Path(__file__).parent
CFG  = yaml.safe_load((BASE / "config.yaml").read_text())
MT_FOLDER = Path(CFG["data_source"]["csv"]["watch_folder"])
OUT  = BASE / "cache" / "latest" / "production_data.json"

# ── Business constants (mirrored from the dashboard HTML) ────────────────────
ADJ_IDS = {"REMADJ", "CRNZAD", "CRNEMXADRG", "CRNZIADJUS", "CRNIMPADJ",
           "DENREPCOM", "DENREPSDAY", "DENREPSIM", "CSSAO4REP",
           "CRNDRCALLF", "CRNDRCALLN", "DENDRCALLR"}
EXCLUDED_PRODUCTS = {"DIGMODCADREM", "MODSTOREM", "DIGMODCADFIX"}
EXCLUDED_CODES    = {"SHARSA"}            # Sandra Sharp
EXCL_DEPTS        = {"BUSOFF", "ADMIN", "LOGISTICS", "ABUT/BAR QC"}

# Map raw Magic Touch dept labels → dashboard dept buckets
DTMAP = {
    "C&B": "C&B", "FIXED QC": "C&B", "PORCELAIN": "C&B", "CAD SCANNING": "C&B",
    "STAINGLZ": "C&B", "PRINTING": "C&B", "CAD DESIGN": "C&B", "MILLING": "C&B",
    "DIETRIM": "C&B", "PREP/MODELS - FIXED": "C&B",
    "REMOVABLES": "Removables", "PREP/MODELS - REMOVABLES": "Removables",
    "CHAIRSIDE SERVICES": "Removables",
    "SPLINTS": "SPLINTS", "IMPLANTS": "IMPLANTS", "IMPLANT PT": "IMPLANTS",
    "PARTIAL": "PARTIAL", "SURGICAL GUIDES": "Surgical Guides", "MISC": "Surgical Guides",
    "ORTHO": "Ortho",
}


def sf(v) -> float:
    try:
        return float(re.sub(r"[$,]", "", str(v)).strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def norm_dept(raw: str):
    raw = (raw or "").strip()
    up  = raw.upper()
    if up in EXCL_DEPTS:
        return None
    return DTMAP.get(up, raw)


# ── Departments + products  →  from prod_by_dept.xls ─────────────────────────
def parse_units(report: dict):
    df = load_prod_by_dept(MT_FOLDER)
    depts: dict[str, dict] = {}
    seen_prod: set = set()

    for _, row in df.iterrows():
        dept = norm_dept(str(row.get("department", "")))
        if dept is None:
            continue
        depts.setdefault(dept, {"new": 0, "remakes": 0, "total": 0,
                                "adjustments": 0, "products": [], "adjProducts": []})

        pid  = str(row.get("product_id",  "")).strip()
        desc = str(row.get("description", "")).strip()
        key  = (dept, pid)
        if not pid or key in seen_prod or pid in EXCLUDED_PRODUCTS:
            continue
        seen_prod.add(key)

        new = int(row.get("new_units",    0) or 0)
        rem = int(row.get("remake_units", 0) or 0)
        tot = new + rem
        if tot <= 0:
            continue

        prod = {"id": pid, "desc": desc, "new": new, "remakes": rem, "total": tot}
        if pid in ADJ_IDS:
            depts[dept]["adjProducts"].append(prod)
            depts[dept]["adjustments"] += tot
        else:
            depts[dept]["products"].append(prod)
            depts[dept]["new"]     += new
            depts[dept]["remakes"] += rem
            depts[dept]["total"]   += tot

    # Try to pull period label from the XLS header rows
    period = _read_period_from_xls(MT_FOLDER / "prod_by_dept.xls")
    _now = datetime.now()
    _fmt = f"{_now.month}/{_now.day}/{_now.year}"   # cross-platform M/D/YYYY
    report["period"]      = period or _fmt
    report["depts_count"] = len(depts)
    report["units_total"] = sum(d["total"] for d in depts.values())
    return depts


def _read_period_from_xls(path: Path) -> str | None:
    """Scan first 12 rows for a 'From: … To: …' period label."""
    if not path.exists():
        return None
    try:
        import xlrd
        wb = xlrd.open_workbook(str(path))
        sh = wb.sheet_by_index(0)
        for i in range(min(12, sh.nrows)):
            cell = str(sh.cell_value(i, 0)).strip()
            m = re.search(r"From:\s*([\d/]+)\s*To:\s*([\d/]+)", cell)
            if m:
                return f"{m.group(1)} – {m.group(2)}"
    except Exception:
        pass
    return None


# ── Employee productivity  →  from EmployeeProductivity.xls ──────────────────
def parse_employees(report: dict):
    df = load_employee_productivity(MT_FOLDER)
    emps: list[dict] = []
    seen_codes: set  = set()

    for _, row in df.iterrows():
        code = str(row.get("tech_code", "")).strip()
        if not code or code in EXCLUDED_CODES or code in seen_codes:
            continue
        seen_codes.add(code)

        dept  = norm_dept(str(row.get("department", "")))
        # Prefer production_* (actual bench time) over total_* (includes admin)
        units = int(row.get("production_units", 0) or row.get("total_units", 0) or 0)
        hours = round(float(row.get("production_hours", 0) or row.get("total_hours", 0) or 0), 1)
        uph   = round(float(row.get("uph", 0) or 0), 2)

        emps.append({
            "code":  code,
            "name":  str(row.get("tech_name", "")).strip(),
            "dept":  dept or "",
            "units": units,
            "hours": hours,
            "uph":   uph,
        })

    report["employees_count"] = len(emps)
    return emps


# ── Remake reasons  →  Remake by Lab Customer Products.csv ───────────────────
def parse_reasons(report: dict):
    df = load_remake_by_lab(MT_FOLDER)
    has_units = not df.empty and "units" in df.columns

    if df.empty:
        df        = load_remake_reasons(MT_FOLDER)
        has_units = False

    if df.empty or "remake_reason" not in df.columns:
        report["reasons_count"]    = 0
        report["remake_units_ytd"] = 0
        return []

    df = df[df["remake_reason"].str.strip() != ""].copy()
    reasons: list[dict] = []

    if has_units:
        grp = (
            df.groupby("remake_reason")
              .agg(units=("units", "sum"), amount=("amount", "sum"))
              .reset_index()
              .sort_values("units", ascending=False)
        )
        for _, r in grp.iterrows():
            name = str(r["remake_reason"]).strip()
            if name == "-- No Reason --":
                continue
            reasons.append({
                "name":   name,
                "units":  int(r["units"]),
                "amount": round(float(r["amount"]), 2),
            })
    else:
        # reason-only source: count distinct cases per reason
        grp = (
            df.groupby("remake_reason")
              .size().reset_index(name="count")
              .sort_values("count", ascending=False)
        )
        for _, r in grp.iterrows():
            name = str(r["remake_reason"]).strip()
            if name == "-- No Reason --":
                continue
            reasons.append({"name": name, "units": int(r["count"]), "amount": 0})

    report["reasons_count"]    = len(reasons)
    report["remake_units_ytd"] = sum(x["units"] for x in reasons)
    return reasons


# ── Monthly trend  →  derived from AllCasesByDateIn.csv ──────────────────────
def parse_monthly(report: dict):
    df = load_all_cases_daily(MT_FOLDER)
    if df.empty:
        report["monthly_count"] = 0
        return []

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["ym"] = df["date"].dt.strftime("%Y-%m")

    grp = (
        df.groupby("ym")
          .agg(units=("units_in", "sum"), cases=("cases_in", "sum"))
          .reset_index()
          .sort_values("ym")
    )

    MONTHS = ["","Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"]
    out: list[dict] = []
    for _, r in grp.iterrows():
        ym    = str(r["ym"])
        units = int(r["units"])
        if units <= 0:
            continue
        y, m  = ym.split("-")
        label = f"{MONTHS[int(m)]} {y}"
        out.append({"month": label, "ym": ym, "units": units,
                    "remakes": 0, "remake_rate": 0})

    report["monthly_count"] = len(out)
    return out


def main():
    report:    dict = {}
    depts      = parse_units(report)
    employees  = parse_employees(report)
    reasons    = parse_reasons(report)
    monthly    = parse_monthly(report)

    techs = [{**e, "remakes": 0, "adjustments": 0} for e in employees]

    data = {
        "period":    report["period"],
        "depts":     depts,
        "reasons":   reasons,
        "techs":     techs,
        "daily":     [], "weekly": [], "monthly": monthly,
        "deptWeekly": [], "deptMonthly": [],
        "_generated": datetime.now().isoformat(timespec="seconds"),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, indent=1), encoding="utf-8")

    print("── Production pipeline ─────────────────────────────")
    print(f"source folder      : {MT_FOLDER}")
    print(f"period             : {report['period']}")
    print(f"departments        : {report['depts_count']}")
    print(f"  total units      : {report['units_total']:,}")
    for name, d in depts.items():
        print(f"    {name:<20} new={d['new']:>6,}  rem={d['remakes']:>5,}  "
              f"tot={d['total']:>6,}  adj={d['adjustments']:>4,}  "
              f"prods={len(d['products'])}")
    print(f"employees          : {report['employees_count']}")
    print(f"remake reasons     : {report['reasons_count']}  "
          f"(units YTD={report['remake_units_ytd']:,})")
    if monthly:
        print(f"monthly trend pts  : {report['monthly_count']}  "
              f"({monthly[0]['month']} … {monthly[-1]['month']})")
    else:
        print("monthly trend pts  : 0")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
