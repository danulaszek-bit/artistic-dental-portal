"""
Production Manager / GM Summary / Fixed Dashboard — data pipeline
===================================================================
Reads Magic Touch Job Management exports from C:\\MT_Reports_Local (synced via
robocopy every 3 minutes) and writes flat CSVs into cache/latest/, matching the
convention pipeline.py already uses for the Executive/Logistics pages.

Two separate department-bucket maps are used, because the source reports
report department at two different granularities:

  SALES_DTMAP  — prod_by_dept.xls already groups revenue at roughly
                 Fixed/Removable granularity (e.g. all of Fixed's production
                 collapses to one "C&B" bucket there). This is the only
                 source with dollar figures (Remake Discount, Net Sales), so
                 it drives the $-based remake-rate KPI at that coarser level.

  TECH_DTMAP   — EmployeeProductivity.xls / tech_productivity.csv /
                 InternalRemakesAnalysis.xls report department at the fine
                 technician-station granularity (CAD Design, Milling,
                 Printing, Porcelain, Stain & Glaze, ...). This drives the
                 per-technician / per-sub-area unit tracking that goals are
                 set against.

Goal/PTO figures are NOT baked into these CSVs — they live in goals_store.py
(SQLite) and are joined in live at render time by the Streamlit pages, so a
goal edit or PTO entry is reflected immediately without waiting for the next
pipeline run.

Run:  py production_pipeline.py
"""
from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import yaml

from mt_reports_parser import (
    load_prod_by_dept,
    load_employee_productivity,
    load_tech_productivity,
    load_internal_remakes,
    load_remake_by_lab,
    load_remake_reasons,
    load_all_cases_daily,
)
import goals_store

BASE = Path(__file__).parent
CFG  = yaml.safe_load((BASE / "config.yaml").read_text())
MT_FOLDER = Path(CFG["data_source"]["csv"]["watch_folder"])
OUT_DIR   = BASE / "cache" / "latest"

# ── Sales-side bucket map (prod_by_dept.xls's own department field) ──────────
# Keys are matched case-insensitively.
SALES_DTMAP = {
    "C&B":               "Fixed",
    "CHAIRSIDE SERVICES":"Removable",
    "REMOVABLES":        "Removable",
    "SPLINTS":           "Removable",
    "PARTIAL":           "Removable",
    "ORTHO":             "Removable",
    "IMPLANT PT":        "Removable",
    "IMPLANTS":          "Removable",
    "SURGICAL GUIDES":   "Removable",
    # Excluded (no key -> dropped): '-- Undefined --', 'MISC', 'Sales'
}

# ── Technician-side bucket map (fine station-level department field) ─────────
# value = (dashboard, area). Areas ending in a bucket also used for goals_store.
TECH_DTMAP = {
    "C&B":                      ("Fixed", "Crown & Bridge"),
    "CB GRP":                   ("Fixed", "Crown & Bridge"),
    "CROWN & BRIDGE GROUP":     ("Fixed", "Crown & Bridge"),
    "CAD DESIGN":                ("Fixed", "CAD/CAM"),
    "CAD SCANNING":              ("Fixed", "CAD/CAM"),
    "MILLING":                   ("Fixed", "CAD/CAM"),
    "PRINTING":                  ("Fixed", "CAD/CAM"),
    "CAD ADMIN":                 ("Fixed", "CAD/CAM"),
    "PORCELAIN":                 ("Fixed", "Ceramics"),
    "STAINGLZ":                  ("Fixed", "Ceramics"),
    "PORC GRP":                  ("Fixed", "Ceramics"),
    "CEMENT":                    ("Fixed", "Ceramics"),
    "OPAQUE":                    ("Fixed", "Ceramics"),
    "WAX":                       ("Fixed", "Ceramics"),
    "FINISHING":                 ("Fixed", "Ceramics"),
    "CUSTSHADE":                 ("Fixed", "Ceramics"),
    "SINTER":                    ("Fixed", "Ceramics"),
    "FIXED QC":                  ("Fixed", "Fixed QC"),
    "ABUT/BAR QC":               ("Fixed", "Fixed QC"),          # tentative — confirm with Danny
    "ALLOY":                     ("Fixed", "Crown & Bridge"),    # tentative — confirm with Danny
    "REMOVABLES":                ("Removable", "Removables"),
    "REM GRP":                   ("Removable", "Removables"),
    "PREP/MODELS - REMOVABLES":  ("Removable", "Removables"),
    "CHAIRSIDE SERVICES":        ("Removable", "Chairside"),
    "SPLINTS":                   ("Removable", "Splints"),
    "PARTIAL":                   ("Removable", "Partial"),
    "ORTHO":                     ("Removable", "Ortho"),
    "REMQC":                     ("Removable", "Rem QC"),
    "SURGICAL GUIDES":           ("Removable", "Surgical Guides"),  # tentative — confirm with Danny; large $ bucket
    "IMPLANT PT":                ("Removable", "Implants"),      # tentative — confirm with Danny
    "IMPLANTS":                  ("Removable", "Implants"),      # tentative — confirm with Danny
    "DIETRIM":                   ("GM", "Model/Die"),
    "PREP/MODELS - FIXED":       ("GM", "Model/Die"),
    # Everything else (ADMIN, BUSOFF, LOGISTICS, MISC, Accounting, Customer
    # Service, DELIVERY, Maintenance, Marketing, NON TECHNICAL SUPPORT,
    # Outsource Business Office, Sales, SCHEDULING, Shipping, TRAINING, CTS,
    # '') is intentionally absent -> excluded (non-production / separate project).
}

EXCLUDED_CODES = {"SHARSA"}   # Sandra Sharp — non-production, present in exports


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip()).upper()


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


# ── 1. Department financials (dollars) — from prod_by_dept.xls ───────────────
def build_dept_financials(period: str) -> pd.DataFrame:
    pbd = load_prod_by_dept(MT_FOLDER)
    if pbd.empty:
        return pd.DataFrame()

    pbd["bucket"] = pbd["department"].apply(lambda d: SALES_DTMAP.get(_norm(d)))
    pbd = pbd.dropna(subset=["bucket"])

    fin = (
        pbd.groupby("bucket")
           .agg(new_units=("new_units", "sum"),
                remake_units=("remake_units", "sum"),
                remake_discount=("remake_discount", "sum"),
                net_sales=("net_sales", "sum"))
           .reset_index()
           .rename(columns={"bucket": "dashboard"})
    )
    fin["remake_rate_pct"] = fin.apply(
        lambda r: round(r["remake_discount"] / r["net_sales"] * 100, 2) if r["net_sales"] else 0.0,
        axis=1,
    )
    fin["period"] = period
    return fin


# ── 2. Technician production (units) — from EmployeeProductivity + tech_productivity ─
def build_tech_production(period: str) -> pd.DataFrame:
    emp = load_employee_productivity(MT_FOLDER)
    if emp.empty:
        return pd.DataFrame()
    emp = emp[~emp["tech_code"].isin(EXCLUDED_CODES)].copy()

    tp = load_tech_productivity(MT_FOLDER)
    today = pd.Timestamp(date.today())
    if not tp.empty:
        today_units = tp[tp["completion_date"] == today].groupby("tech_code")["accepted"].sum()
        period_units = tp.groupby("tech_code")["accepted"].sum()
        period_rejected = tp.groupby("tech_code")["rejected"].sum()
        n_days_elapsed = max(tp["completion_date"].nunique(), 1)
    else:
        today_units = period_units = period_rejected = pd.Series(dtype=float)
        n_days_elapsed = 1

    now = datetime.now()
    remakes = load_internal_remakes(MT_FOLDER, year=now.year, month=now.month)
    if not remakes:
        remakes = load_internal_remakes(MT_FOLDER)

    rows = []
    for _, e in emp.iterrows():
        code = str(e["tech_code"]).strip()
        bucket = TECH_DTMAP.get(_norm(e["department"]))
        if not bucket or bucket[0] == "GM":
            continue  # Model/Die has no manager dashboard yet — skip roster/goal tracking
        dashboard, area = bucket
        rows.append({
            "tech_code":       code,
            "name":            e["tech_name"],
            "dashboard":       dashboard,
            "area":            area,
            "station":         str(e["department"]).strip(),
            "period_units":    int(period_units.get(code, 0)),
            "today_units":     int(today_units.get(code, 0)),
            "period_rejected": int(period_rejected.get(code, 0)),
            "avg_daily_units": round(period_units.get(code, 0) / n_days_elapsed, 1),
            "hours":           round(float(e.get("production_hours") or e.get("total_hours") or 0), 1),
            "uph":             round(float(e.get("uph") or 0), 2),
            "internal_remakes":remakes.get(code, 0),
            "period":          period,
        })
    return pd.DataFrame(rows)


# ── 3. Product-level remake rates — from prod_by_dept.xls's product rows ─────
def build_product_remakes(period: str, min_units: int = 5) -> pd.DataFrame:
    pbd = load_prod_by_dept(MT_FOLDER)
    if pbd.empty:
        return pd.DataFrame()

    pbd["bucket"] = pbd["department"].apply(lambda d: SALES_DTMAP.get(_norm(d)))
    pbd = pbd.dropna(subset=["bucket"]).copy()
    pbd["total_units"] = pbd["new_units"] + pbd["remake_units"]
    pbd = pbd[pbd["total_units"] >= min_units]
    if pbd.empty:
        return pd.DataFrame()

    pbd["remake_rate_pct"] = (pbd["remake_units"] / pbd["total_units"] * 100).round(1)
    pbd["period"] = period
    return (
        pbd[["bucket", "product_id", "description", "new_units", "remake_units",
             "total_units", "remake_rate_pct", "period"]]
           .rename(columns={"bucket": "dashboard"})
           .sort_values("remake_rate_pct", ascending=False)
           .reset_index(drop=True)
    )


# ── 4. Top remake reasons — from Remake by Lab (with $/units) or reasons-only fallback ─
def build_top_reasons(period: str, top_n: int = 10) -> pd.DataFrame:
    df = load_remake_by_lab(MT_FOLDER)
    has_units = not df.empty and "units" in df.columns

    if df.empty:
        df = load_remake_reasons(MT_FOLDER)
        has_units = False
    if df.empty or "remake_reason" not in df.columns:
        return pd.DataFrame()

    df = df[df["remake_reason"].str.strip() != ""].copy()
    if df.empty:
        return pd.DataFrame()

    if has_units:
        out = (
            df.groupby("remake_reason")
              .agg(units=("units", "sum"), amount=("amount", "sum"))
              .reset_index()
              .sort_values("units", ascending=False)
        )
    else:
        # reasons-only source: count distinct CASES per reason, not raw rows
        # (a case can have multiple product lines under the same reason).
        out = (
            df.groupby("remake_reason")["case_number"]
              .nunique().reset_index(name="units")
        )
        out["amount"] = 0.0
        out = out.sort_values("units", ascending=False)

    out["period"] = period
    return out.head(top_n).reset_index(drop=True)


# ── 5. Daily snapshot — append (not overwrite) so trend charts build up over time ────
def append_daily_snapshot(fin: pd.DataFrame, techs: pd.DataFrame) -> None:
    """
    Appends one row per dashboard per day to cache/latest/daily_history.csv,
    capturing % of goal (against current goals_store state) and $ remake rate.
    Re-running the pipeline the same day overwrites that day's row rather than
    duplicating it. This is the only way day/week/month trend charts can exist
    going forward — there's no historical equivalent to backfill from.
    """
    if techs.empty:
        return
    hist_path = OUT_DIR / "daily_history.csv"
    today_str = date.today().isoformat()

    rows = []
    for dashboard in ("Fixed", "Removable"):
        sub = techs[techs["dashboard"] == dashboard]
        if sub.empty:
            continue
        total_goal, total_units = 0.0, 0.0
        for _, r in sub.iterrows():
            goal = goals_store.get_current_goal(r["tech_code"])
            if not goal:
                continue
            total_goal += goal
            total_units += r["today_units"]
        pct = round(total_units / total_goal * 100, 1) if total_goal else None

        fin_row = fin[fin["dashboard"] == dashboard] if not fin.empty else pd.DataFrame()
        remake_pct = fin_row["remake_rate_pct"].iloc[0] if not fin_row.empty else None

        rows.append({"date": today_str, "dashboard": dashboard,
                      "pct_of_goal": pct, "remake_rate_pct": remake_pct})

    if not rows:
        return
    new_rows = pd.DataFrame(rows)
    if hist_path.exists():
        existing = pd.read_csv(hist_path)
        existing = existing[~((existing["date"] == today_str) &
                              (existing["dashboard"].isin(new_rows["dashboard"])))]
        combined = pd.concat([existing, new_rows], ignore_index=True)
    else:
        combined = new_rows
    combined.to_csv(hist_path, index=False)


# ── 3. Sync roster + seed default goals into goals_store ─────────────────────
def sync_goals_store(tech_df: pd.DataFrame) -> int:
    if tech_df.empty:
        return 0
    roster = [
        {"tech_code": r["tech_code"], "name": r["name"],
         "dashboard": r["dashboard"], "area": r["area"]}
        for _, r in tech_df.iterrows()
    ]
    goals_store.refresh_roster(roster)

    seeded = 0
    for _, r in tech_df.iterrows():
        if goals_store.get_goal_history(r["tech_code"]):
            continue  # manager has already set (or a prior run already seeded) a goal
        default_goal = max(round(r["avg_daily_units"]), 1) if r["avg_daily_units"] > 0 else 5
        goals_store.set_goal(r["tech_code"], default_goal)
        seeded += 1
    return seeded


# ═══════════════════════════════════════════════════════════════════════════════
#  GM Summary — restored rich baked-HTML rendering
# ═══════════════════════════════════════════════════════════════════════════════
# GM Summary is read-only, so there's no reason it needs the Streamlit-native
# rebuild the Fixed Dashboard genuinely requires for editable goals/PTO. Danny
# preferred the original hand-tuned dashboard (assets/production_dashboard.html,
# same file as _scar_dash_review/artistic-dental-dashboard_38.html — his final
# design-review iteration) over the simplified Streamlit reconstruction, so
# this restores that exact mechanism: parse into the flat per-department shape
# the template expects, bake a `const S = {...}` into the template, serve via
# components.html() from pages/4_GM_Summary.py. Uses the ORIGINAL flat
# department grouping (C&B / Removables / Splints / Partial / Ortho / Implants
# / Surgical Guides), not the Fixed/Removable split — that split is specific to
# the manager-dashboard goal-tracking use case, not this company-wide view.

GM_ADJ_IDS = {"REMADJ", "CRNZAD", "CRNEMXADRG", "CRNZIADJUS", "CRNIMPADJ",
              "DENREPCOM", "DENREPSDAY", "DENREPSIM", "CSSAO4REP",
              "CRNDRCALLF", "CRNDRCALLN", "DENDRCALLR"}
GM_EXCLUDED_PRODUCTS = {"DIGMODCADREM", "MODSTOREM", "DIGMODCADFIX"}
GM_EXCL_DEPTS = {"BUSOFF", "ADMIN", "LOGISTICS", "ABUT/BAR QC"}

GM_DTMAP = {
    "C&B": "C&B", "FIXED QC": "C&B", "PORCELAIN": "C&B", "CAD SCANNING": "C&B",
    "STAINGLZ": "C&B", "PRINTING": "C&B", "CAD DESIGN": "C&B", "MILLING": "C&B",
    "DIETRIM": "C&B", "PREP/MODELS - FIXED": "C&B",
    "REMOVABLES": "Removables", "PREP/MODELS - REMOVABLES": "Removables",
    "CHAIRSIDE SERVICES": "Removables",
    "SPLINTS": "SPLINTS", "IMPLANTS": "IMPLANTS", "IMPLANT PT": "IMPLANTS",
    "PARTIAL": "PARTIAL", "SURGICAL GUIDES": "Surgical Guides", "MISC": "Surgical Guides",
    "ORTHO": "Ortho",
}


def gm_norm_dept(raw: str):
    raw = (raw or "").strip()
    up = raw.upper()
    if up in GM_EXCL_DEPTS:
        return None
    return GM_DTMAP.get(up, raw)


def gm_parse_units(report: dict):
    df = load_prod_by_dept(MT_FOLDER)
    depts: dict[str, dict] = {}
    seen_prod: set = set()

    for _, row in df.iterrows():
        dept = gm_norm_dept(str(row.get("department", "")))
        if dept is None:
            continue
        depts.setdefault(dept, {"new": 0, "remakes": 0, "total": 0,
                                "adjustments": 0, "products": [], "adjProducts": []})

        pid  = str(row.get("product_id", "")).strip()
        desc = str(row.get("description", "")).strip()
        key  = (dept, pid)
        if not pid or key in seen_prod or pid in GM_EXCLUDED_PRODUCTS:
            continue
        seen_prod.add(key)

        new = int(row.get("new_units", 0) or 0)
        rem = int(row.get("remake_units", 0) or 0)
        tot = new + rem
        if tot <= 0:
            continue

        prod = {"id": pid, "desc": desc, "new": new, "remakes": rem, "total": tot}
        if pid in GM_ADJ_IDS:
            depts[dept]["adjProducts"].append(prod)
            depts[dept]["adjustments"] += tot
        else:
            depts[dept]["products"].append(prod)
            depts[dept]["new"]     += new
            depts[dept]["remakes"] += rem
            depts[dept]["total"]   += tot

    period = _read_period_from_xls(MT_FOLDER / "prod_by_dept.xls")
    _now = datetime.now()
    report["period"]      = period or f"{_now.month}/{_now.day}/{_now.year}"
    report["depts_count"] = len(depts)
    report["units_total"] = sum(d["total"] for d in depts.values())
    return depts


def gm_parse_employees(report: dict):
    df = load_employee_productivity(MT_FOLDER)
    emps: list[dict] = []
    seen_codes: set = set()

    for _, row in df.iterrows():
        code = str(row.get("tech_code", "")).strip()
        if not code or code in EXCLUDED_CODES or code in seen_codes:
            continue
        seen_codes.add(code)

        dept  = gm_norm_dept(str(row.get("department", "")))
        units = int(row.get("production_units", 0) or row.get("total_units", 0) or 0)
        hours = round(float(row.get("production_hours", 0) or row.get("total_hours", 0) or 0), 1)
        uph   = round(float(row.get("uph", 0) or 0), 2)

        emps.append({
            "code":  code, "name": str(row.get("tech_name", "")).strip(),
            "dept":  dept or "", "units": units, "hours": hours, "uph": uph,
        })

    report["employees_count"] = len(emps)
    return emps


def gm_parse_reasons(report: dict):
    df = load_remake_by_lab(MT_FOLDER)
    has_units = not df.empty and "units" in df.columns

    if df.empty:
        df = load_remake_reasons(MT_FOLDER)
        has_units = False

    if df.empty or "remake_reason" not in df.columns:
        report["reasons_count"] = 0
        report["remake_units_ytd"] = 0
        return []

    df = df[df["remake_reason"].str.strip() != ""].copy()
    reasons: list[dict] = []

    if has_units:
        grp = (
            df.groupby("remake_reason")
              .agg(units=("units", "sum"), amount=("amount", "sum"))
              .reset_index().sort_values("units", ascending=False)
        )
        for _, r in grp.iterrows():
            name = str(r["remake_reason"]).strip()
            if name == "-- No Reason --":
                continue
            reasons.append({"name": name, "units": int(r["units"]),
                            "amount": round(float(r["amount"]), 2)})
    else:
        # reasons-only source: count distinct CASES per reason, not raw rows
        # (a case can have multiple product lines under the same reason).
        grp = (
            df.groupby("remake_reason")["case_number"]
              .nunique().reset_index(name="count")
              .sort_values("count", ascending=False)
        )
        for _, r in grp.iterrows():
            name = str(r["remake_reason"]).strip()
            if name == "-- No Reason --":
                continue
            reasons.append({"name": name, "units": int(r["count"]), "amount": 0})

    report["reasons_count"] = len(reasons)
    report["remake_units_ytd"] = sum(x["units"] for x in reasons)
    return reasons


def gm_parse_monthly(report: dict):
    df = load_all_cases_daily(MT_FOLDER)
    if df.empty:
        report["monthly_count"] = 0
        return []

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["ym"] = df["date"].dt.strftime("%Y-%m")

    grp = (
        df.groupby("ym").agg(units=("units_in", "sum"), cases=("cases_in", "sum"))
          .reset_index().sort_values("ym")
    )
    MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    out: list[dict] = []
    for _, r in grp.iterrows():
        ym = str(r["ym"])
        units = int(r["units"])
        if units <= 0:
            continue
        y, m = ym.split("-")
        out.append({"month": f"{MONTHS[int(m)]} {y}", "ym": ym, "units": units,
                    "remakes": 0, "remake_rate": 0})

    report["monthly_count"] = len(out)
    return out


def gm_render_live_html(depts: dict, employees: list, reasons: list, report: dict,
                        techs: list | None = None):
    """
    Read assets/production_dashboard.html (the template — same file as
    Danny's _scar_dash_review/artistic-dental-dashboard_38.html), inject
    current-period data into `const S` plus a MONTHLY_DEPT entry, write to
    assets/production_dashboard_live.html. pages/4_GM_Summary.py serves the
    result via components.html() — no runtime JS injection needed.
    """
    import re as _re
    import json as _json

    template = BASE / "assets" / "production_dashboard.html"
    out_path = BASE / "assets" / "production_dashboard_live.html"
    if not template.exists():
        print("gm_render_live_html: template not found, skipping")
        return

    html = template.read_text(encoding="utf-8")
    _now = datetime.now()
    month_key   = f"{_now.year}-{_now.month:02d}"
    month_label = _now.strftime("%B %Y")

    if techs is None:
        techs = [{
            "code": e.get("code", ""), "name": e.get("name", ""),
            "dept": e.get("dept", "") or "", "units": int(e.get("units", 0) or 0),
            "remakes": 0, "adjustments": 0,
            "hours": round(float(e.get("hours", 0) or 0), 1),
            "uph": round(float(e.get("uph", 0) or 0), 2),
        } for e in employees]

    s_obj = {
        "depts": depts, "techs": techs, "reasons": reasons,
        "products": [], "adjProducts": [], "daily": [], "weekly": [],
        "monthly": [], "deptWeekly": [], "deptMonthly": [], "period": report["period"],
    }
    s_json = _json.dumps(s_obj, separators=(",", ":"))
    html = _re.sub(
        r"const S=\{depts:\{\},.+?period:'No data loaded'\};",
        f"const S={s_json};", html, flags=_re.DOTALL,
    )

    techs_by_dept: dict[str, list] = {}
    for t in techs:
        d = (t.get("dept") or "").strip()
        if d and int(t.get("units", 0) or 0) > 0:
            techs_by_dept.setdefault(d, []).append({
                "name": t.get("name", ""), "u": int(t.get("units", 0) or 0),
                "r": int(t.get("remakes", 0) or 0), "a": int(t.get("adjustments", 0) or 0),
            })

    cur_month: dict = {}
    for dept_name, dd in depts.items():
        if dept_name in ("-- Undefined --", "Sales") or not dd.get("total", 0):
            continue
        prods = [
            {"id": p["id"], "desc": (p["desc"] or "")[:30],
             "u": p["new"] + p["remakes"], "r": p["remakes"]}
            for p in dd.get("products", [])
            if p.get("new", 0) + p.get("remakes", 0) > 0
        ][:8]
        cur_month[dept_name] = {
            "units": dd["total"], "remakes": dd["remakes"], "adj": dd["adjustments"],
            "products": prods, "techs": techs_by_dept.get(dept_name, []),
        }

    if cur_month:
        new_entry = f'"{month_key}":{_json.dumps(cur_month, separators=(",",":"))}'
        marker = "const MONTHLY_DEPT = {"
        idx = html.find(marker)
        if idx != -1:
            end = html.index("};", idx) + 2
            old_block = html[idx:end]
            old_block = _re.sub(
                rf',\s*"{month_key}":\{{.*?\}}(?=\s*[,}}])', "", old_block, flags=_re.DOTALL,
            )
            new_block = old_block[:-2] + "," + new_entry + "};"
            html = html[:idx] + new_block + html[end:]

    html = _re.sub(
        r"(const MONTH_LABELS = \{[^}]*)\}",
        lambda m: (m.group(0) if f"'{month_key}'" in m.group(0)
                  else m.group(1) + f",'{month_key}':'{month_label}'" + "}"),
        html,
    )

    auto_close = (
        "\n<script>\ndocument.addEventListener('DOMContentLoaded',function(){\n"
        "  if(Object.keys(S.depts).length>0){closeUpload();}\n});\n</script>\n"
    )
    html = html.replace("</body>", auto_close + "</body>", 1)

    out_path.write_text(html, encoding="utf-8")
    print(f"rendered GM Summary HTML -> {out_path.name}  ({out_path.stat().st_size:,} bytes)")


def build_gm_summary_html():
    report: dict = {}
    depts     = gm_parse_units(report)
    employees = gm_parse_employees(report)
    reasons   = gm_parse_reasons(report)
    gm_parse_monthly(report)  # currently unused by the template's live-data path; kept for parity

    _now = datetime.now()
    real_remakes = load_internal_remakes(MT_FOLDER, year=_now.year, month=_now.month)
    if not real_remakes:
        real_remakes = load_internal_remakes(MT_FOLDER)
    techs = [{**e, "remakes": real_remakes.get(e["code"], 0), "adjustments": 0}
             for e in employees]

    gm_render_live_html(depts, employees, reasons, report, techs)
    return report, depts, employees, reasons


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _today = date.today()
    period = _read_period_from_xls(MT_FOLDER / "prod_by_dept.xls") or f"{_today.month}/{_today.day}/{_today.year}"

    fin = build_dept_financials(period)
    fin_path = OUT_DIR / "dept_financials.csv"
    fin.to_csv(fin_path, index=False)

    techs = build_tech_production(period)
    techs_path = OUT_DIR / "tech_production.csv"
    techs.to_csv(techs_path, index=False)

    products = build_product_remakes(period)
    products_path = OUT_DIR / "product_remakes.csv"
    products.to_csv(products_path, index=False)

    reasons = build_top_reasons(period)
    reasons_path = OUT_DIR / "top_reasons.csv"
    reasons.to_csv(reasons_path, index=False)

    seeded = sync_goals_store(techs)
    append_daily_snapshot(fin, techs)

    # Labor estimates → local SQLite only (labor_history). Deliberately NOT a
    # cache/latest CSV: pay-derived figures must never reach the committed/
    # cloud-visible artifacts.
    try:
        import labor_calc
        labor_rows, unrated_combos = labor_calc.persist_estimates(MT_FOLDER)
    except Exception as exc:
        print(f"WARNING: labor estimate pass failed ({exc}) - continuing")
        labor_rows, unrated_combos = 0, 0

    gm_report, gm_depts, gm_employees, gm_reasons = build_gm_summary_html()

    print("-- Production pipeline ---------------------------")
    print(f"source folder      : {MT_FOLDER}")
    print(f"period             : {period}")
    if not fin.empty:
        print("dept financials    :")
        for _, r in fin.iterrows():
            print(f"    {r['dashboard']:<10} net_sales=${r['net_sales']:>10,.0f}  "
                  f"remake=${r['remake_discount']:>8,.0f}  rate={r['remake_rate_pct']:>5.1f}%")
    else:
        print("dept financials    : none (prod_by_dept.xls not found or empty)")
    if not techs.empty:
        print(f"technicians        : {len(techs)}  "
              f"(Fixed={sum(techs['dashboard']=='Fixed')}, "
              f"Removable={sum(techs['dashboard']=='Removable')})")
    else:
        print("technicians        : none")
    print(f"goals seeded       : {seeded} (new technicians with no goal history yet)")
    print(f"product remake rows: {len(products)}")
    print(f"top reasons rows   : {len(reasons)}")
    print(f"\nwrote {fin_path}")
    print(f"wrote {techs_path}")
    print(f"wrote {products_path}")
    print(f"wrote {reasons_path}")
    print(f"appended cache/latest/daily_history.csv")
    print(f"GM Summary depts   : {gm_report.get('depts_count', 0)}  "
          f"(employees={gm_report.get('employees_count', 0)}, "
          f"reasons={gm_report.get('reasons_count', 0)})")
    print(f"labor estimates    : {labor_rows} day-rows upserted (local DB), "
          f"{unrated_combos} unrated piece-task combo(s)")


if __name__ == "__main__":
    main()
