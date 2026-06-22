"""
Artistic Dental Studio -- Monitor Dashboards Server

One Flask process serves a dashboard per department. Each dashboard is meant
to be displayed full-screen on a TV / monitor in that department's area.

Data sources
------------
* cache/latest/cases_logistics.csv -- open WIP cases with Pan #, Last Location,
                                      Due Date, Status, pseudo_dept.
* cache/latest/remakes_full.csv    -- used to flag which open cases are remakes.
* cache/case_history.csv           -- used to compute the 30-day on-time meter.
* product_type_map.yaml            -- user-editable Pan-prefix and Last-Location
                                      -> Product Type mapping.

Run:
    cd C:\\ArtisticDentalPortal\\monitor_dashboards
    pip install flask pandas pyyaml
    python serve_dashboards.py

Then point TVs at:
    http://<this-pc-ip>:8080/                index of dashboards
    http://<this-pc-ip>:8080/dept/Removable  Removable department
"""

from __future__ import annotations

import os
import socket
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from flask import Flask, jsonify, render_template, abort

from dashboard_history import (
    load_due_date_history,
    case_due_date_manipulated,
    count_snapshots,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent
PORTAL_ROOT = HERE.parent
CACHE_DIR = PORTAL_ROOT / "cache"
LATEST_DIR = CACHE_DIR / "latest"
CASE_HISTORY = CACHE_DIR / "case_history.csv"
PRODUCT_MAP_YAML = HERE / "product_type_map.yaml"
LIVE_EXPORTS    = PORTAL_ROOT / "live_exports"       # legacy path (kept for reference)
MT_REPORTS_LOCAL = Path("C:/MT_Reports_Local")       # new robocopy-synced source

# Source data is considered "stale" once the freshest feed is older than this.
# Exports run roughly every 2 hours during the day; 4h leaves slack for a
# missed run before the floor sees a warning. Overnight gaps will trip this
# until the morning export -- raise it if that's noisy.
STALE_THRESHOLD_SECONDS = 4 * 3600

# ---------------------------------------------------------------------------
# Department configuration
# ---------------------------------------------------------------------------
# pseudo_dept values come from pipeline_logistics.py.
DEPARTMENTS = {
    "Removable": {
        "title": "Removable",
        "subtitle": "Dentures, Partials, Try-Ins",
        "pseudo_dept": "Removable",
    },
    # When ready, duplicate one of these:
    # "Fixed":    {"title": "Fixed / C&B", "subtitle": "Crown and Bridge", "pseudo_dept": "Fixed"},
    # "Implants": {"title": "Implants",    "subtitle": "Porcelain and Milling", "pseudo_dept": "Implants"},
    # "CAD_CAM":  {"title": "CAD / CAM",   "subtitle": "Digital fabrication",   "pseudo_dept": "CAD_CAM"},
    # "Surgical": {"title": "Surgical",    "subtitle": "Guides, CT, Scanning",  "pseudo_dept": "Surgical"},
}

# ---------------------------------------------------------------------------
# Product-type mapping (loaded fresh each request so edits take effect)
# ---------------------------------------------------------------------------
def load_product_type_map() -> dict:
    if not PRODUCT_MAP_YAML.exists():
        return {"pan_prefix": {}, "last_location": {}}
    with PRODUCT_MAP_YAML.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    cfg.setdefault("pan_prefix", {})
    cfg.setdefault("last_location", {})
    cfg["pan_prefix"]    = {str(k).upper(): v for k, v in cfg["pan_prefix"].items()}
    cfg["last_location"] = {str(k).upper(): v for k, v in cfg["last_location"].items()}
    return cfg


def derive_product_type(pan_number: str, last_location: str, cfg: dict) -> str:
    """Pan-prefix wins; then Last Location; else dash."""
    pan = str(pan_number or "").strip().upper()
    loc = str(last_location or "").strip().upper()

    pfx_map = cfg["pan_prefix"]
    # Longer prefixes first (e.g. 'EVDSGN' before 'E').
    for k in sorted(pfx_map.keys(), key=len, reverse=True):
        if pan.startswith(k):
            return pfx_map[k]
    if loc in cfg["last_location"]:
        return cfg["last_location"][loc]
    return "-"


# ---------------------------------------------------------------------------
# Business-day helpers
# ---------------------------------------------------------------------------
def next_business_day(d):
    """Return the next weekday (Mon-Fri) after d. d is a date or Timestamp."""
    bd = pd.tseries.offsets.BusinessDay(1)
    return (pd.Timestamp(d).normalize() + bd).date()


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------
def load_open_cases() -> pd.DataFrame:
    """Read cache/latest/cases_logistics.csv as the source of open cases."""
    path = LATEST_DIR / "cases_logistics.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    df = pd.read_csv(path)
    df["Cases_DueDate"] = pd.to_datetime(df["Cases_DueDate"], errors="coerce")
    df["Cases_DateIn"]  = pd.to_datetime(df["Cases_DateIn"],  errors="coerce")
    df["Cases_TotalCharge"] = pd.to_numeric(df.get("Cases_TotalCharge"), errors="coerce").fillna(0.0)
    return df


def load_open_remake_case_numbers() -> set:
    """Return the set of case_numbers that are open AND remakes."""
    path = LATEST_DIR / "remakes_full.csv"
    if not path.exists():
        return set()
    rf = pd.read_csv(path)
    open_states = {"In Production", "Invoiced TryIn", "On Hold",
                   "Sent for TryIn", "Submitted", "Outsourced"}
    open_rf = rf[rf["status"].astype(str).isin(open_states)]
    return set(open_rf["case_number"].astype(str).str.strip())


def compute_on_time_pct() -> dict:
    """
    On-time shipping % for the last 30 days.

    Definition (matches pipeline.py.compute_on_time_ship):
        actual  ship date = Cases_ShipmentDate (fallback Cases_InvoiceDate)
        planned ship date = Cases_ShipDate
        met_plan          = actual <= planned

    Plus a secondary filter: even if the case met its plan, if the planned
    DUE date was changed within 3 days of the original (from snapshot
    history), we count it as late -- the deadline was moved near the wire.

    Snapshots only carry due_date history today, so manipulation detection
    works on due_date.  Once Cases_ShipDate is added to wip_detail.csv
    snapshots, ship_date manipulation can also be detected directly.
    """
    out = {"on_time_pct": None, "shipped": 0, "on_time": 0, "late": 0,
           "manipulated": 0, "missed_plan": 0, "window_days": 30,
           "snapshots_available": count_snapshots(CACHE_DIR),
           "source_file_present": False,
           "as_of": datetime.now().strftime("%Y-%m-%d %H:%M")}

    src = PORTAL_ROOT / "live_exports" / "Active_30_day.csv"
    if not src.exists():
        return out
    out["source_file_present"] = True

    try:
        df = pd.read_csv(src, encoding="latin-1", engine="python",
                         on_bad_lines="skip")
    except Exception:
        return out

    if "Cases_CaseNumber" in df.columns:
        df = df.drop_duplicates(subset=["Cases_CaseNumber"], keep="first").copy()

    df["plan"] = pd.to_datetime(df.get("Cases_ShipDate"),    errors="coerce").dt.normalize()
    df["inv"]  = pd.to_datetime(df.get("Cases_InvoiceDate"), errors="coerce").dt.normalize()
    if "Cases_ShipmentDate" in df.columns:
        df["ship"] = pd.to_datetime(df["Cases_ShipmentDate"], errors="coerce").dt.normalize()
    else:
        df["ship"] = pd.NaT
    df["actual"] = df["ship"].fillna(df["inv"])

    today  = pd.Timestamp.today().normalize()
    cutoff = today - pd.Timedelta(days=30)
    win = df.dropna(subset=["plan", "actual"])
    win = win[(win["actual"] >= cutoff) & (win["actual"] <= today)].copy()
    if win.empty:
        return out

    met_plan = win["actual"] <= win["plan"]

    history = load_due_date_history(CACHE_DIR)
    win["__cn"] = win["Cases_CaseNumber"].astype(str).str.strip()
    manipulated = win["__cn"].map(
        lambda cn: case_due_date_manipulated(history.get(cn, []), threshold_days=3)
    )

    on_time = met_plan & ~manipulated
    total = len(win)
    out.update({
        "on_time_pct": round(100.0 * int(on_time.sum()) / total, 1),
        "shipped":     total,
        "on_time":     int(on_time.sum()),
        "late":        total - int(on_time.sum()),
        "manipulated": int(manipulated.sum()),
        "missed_plan": int((~met_plan).sum()),
    })
    return out


# ---------------------------------------------------------------------------
# Data freshness (staleness banner)
# ---------------------------------------------------------------------------
def _file_age(path: Path) -> dict:
    if not path.exists():
        return {"present": False, "mtime": None, "age_seconds": None}
    mt = path.stat().st_mtime
    age = max(0, int(time.time() - mt))
    return {
        "present": True,
        "mtime": datetime.fromtimestamp(mt).strftime("%Y-%m-%d %H:%M"),
        "age_seconds": age,
    }


def compute_freshness() -> dict:
    """Report the age of the source files the dashboard depends on."""
    files = {
        "cases_logistics": LATEST_DIR / "cases_logistics.csv",       # the case list
        "active_30_day":   MT_REPORTS_LOCAL / "Active_30_day.csv", # on-time meter
        "wip":             MT_REPORTS_LOCAL / "WIP_cases.csv",     # upstream WIP feed
    }
    info = {k: _file_age(p) for k, p in files.items()}
    ages = [v["age_seconds"] for v in info.values() if v["age_seconds"] is not None]
    oldest = max(ages) if ages else None
    return {
        "files": info,
        "oldest_age_seconds": oldest,
        "stale_threshold_seconds": STALE_THRESHOLD_SECONDS,
        "is_stale": (oldest is not None and oldest > STALE_THRESHOLD_SECONDS),
        "any_missing": any(not v["present"] for v in info.values()),
    }


# ---------------------------------------------------------------------------
# Department payload
# ---------------------------------------------------------------------------
def build_dept_payload(dept_key: str) -> dict:
    if dept_key not in DEPARTMENTS:
        abort(404, f"Unknown department '{dept_key}'")
    dept = DEPARTMENTS[dept_key]

    cfg = load_product_type_map()
    df = load_open_cases()
    remake_set = load_open_remake_case_numbers()

    sub = df[df["pseudo_dept"] == dept["pseudo_dept"]].copy()
    # Hide On Hold cases from the dept view (still counted in pipeline KPIs).
    sub = sub[sub["Cases_Status"].astype(str).str.strip().str.lower() != "on hold"]

    today = pd.Timestamp.now().normalize().date()
    tomorrow_biz = next_business_day(today)

    rows = []
    for _, r in sub.iterrows():
        due_ts = r.get("Cases_DueDate")
        due_date = due_ts.date() if pd.notna(due_ts) else None

        if due_date is None:
            urgency = "none"
        elif due_date < today:
            urgency = "past"
        elif due_date == today:
            urgency = "today"
        elif due_date == tomorrow_biz:
            urgency = "next_biz"
        else:
            urgency = "future"

        case_num = str(r["Cases_CaseNumber"]).strip()
        is_remake = case_num in remake_set
        # Convert NaN to empty string (str(NaN) would otherwise yield "nan").
        pan_raw = r.get("Cases_PanNumber")
        loc_raw = r.get("Cases_LastLocation")
        pan = str(pan_raw).strip() if pd.notna(pan_raw) else ""
        loc = str(loc_raw).strip() if pd.notna(loc_raw) else ""

        rows.append({
            "case_number":    case_num,
            "pan_number":     pan,
            "product_type":   derive_product_type(pan, loc, cfg),
            "last_location":  loc,
            "ship_date":      due_ts.strftime("%Y-%m-%d") if pd.notna(due_ts) else "",
            "ship_date_full": due_ts.strftime("%Y-%m-%d %H:%M") if pd.notna(due_ts) else "",
            "status":         r.get("Cases_Status", ""),
            "doctor":         r.get("Cases_DoctorName", ""),
            "days_overdue":   int(r.get("days_overdue") or 0),
            "urgency":        urgency,
            "is_remake":      bool(is_remake),
        })

    # Sort: most urgent first means soonest ship_date first; no-date rows last.
    def sort_key(row):
        if row["ship_date"]:
            return (1, row["ship_date"])
        return (2, "")
    rows.sort(key=sort_key)

    summary = {
        "total":     len(rows),
        "past_due":  sum(1 for r in rows if r["urgency"] == "past"),
        "due_today": sum(1 for r in rows if r["urgency"] == "today"),
        "due_next":  sum(1 for r in rows if r["urgency"] == "next_biz"),
        "remakes":   sum(1 for r in rows if r["is_remake"]),
    }

    # Filter the displayed case list down to actionable items:
    #   - urgency: past due, due today, or next business day
    #   - identifiable: has a product type (not "-") OR a non-blank last location
    displayed = [
        r for r in rows
        if r["urgency"] in ("past", "today", "next_biz")
        and (r["product_type"] != "-" or (r["last_location"] or "").strip())
    ]
    summary["displayed"] = len(displayed)

    return {
        "dept_key":     dept_key,
        "title":        dept["title"],
        "subtitle":     dept.get("subtitle", ""),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "today":        today.isoformat(),
        "next_biz_day": tomorrow_biz.isoformat(),
        "on_time":      compute_on_time_pct(),
        "summary":      summary,
        "cases":        displayed,
        "freshness":    compute_freshness(),
    }


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__,
            template_folder=str(HERE / "templates"),
            static_folder=str(HERE / "static"))


@app.route("/")
def index():
    return render_template("index.html", departments=DEPARTMENTS)


@app.route("/dept/<dept_key>")
def dept_view(dept_key: str):
    if dept_key not in DEPARTMENTS:
        abort(404)
    return render_template("dept_dashboard.html",
                           dept_key=dept_key,
                           dept=DEPARTMENTS[dept_key])


@app.route("/api/dept/<dept_key>")
def api_dept(dept_key: str):
    try:
        payload = build_dept_payload(dept_key)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 500
    return jsonify(payload)


@app.route("/healthz")
def healthz():
    try:
        n_open = len(load_open_cases())
        return jsonify({"ok": True, "open_cases": n_open,
                        "depts": list(DEPARTMENTS.keys())})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------
def get_lan_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


if __name__ == "__main__":
    port = int(os.environ.get("DASHBOARD_PORT", "8080"))
    ip = get_lan_ip()
    print("=" * 60)
    print("  Artistic Dental -- Monitor Dashboards")
    print("=" * 60)
    print(f"  Index:   http://{ip}:{port}/")
    for k in DEPARTMENTS:
        print(f"           http://{ip}:{port}/dept/{k}")
    print(f"  Health:  http://{ip}:{port}/healthz")
    print("=" * 60)
    print("  Press Ctrl+C to stop.")
    app.run(host="0.0.0.0", port=port, debug=False)
