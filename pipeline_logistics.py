"""
Artistic Dental — Logistics Module
====================================
Station-time tracking + per-case logistics KPIs.
Designed to be called from run_pipeline() in pipeline.py.

INPUTS
------
  cases_df         pandas DataFrame loaded from AllCasesByDateIn.csv
                   (or fallback Cases_* tabular export)
                   Expected columns (raw, NOT renamed):
                     Cases_CaseID, Cases_CaseNumber, Cases_CustomerID,
                     Cases_DoctorName, Cases_PanNumber,
                     Cases_DateIn, Cases_DueDate, Cases_Status,
                     Cases_LastLocation, Cases_TotalCharge
                   Optional: Products_Department (preferred over location-prefix mapping)

  base_dir         Path to project root (contains logistics_config.yaml)
  cache_dir        Path to cache/ folder (for station_history.csv)
  latest_dir       Path to cache/latest/ folder (for output CSVs)

OUTPUTS
-------
  cache/station_history.csv          (updated each run)
  cache/latest/cases_logistics.csv   (per-case enriched)
  cache/latest/logistics_summary.csv (top-level KPI counts)

INTEGRATION
-----------
In pipeline.py, after loading cases and before the Drive upload:

    from pipeline_logistics import compute_logistics
    raw_cases = tables.get("active_cases_raw") or _load_all_cases_raw(folder)
    compute_logistics(raw_cases, BASE_DIR, CACHE_DIR, LATEST_DIR)

The output CSVs are picked up by pages/3_Logistics.py via the same
`load_kpi_data()` pattern other pages use.
"""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

log = logging.getLogger("logistics")

OPEN_STATUSES = ["In Production", "On Hold", "Outsourced",
                 "Sent for TryIn", "Submitted"]


# ── Config loading ────────────────────────────────────────────────────────────

def load_logistics_config(base_dir: Path) -> dict:
    """Load logistics_config.yaml. Returns sensible defaults if missing."""
    cfg_path = base_dir / "logistics_config.yaml"
    if not cfg_path.exists():
        log.warning("logistics_config.yaml not found at %s — using defaults", cfg_path)
        return _default_config()
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f) or {}
    cfg.setdefault("departments", {})
    cfg.setdefault("unknown_threshold_days", 5)
    cfg.setdefault("behind_criteria", _default_config()["behind_criteria"])
    return cfg


def _default_config() -> dict:
    return {
        "departments": {},
        "unknown_threshold_days": 5,
        "behind_criteria": {
            "past_due_date": True,
            "stuck_at_station": True,
            "high_value_aged": False,
            "high_value_threshold_usd": 1000,
            "high_value_age_days": 14,
        },
    }


# ── Department mapping ────────────────────────────────────────────────────────

def map_location_to_department(loc, dept_config: dict):
    """
    Given a Cases_LastLocation value, return (department_name, threshold_days).
    Matching:
      1. exact match against `names`
      2. starts-with match against `prefixes`
      3. fallback "Unknown" + unknown_threshold_days
    """
    if pd.isna(loc) or not str(loc).strip():
        return "Unknown", dept_config.get("unknown_threshold_days", 5)
    loc_up = str(loc).strip().upper()
    departments = dept_config.get("departments", {}) or {}
    # exact match
    for dept_name, dept in departments.items():
        names = [str(n).upper() for n in (dept.get("names") or [])]
        if loc_up in names:
            return dept_name, dept.get("threshold_days", 5)
    # prefix match
    for dept_name, dept in departments.items():
        prefixes = [str(p).upper() for p in (dept.get("prefixes") or [])]
        for p in prefixes:
            if loc_up.startswith(p):
                return dept_name, dept.get("threshold_days", 5)
    return "Unknown", dept_config.get("unknown_threshold_days", 5)


# ── Station-time history (snapshot deltas) ────────────────────────────────────
#
# If DLCPM exposes a real "LastLocationChangeDate" field, replace
# update_station_history() with a one-line read of that field.

def update_station_history(open_cases: pd.DataFrame, history_path: Path) -> pd.DataFrame:
    """
    Append/update rolling station history.
    Schema: case_id, location, first_seen_at, last_seen_at
    """
    now = pd.Timestamp.now()

    if history_path.exists():
        hist = pd.read_csv(history_path, parse_dates=["first_seen_at", "last_seen_at"])
    else:
        hist = pd.DataFrame(columns=["case_id", "location", "first_seen_at", "last_seen_at"])

    # Most recent (case_id, location) per case for quick lookup
    if not hist.empty:
        last_obs = hist.sort_values("last_seen_at").drop_duplicates("case_id", keep="last")
        last_loc_map = dict(zip(last_obs["case_id"].astype(str), last_obs["location"].fillna("")))
    else:
        last_loc_map = {}

    new_rows = []
    bumped_idx = []

    case_id_col = "Cases_CaseID" if "Cases_CaseID" in open_cases.columns else "Cases_CaseNumber"

    for _, r in open_cases.iterrows():
        cid = r.get(case_id_col)
        loc = str(r.get("Cases_LastLocation", "") or "").strip()
        if pd.isna(cid) or cid in (None, ""):
            continue
        cid = str(cid)
        prev_loc = last_loc_map.get(cid)

        if prev_loc == loc:
            # same location → bump last_seen_at on the existing row
            mask = (hist["case_id"].astype(str) == cid) & (hist["location"].fillna("") == loc)
            if mask.any():
                hist.loc[mask, "last_seen_at"] = now
                bumped_idx.extend(hist.index[mask].tolist())
                continue
        # new case or location changed → insert
        new_rows.append({
            "case_id": cid, "location": loc,
            "first_seen_at": now, "last_seen_at": now,
        })

    if new_rows:
        hist = pd.concat([hist, pd.DataFrame(new_rows)], ignore_index=True)

    history_path.parent.mkdir(parents=True, exist_ok=True)
    hist.to_csv(history_path, index=False)

    log.info("station_history: %d total rows, %d bumped, %d new",
             len(hist), len(set(bumped_idx)), len(new_rows))
    return hist


# ── Main entrypoint ───────────────────────────────────────────────────────────

def compute_logistics(cases_df: pd.DataFrame,
                       base_dir: Path,
                       cache_dir: Path,
                       latest_dir: Path) -> dict:
    """
    Compute logistics KPIs and write output CSVs.
    Returns dict with 'cases_logistics' and 'logistics_summary' dataframes.
    """
    if cases_df is None or cases_df.empty:
        log.warning("compute_logistics called with empty cases_df")
        return {"cases_logistics": pd.DataFrame(), "logistics_summary": pd.DataFrame()}

    # ── Extract per-case product lines BEFORE dedup ───────────────────────────
    # WIP.csv is product-line-level — one row per Products_ProductID. Capture
    # those lines into case_product_lines.csv for the dig-in popup, then
    # collapse to one row per case for the main logistics view.
    prod_cols = [c for c in ("Cases_CaseNumber", "Products_Type",
                              "Products_ProductID", "Products_Department")
                  if c in cases_df.columns]
    if "Cases_CaseNumber" in cases_df.columns and "Products_ProductID" in cases_df.columns:
        product_lines = (
            cases_df[prod_cols]
            .dropna(subset=["Cases_CaseNumber"])
            .drop_duplicates()
            .copy()
        )
    else:
        product_lines = pd.DataFrame()

    # Dedupe to one row per case for the rest of compute_logistics
    if "Cases_CaseNumber" in cases_df.columns:
        before = len(cases_df)
        cases_df = cases_df.drop_duplicates(subset=["Cases_CaseNumber"], keep="first").copy()
        if before - len(cases_df):
            log.info("Logistics: collapsed %d product-line rows → %d unique cases",
                     before, len(cases_df))

    cfg = load_logistics_config(base_dir)

    # Filter to open cases (in-flight, not invoiced/cancelled)
    open_cases = cases_df[cases_df["Cases_Status"].isin(OPEN_STATUSES)].copy()
    if open_cases.empty:
        log.info("No open cases for logistics view")
        return {"cases_logistics": pd.DataFrame(), "logistics_summary": pd.DataFrame()}

    # Update + read station history → days_at_station
    history_path = cache_dir / "station_history.csv"
    hist = update_station_history(open_cases, history_path)

    if not hist.empty:
        # Coerce first_seen_at to datetime64 — on the first run the column is
        # object-dtype (built from dicts of pd.Timestamp), which breaks .dt.days.
        hist["first_seen_at"] = pd.to_datetime(hist["first_seen_at"], errors="coerce")
        latest_obs = hist.sort_values("last_seen_at").drop_duplicates("case_id", keep="last")
        latest_obs["days_at_station"] = (
            pd.Timestamp.now() - latest_obs["first_seen_at"]
        ).dt.days.clip(lower=0).fillna(0).astype(int)
        days_map = dict(zip(latest_obs["case_id"].astype(str), latest_obs["days_at_station"]))
    else:
        days_map = {}

    case_id_col = "Cases_CaseID" if "Cases_CaseID" in open_cases.columns else "Cases_CaseNumber"
    open_cases["days_at_station"] = (
        open_cases[case_id_col].astype(str).map(days_map).fillna(0).astype(int)
    )

    # Department mapping
    # If Products_Department is available, prefer it (still fall back to mapping)
    if "Products_Department" in open_cases.columns:
        # Use real department, but also compute threshold from config (looking up by dept name)
        dept_lookup = cfg.get("departments", {})
        open_cases["pseudo_dept"] = open_cases["Products_Department"].fillna("Unknown")
        open_cases["dept_threshold_days"] = open_cases["pseudo_dept"].apply(
            lambda d: dept_lookup.get(d, {}).get("threshold_days", cfg["unknown_threshold_days"])
        )
    else:
        mapped = open_cases["Cases_LastLocation"].apply(
            lambda l: pd.Series(map_location_to_department(l, cfg))
        )
        mapped.columns = ["pseudo_dept", "dept_threshold_days"]
        open_cases[["pseudo_dept", "dept_threshold_days"]] = mapped

    # Parse dates / numbers defensively
    open_cases["Cases_DueDate"] = pd.to_datetime(open_cases.get("Cases_DueDate"), errors="coerce")
    open_cases["Cases_DateIn"]  = pd.to_datetime(open_cases.get("Cases_DateIn"),  errors="coerce")
    open_cases["Cases_TotalCharge"] = pd.to_numeric(
        open_cases.get("Cases_TotalCharge"), errors="coerce"
    ).fillna(0)

    today = pd.Timestamp.today().normalize()
    open_cases["age_days"] = (today - open_cases["Cases_DateIn"]).dt.days.fillna(0).astype(int)
    open_cases["days_overdue"] = (
        (today - open_cases["Cases_DueDate"]).dt.days
        .where(open_cases["Cases_DueDate"] < today, 0)
        .fillna(0)
        .astype(int)
    )

    # Behind flags
    crit = cfg["behind_criteria"]
    open_cases["flag_past_due"] = bool(crit.get("past_due_date", True)) & (open_cases["days_overdue"] > 0)
    open_cases["flag_stuck"] = bool(crit.get("stuck_at_station", True)) & (
        open_cases["days_at_station"] > open_cases["dept_threshold_days"]
    )
    if crit.get("high_value_aged", False):
        open_cases["flag_high_value_aged"] = (
            (open_cases["Cases_TotalCharge"] >= crit.get("high_value_threshold_usd", 1000)) &
            (open_cases["age_days"] >= crit.get("high_value_age_days", 14))
        )
    else:
        open_cases["flag_high_value_aged"] = False

    open_cases["is_behind"] = (
        open_cases["flag_past_due"] | open_cases["flag_stuck"] | open_cases["flag_high_value_aged"]
    )

    # Project to safe column set (no patient names — PHI policy)
    out_cols = [
        "Cases_CaseNumber", "Cases_CustomerID", "Cases_DoctorName",
        "Cases_PanNumber",
        "Cases_DateIn", "Cases_DueDate", "Cases_ShipDate", "Cases_Status",
        "Cases_LastLocation", "Cases_TotalCharge",
        "pseudo_dept", "days_at_station", "dept_threshold_days",
        "age_days", "days_overdue",
        "flag_past_due", "flag_stuck", "flag_high_value_aged", "is_behind",
    ]
    out_cols = [c for c in out_cols if c in open_cases.columns]
    cases_logistics = open_cases[out_cols].copy()

    # Top-level summary
    summary = pd.DataFrame([{
        "open_cases":      len(cases_logistics),
        "overdue_cases":   int(cases_logistics["flag_past_due"].sum()),
        "stuck_cases":     int(cases_logistics["flag_stuck"].sum()),
        "behind_total":    int(cases_logistics["is_behind"].sum()),
        "value_at_risk":   float(cases_logistics.loc[cases_logistics["is_behind"], "Cases_TotalCharge"].sum()),
        "oldest_age_days": int(cases_logistics["age_days"].max() if len(cases_logistics) else 0),
        "median_age_days": int(cases_logistics["age_days"].median() if len(cases_logistics) else 0),
        "computed_at":     datetime.now().isoformat(timespec="seconds"),
    }])

    # Persist
    latest_dir.mkdir(parents=True, exist_ok=True)
    cases_logistics.to_csv(latest_dir / "cases_logistics.csv", index=False)
    summary.to_csv(latest_dir / "logistics_summary.csv", index=False)

    # Per-case product lines — restrict to cases that survived all filters
    if not product_lines.empty and "Cases_CaseNumber" in cases_logistics.columns:
        keep_ids = set(cases_logistics["Cases_CaseNumber"].astype(str))
        product_lines["Cases_CaseNumber"] = product_lines["Cases_CaseNumber"].astype(str)
        product_lines = product_lines[product_lines["Cases_CaseNumber"].isin(keep_ids)].copy()
        product_lines.to_csv(latest_dir / "case_product_lines.csv", index=False)
        log.info("Case product lines: %d rows across %d cases written",
                 len(product_lines), product_lines["Cases_CaseNumber"].nunique())

    log.info("Logistics computed: %d open, %d behind, $%.0f at risk",
             summary.iloc[0]["open_cases"], summary.iloc[0]["behind_total"],
             summary.iloc[0]["value_at_risk"])

    return {"cases_logistics": cases_logistics, "logistics_summary": summary}
