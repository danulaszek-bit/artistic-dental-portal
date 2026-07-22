"""
labor_calc.py
==============
Daily labor-dollar estimates per technician, computed from the MagicTouch
exports + manager-entered pay settings in goals_store. PRIVACY-SENSITIVE:
results must never be written to committed files (cache/latest/) — they live
only in the local SQLite DB (labor_history) and are rendered live in-page.

Pay types:
  hourly — paid TimeClock hours × hourly rate effective that day.
           Paid hours = all logged activity EXCEPT 'Non-Paid TO' and 'Lunch'
           (Vacation/Personal/Bereavement/Holiday punches in the report are
           paid time entries the office logs as 8:00 blocks).
  unit   — per completed task: accepted units × that employee's rate for that
           task code (effective-dated). Tasks with no rate contribute $0 and
           are returned in the unrated list so the settings page can flag
           them loudly — never silently ignored.
  salary — annual base ÷ 250 per "day on". A day on = any TimeClock activity,
           OR a PTO day (salary continues per Danny), OR a scheduled
           out-of-lab day.

Out-of-lab days charge the technician's dollars to the entry's target area
instead of their home area (they're always salaried per Danny).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

import goals_store
from mt_reports_parser import load_timeclock, load_tech_productivity

UNPAID_ACTIVITIES = {"Non-Paid TO", "Lunch"}
SALARY_WORKING_DAYS = 250


def compute_labor_estimates(mt_folder: Path) -> tuple[pd.DataFrame, list[dict]]:
    """
    Returns (labor_df, unrated) where labor_df has one row per
    (work_date, tech_code, area): work_date (ISO str), tech_code, name, area,
    dashboard, pay_type, dollars — and unrated lists piece-pay task
    completions with no rate set: {'tech_code','name','task_code','task_desc',
    'units'} aggregated across the window.

    The window is whatever the TimeClock / Performance exports currently hold
    (~1 week going forward) — the pipeline persists each day's rows to
    labor_history so history accumulates beyond the export window.
    """
    roster = {t["tech_code"]: t for t in goals_store.list_technicians(active_only=True)}
    if not roster:
        return pd.DataFrame(), []

    tc = load_timeclock(mt_folder)
    tp = load_tech_productivity(mt_folder)

    # Paid hours per (tech, date) — hourly pay basis + salary "day on" signal
    hours_by_day: dict[tuple[str, str], float] = {}
    if not tc.empty:
        paid = tc[~tc["activity"].isin(UNPAID_ACTIVITIES)].copy()
        paid = paid.dropna(subset=["date_in"])
        paid["d"] = paid["date_in"].dt.date.astype(str)
        grp = paid.groupby(["employee_id", "d"])["hours"].sum()
        hours_by_day = {(code, d): h for (code, d), h in grp.items()}

    # Accepted units per (tech, date, task) — piece-pay basis
    units_by_task: dict[tuple[str, str, str], dict] = {}
    if not tp.empty:
        tpp = tp.dropna(subset=["completion_date"]).copy()
        tpp["d"] = tpp["completion_date"].dt.date.astype(str)
        grp = (tpp.groupby(["tech_code", "d", "task_code"])
                  .agg(units=("accepted", "sum"), task_desc=("task_desc", "first")))
        for (code, d, task), row in grp.iterrows():
            units_by_task[(str(code), d, str(task))] = {
                "units": float(row["units"]), "task_desc": str(row["task_desc"]),
            }

    # All dates in scope
    dates: set[str] = {d for (_, d) in hours_by_day} | {d for (_, d, _) in units_by_task}

    # Days each tech shows task-completion activity — salary "day on" fallback
    # for when the TimeClock export isn't scheduled/available.
    task_days: set[tuple[str, str]] = {(c, d) for (c, d, _) in units_by_task}

    rows: list[dict] = []
    unrated_agg: dict[tuple[str, str], dict] = {}

    for code, tech in roster.items():
        for d_str in sorted(dates):
            d = date.fromisoformat(d_str)
            pay = goals_store.get_pay_setting_on(code, d)
            if not pay:
                continue  # no pay setting yet — nothing to estimate

            ool_area = goals_store.get_out_of_lab_on(code, d)
            area = ool_area or tech["area"]
            dollars = 0.0

            if pay["pay_type"] == "hourly":
                h = hours_by_day.get((code, d_str), 0.0)
                dollars = h * pay["base_rate"]

            elif pay["pay_type"] == "unit":
                for (t_code, t_d, task), info in units_by_task.items():
                    if t_code != code or t_d != d_str or info["units"] <= 0:
                        continue
                    rate = goals_store.get_task_rate_on(code, task, d)
                    if rate is None:
                        key = (code, task)
                        agg = unrated_agg.setdefault(key, {
                            "tech_code": code, "name": tech["name"],
                            "task_code": task, "task_desc": info["task_desc"],
                            "units": 0.0,
                        })
                        agg["units"] += info["units"]
                    else:
                        dollars += info["units"] * rate

            elif pay["pay_type"] == "salary":
                day_on = (
                    (code, d_str) in hours_by_day
                    or (code, d_str) in task_days
                    or goals_store.get_pto_on(code, d) is not None
                    or ool_area is not None
                )
                if day_on:
                    dollars = pay["base_rate"] / SALARY_WORKING_DAYS

            if dollars > 0:
                rows.append({
                    "work_date": d_str, "tech_code": code, "name": tech["name"],
                    "area": area, "dashboard": tech["dashboard"],
                    "pay_type": pay["pay_type"], "dollars": round(dollars, 2),
                })

    return pd.DataFrame(rows), list(unrated_agg.values())


def persist_estimates(mt_folder: Path) -> tuple[int, int]:
    """Compute and upsert estimates into labor_history (local SQLite only).
    Returns (rows_upserted, unrated_combos)."""
    df, unrated = compute_labor_estimates(mt_folder)
    if df.empty:
        return 0, len(unrated)
    n = goals_store.upsert_labor_estimates(
        df[["work_date", "tech_code", "area", "dashboard", "pay_type", "dollars"]]
          .to_dict("records")
    )
    return n, len(unrated)
