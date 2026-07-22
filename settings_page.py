"""
settings_page.py
=================
Shared renderer for the Fixed / Removable "Employee Settings" backpages —
the manager-only area for pay configuration and goals, kept off the metrics
dashboards so those stay clean for daily use.

Per employee:
  - Pay type (hourly / unit / salary) + base rate, effective-dated on save.
  - For unit (piece) pay: a per-task rate matrix, pre-populated from the
    tasks this employee has actually completed in the current Performance
    export window, with unrated tasks flagged loudly.
  - Daily unit goal (moved here from the dashboard table) + goal history.

Everything writes to goals_store (local SQLite, gitignored) — nothing here
ever reaches a committed file or the cloud app.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

import goals_store
from auth_gate import require_password
from manager_theme import COLORS, BASE_CSS
from mt_reports_parser import load_tech_productivity

MT_FOLDER = Path("C:/MT_Reports_Local")


@st.cache_data(ttl=300)
def _tasks_completed() -> pd.DataFrame:
    """(tech_code, task_code, task_desc, units) across the current export window."""
    tp = load_tech_productivity(MT_FOLDER)
    if tp.empty:
        return pd.DataFrame()
    return (
        tp.groupby(["tech_code", "task_code", "task_desc"])
          .agg(units=("accepted", "sum")).reset_index()
    )


def render_settings(dashboard: str, label: str, pw_key: str) -> None:
    st.set_page_config(page_title=f"{label} Settings — Artistic Dental",
                       page_icon="⚙️", layout="wide",
                       initial_sidebar_state="collapsed")
    st.markdown(BASE_CSS, unsafe_allow_html=True)
    require_password(pw_key, f"{label} — Employee Settings")

    st.markdown(f"""
    <div style="font-size:13px;color:{COLORS['txt2']};margin-bottom:4px;">
      <a href="/{label.replace(' ', '_')}_Dashboard" target="_self"
         style="color:{COLORS['txt2']};text-decoration:none;">{label} Dashboard</a>
      &nbsp;›&nbsp;<b style="color:{COLORS['txt']};">Employee Settings</b>
    </div>
    <h1 style="margin-top:0;">⚙️ {label} — Employee Settings</h1>
    <p style="color:{COLORS['txt2']};font-size:14px;margin-top:-6px;">
      Pay configuration and goals. All changes are effective-dated from today —
      history is never rewritten.
    </p>
    """, unsafe_allow_html=True)

    techs = goals_store.list_technicians(dashboard=dashboard, active_only=True)
    if not techs:
        st.warning("No technician roster yet — run the pipeline first.")
        st.stop()

    names = {t["name"]: t for t in techs}
    who = st.selectbox("Employee", list(names.keys()))
    tech = names[who]
    code = tech["tech_code"]

    pay = goals_store.get_pay_setting_on(code) or {}
    cur_type = pay.get("pay_type")
    cur_rate = pay.get("base_rate", 0.0)

    st.markdown(f"#### {who} <span style='font-size:13px;color:{COLORS['txt2']}'>"
                f"({tech['area']} · {code})</span>", unsafe_allow_html=True)

    # ── Pay type + base rate ──────────────────────────────────────────────────
    pcol, gcol = st.columns(2)

    with pcol:
        st.markdown("**Pay**")
        if not cur_type:
            st.warning("No pay type set yet for this employee.")
        type_idx = {"hourly": 0, "unit": 1, "salary": 2}.get(cur_type, 0)
        with st.form(f"pay_form_{code}"):
            new_type = st.radio("Pay type", ["hourly", "unit", "salary"],
                                index=type_idx, horizontal=True,
                                format_func={"hourly": "Hourly", "unit": "Unit (piece)",
                                             "salary": "Salary"}.get)
            new_rate = st.number_input(
                "Rate  ·  hourly = $/hour · salary = annual $ · unit = leave 0 (task rates below)",
                min_value=0.0, value=float(cur_rate), step=0.5, format="%.2f",
            )
            if st.form_submit_button("Save Pay Setting", type="primary"):
                goals_store.set_pay_setting(code, new_type, new_rate)
                st.success(f"Saved: {new_type}, rate {new_rate:,.2f}, effective {date.today()}.")
                st.rerun()
        if cur_type == "salary" and cur_rate:
            st.caption(f"Daily labor cost: ${cur_rate/250:,.2f}  (annual ÷ 250 working days)")

    with gcol:
        st.markdown("**Daily Unit Goal**")
        cur_goal = goals_store.get_current_goal(code)
        with st.form(f"goal_form_{code}"):
            new_goal = st.number_input("Units per day", min_value=0.0,
                                       value=float(cur_goal or 0), step=1.0)
            if st.form_submit_button("Save Goal", type="primary"):
                if new_goal > 0:
                    goals_store.set_goal(code, new_goal)
                    st.success(f"Goal {new_goal:g}/day effective {date.today()}.")
                    st.rerun()
                else:
                    st.error("Goal must be greater than 0.")
        hist = goals_store.get_goal_history(code)
        if hist:
            with st.expander(f"Goal history ({len(hist)})"):
                st.dataframe(pd.DataFrame(hist).rename(columns={
                    "units_per_day": "Units/Day", "effective_date": "Effective From",
                    "created_at": "Set On"}), hide_index=True, use_container_width=True)

    # ── Piece-pay task rate matrix ────────────────────────────────────────────
    if (goals_store.get_pay_setting_on(code) or {}).get("pay_type") == "unit":
        st.markdown("---")
        st.markdown("**Task Rates (piece pay)**")
        st.caption("Pre-filled with tasks this employee completed in the current "
                   "export window. Unrated tasks pay $0 until a rate is set.")

        done = _tasks_completed()
        done = done[done["tech_code"] == code] if not done.empty else pd.DataFrame()
        rates = goals_store.get_current_task_rates(code)

        rows = []
        for _, r in done.iterrows():
            t = str(r["task_code"])
            rows.append({"Task": t, "Description": r["task_desc"],
                         "Units (window)": int(r["units"]),
                         "Current Rate": rates.get(t),
                         "New Rate": rates.get(t) or 0.0})
        # Rated tasks not in the current window still shown (history matters)
        for t, rate in rates.items():
            if not any(row["Task"] == t for row in rows):
                rows.append({"Task": t, "Description": "(no completions this window)",
                             "Units (window)": 0, "Current Rate": rate, "New Rate": rate})

        if not rows:
            st.info("No completed tasks in the current export window and no rates set yet.")
        else:
            df = pd.DataFrame(rows)
            unrated = df["Current Rate"].isna().sum()
            if unrated:
                st.error(f"⚠️ {unrated} task(s) UNRATED — completions on those pay $0 "
                         "until a rate is entered.")
            edited = st.data_editor(
                df, hide_index=True, use_container_width=True,
                disabled=["Task", "Description", "Units (window)", "Current Rate"],
                column_config={
                    "Current Rate": st.column_config.NumberColumn(format="$%.2f"),
                    "New Rate": st.column_config.NumberColumn(format="$%.2f", min_value=0.0),
                },
                key=f"rates_editor_{code}",
            )
            if st.button("Save Task Rates", type="primary", key=f"save_rates_{code}"):
                changed = 0
                for _, row in edited.iterrows():
                    old = row["Current Rate"]
                    new = row["New Rate"]
                    if new > 0 and (pd.isna(old) or float(new) != float(old)):
                        goals_store.set_task_rate(code, row["Task"], float(new),
                                                  str(row["Description"]))
                        changed += 1
                if changed:
                    st.success(f"Saved {changed} rate(s), effective {date.today()}.")
                    st.rerun()
                else:
                    st.info("No rate changes to save.")

    st.caption("All settings are effective-dated — past labor and % of goal never recalculate.")
