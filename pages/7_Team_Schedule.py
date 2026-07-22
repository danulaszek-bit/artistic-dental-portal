"""
Artistic Dental Studio — Team Schedule (PTO & Out-of-Lab)
==========================================================
Deliberately UNGATED: anyone in the lab can enter PTO or out-of-lab days
without a manager password (per Danny). Contains no pay data — scheduling
only. Covers both Fixed and Removable rosters.

Capacity projections and labor allocation pick these entries up immediately
on the (password-protected) manager dashboards.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

import goals_store
from manager_theme import COLORS, BASE_CSS

st.set_page_config(
    page_title="Team Schedule — Artistic Dental",
    page_icon="📅",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(BASE_CSS, unsafe_allow_html=True)

st.markdown(f"""
<h1 style="margin-top:0;">📅 Team Schedule — PTO &amp; Out-of-Lab</h1>
<p style="color:{COLORS['txt2']};font-size:14px;margin-top:-6px;">
  Schedule time off or out-of-lab work days. No password needed — pay data lives
  elsewhere. Capacity projections update immediately.
</p>
""", unsafe_allow_html=True)

techs = goals_store.list_technicians(active_only=True)
if not techs:
    st.warning("No technician roster yet — run the pipeline first.")
    st.stop()

today = date.today()
name_to_tech = {f"{t['name']}  ({t['dashboard']} · {t['area']})": t for t in techs}

AREA_CHOICES = ["Chairside", "Removables", "Splints", "Partial", "Ortho",
                "Crown & Bridge", "CAD/CAM", "Ceramics", "Fixed QC", "Rem QC", "Model/Die"]

form_col, list_col = st.columns([1, 1.4])

def business_days_from(start: date, n_days: int) -> list[date]:
    """The first n_days weekdays starting at `start` (start counts if a weekday)."""
    out, d = [], start
    while len(out) < n_days:
        if d.weekday() < 5:      # Mon-Fri
            out.append(d)
        d += timedelta(days=1)
    return out


with form_col:
    st.markdown("### Add an entry")
    with st.form("team_sched_form", clear_on_submit=True):
        who = st.selectbox("Employee", list(name_to_tech.keys()))
        f1, f2 = st.columns(2)
        sched_date = f1.date_input("Start date", value=today + timedelta(days=1))
        n_days = f2.number_input("Business days", min_value=1, max_value=30, value=1, step=1,
                                 help="Weekends are skipped automatically — a week's vacation is 5.")
        entry_type = st.radio("Type", ["PTO full day", "PTO half day", "Out of lab"],
                              horizontal=True)
        target_area = st.selectbox("Working for which department? (out-of-lab only)",
                                   AREA_CHOICES)
        note = st.text_input("Note (optional)")
        if st.form_submit_button("Add to schedule", type="primary"):
            t = name_to_tech[who]
            days = business_days_from(sched_date, int(n_days))
            for d in days:
                if entry_type == "Out of lab":
                    goals_store.add_out_of_lab(t["tech_code"], d, target_area, note)
                else:
                    goals_store.add_pto(t["tech_code"], d,
                                        "full" if "full" in entry_type else "half", note)
            span = f"{days[0]}" if len(days) == 1 else f"{days[0]} → {days[-1]} ({len(days)} business days)"
            if entry_type == "Out of lab":
                st.success(f"{t['name']} — out of lab {span}, working for {target_area}.")
            else:
                st.success(f"{t['name']} — PTO {span}.")
            st.rerun()

with list_col:
    st.markdown("### Next 30 days")
    upcoming = goals_store.list_upcoming_pto(days=30)
    ool = goals_store.list_upcoming_out_of_lab(days=30)
    sched_rows = (
        [{"Date": p["pto_date"], "Employee": p["name"],
          "Dept": f"{p['dashboard']} · {p['area']}",
          "Type": f"PTO ({p['portion']})", "Note": p["note"]} for p in upcoming]
        + [{"Date": o["work_date"], "Employee": o["name"],
            "Dept": f"{o['dashboard']} · {o['area']}",
            "Type": f"Out of lab → {o['target_area']}", "Note": o["note"]} for o in ool]
    )
    if sched_rows:
        st.dataframe(pd.DataFrame(sched_rows).sort_values("Date"),
                     hide_index=True, use_container_width=True)
    else:
        st.caption("Nothing scheduled in the next 30 days.")

st.caption("Artistic Dental Studio · Team Schedule · entries apply to capacity and labor allocation immediately")
