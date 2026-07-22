"""
Artistic Dental Studio — Fixed Dashboard
==========================================
Manager workspace for Crown & Bridge / CAD-CAM / Ceramics / Fixed QC.
The first genuinely interactive, persistent-write page in this app:
technician goals are editable (goals_store.py, SQLite, effective-dated) and
PTO can be entered here, both reflected immediately (no pipeline rerun needed)
since production numbers come from cache/latest/tech_production.csv (written
by production_pipeline.py) while goals/PTO are read live from goals_store.

Visual language follows the mockup built during design review at
C:\\Users\\dulaszek\\Documents\\ArtisticDashboard\\prototype\\fixed-dashboard.html
(status-colored % pills, area cards, drill-down panel), translated to
Streamlit widgets + Plotly and wired to real data.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import goals_store
from auth_gate import require_password
from manager_theme import COLORS, BASE_CSS, tile_html, meter_html, status_color

BASE_DIR   = Path(__file__).parent.parent
LATEST_DIR = BASE_DIR / "cache" / "latest"
DASHBOARD  = "Fixed"

st.set_page_config(
    page_title="Fixed Dashboard — Artistic Dental",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(BASE_CSS, unsafe_allow_html=True)
require_password("fixed", "Fixed Dashboard")


@st.cache_data(ttl=120)
def load_data():
    fin_path   = LATEST_DIR / "dept_financials.csv"
    techs_path = LATEST_DIR / "tech_production.csv"
    fin   = pd.read_csv(fin_path)   if fin_path.exists()   else pd.DataFrame()
    techs = pd.read_csv(techs_path) if techs_path.exists() else pd.DataFrame()
    if not techs.empty:
        techs = techs[techs["dashboard"] == DASHBOARD].copy()
    period = techs["period"].iloc[0] if not techs.empty else "—"
    return fin, techs, period


fin, techs, period = load_data()

st.markdown(f"""
<div class="crumb" style="font-size:13px;color:{COLORS['txt2']};margin-bottom:4px;">
  <a href="/GM_Summary" target="_self" style="color:{COLORS['txt2']};text-decoration:none;">GM Summary</a>
  &nbsp;›&nbsp;<b style="color:{COLORS['txt']};">Fixed Dashboard</b>
</div>
<h1 style="margin-top:0;">Fixed — Crown &amp; Bridge · CAD/CAM · Ceramics</h1>
<p style="color:{COLORS['txt2']};font-size:14px;margin-top:-6px;">
  Manager view · period {period} · goals are editable below, PTO adjusts projected capacity automatically
</p>
""", unsafe_allow_html=True)

if techs.empty:
    st.warning(
        "No Fixed technician data found yet. Run `py production_pipeline.py` "
        "from C:\\ArtisticDentalPortal first."
    )
    st.stop()

today = date.today()

# Pull live goal + PTO per technician
techs = techs.copy()
techs["goal"] = techs["tech_code"].apply(lambda c: goals_store.get_current_goal(c) or 0)
techs["pto_today"] = techs["tech_code"].apply(lambda c: goals_store.get_pto_on(c, today))
techs["pct_of_goal"] = techs.apply(
    lambda r: round(r["today_units"] / r["goal"] * 100, 1) if r["goal"] else None, axis=1
)

# ── KPI tiles ──────────────────────────────────────────────────────────────────
tracked = techs[techs["goal"] > 0]
overall_pct = (
    round(tracked["today_units"].sum() / tracked["goal"].sum() * 100, 1)
    if tracked["goal"].sum() else 0.0
)
fixed_fin = fin[fin["dashboard"] == "Fixed"] if not fin.empty else pd.DataFrame()
remake_pct = fixed_fin["remake_rate_pct"].iloc[0] if not fixed_fin.empty else 0.0
remake_disc = fixed_fin["remake_discount"].iloc[0] if not fixed_fin.empty else 0.0
net_sales = fixed_fin["net_sales"].iloc[0] if not fixed_fin.empty else 0.0

full_pto_today = sum(1 for p in techs["pto_today"] if p == "full")
active_today = len(techs) - full_pto_today
projected_pct = goals_store.projected_capacity_pct(DASHBOARD, today)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(tile_html("Overall % of Goal — Today", f"{overall_pct}%",
                          f"{len(tracked)}/{len(techs)} techs with a goal set",
                          status_color(overall_pct)), unsafe_allow_html=True)
with c2:
    st.markdown(tile_html("Remake Rate ($)", f"{remake_pct}%",
                          f"${remake_disc:,.0f} of ${net_sales:,.0f} in sales",
                          status_color(100 - remake_pct, good=95, warn=90)),
               unsafe_allow_html=True)
with c3:
    st.markdown(tile_html("Techs Active Today", f"{active_today} / {len(techs)}",
                          f"{full_pto_today} on full-day PTO"), unsafe_allow_html=True)
with c4:
    st.markdown(tile_html("Projected Output Today", f"{projected_pct}%",
                          "of full-roster capacity, PTO-adjusted",
                          status_color(projected_pct)), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Area cards ─────────────────────────────────────────────────────────────────
st.markdown("### Areas")
AREA_ORDER = ["Crown & Bridge", "CAD/CAM", "Ceramics", "Fixed QC"]
areas_present = [a for a in AREA_ORDER if a in techs["area"].unique()]

if "fixed_area" not in st.session_state:
    st.session_state.fixed_area = None  # None = show all Fixed technicians

if st.session_state.fixed_area is not None:
    if st.button("← Show all Fixed technicians"):
        st.session_state.fixed_area = None
        st.rerun()

area_cols = st.columns(len(areas_present)) if areas_present else []
for col, area in zip(area_cols, areas_present):
    sub = techs[techs["area"] == area]
    sub_tracked = sub[sub["goal"] > 0]
    a_pct = (round(sub_tracked["today_units"].sum() / sub_tracked["goal"].sum() * 100, 1)
             if sub_tracked["goal"].sum() else 0.0)

    station_html = ""
    stations = sub.groupby("station")["today_units"].sum()
    station_goals = sub.groupby("station").apply(
        lambda g: g[g["goal"] > 0]["goal"].sum(), include_groups=False
    )
    if len(stations) > 1:
        rows_html = []
        palette = [COLORS["acc"], "#1baf7a", "#eda100", COLORS["pur"]]
        for i, (st_name, units) in enumerate(stations.items()):
            g = station_goals.get(st_name, 0)
            st_pct = round(units / g * 100, 1) if g else 0
            color = palette[i % len(palette)]
            rows_html.append(f"""
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
              <span style="width:8px;height:8px;border-radius:2px;background:{color};flex:none;"></span>
              <span style="font-size:11px;color:{COLORS['txt2']};width:90px;flex:none;">{st_name.title()}</span>
              <span style="flex:1;height:6px;border-radius:3px;background:{COLORS['bdr']};">
                <span style="display:block;height:100%;border-radius:3px;width:{min(st_pct,100)}%;background:{color};"></span>
              </span>
              <span style="font-size:11px;color:{COLORS['txt2']};width:30px;text-align:right;">{st_pct}%</span>
            </div>""")
        station_html = "<div style='margin-top:10px;'>" + "".join(rows_html) + "</div>"

    with col:
        active_flag = "border:1px solid " + COLORS["acc"] + ";" if st.session_state.fixed_area == area else ""
        st.markdown(f"""
        <div class="mgr-card" style="{active_flag}">
          <div class="name">{area}</div>
          <div class="sub">{len(sub)} technicians</div>
          {meter_html(a_pct)}
          <div style="text-align:right;font-size:13px;font-weight:700;margin-top:4px;">{a_pct}%</div>
          {station_html}
        </div>
        """, unsafe_allow_html=True)
        if st.button(f"View {area}", key=f"btn_{area}", use_container_width=True):
            st.session_state.fixed_area = area
            st.rerun()

st.markdown("<br>", unsafe_allow_html=True)

# ── Technician table ─────────────────────────────────────────────────────────
sel_area = st.session_state.fixed_area
st.markdown(f"### {sel_area} — Technicians" if sel_area else f"### All Fixed — Technicians ({len(techs)})")

view = techs[techs["area"] == sel_area].copy() if sel_area else techs.copy()
view = view.sort_values("name")
view["out_today"] = view["tech_code"].apply(lambda c: goals_store.get_out_of_lab_on(c, today))
view["PTO / Out"] = view.apply(
    lambda r: f"Out — {r['out_today']}" if r["out_today"]
    else {"full": "PTO Full", "half": "PTO Half"}.get(r["pto_today"], "—"), axis=1)

display_df = view.rename(columns={
    "name": "Technician", "station": "Station", "goal": "Today's Goal",
    "today_units": "Completed", "pct_of_goal": "% of Goal",
})

st.dataframe(
    display_df[["Technician", "Station", "Today's Goal", "Completed", "% of Goal", "PTO / Out"]],
    hide_index=True,
    use_container_width=True,
)
st.page_link("pages/7_Fixed_Settings.py",
             label="⚙️ Edit goals, pay types & task rates → Employee Settings")

# ── Drill-down ─────────────────────────────────────────────────────────────────
st.markdown("### Technician Detail")
pick = st.selectbox("View details for:", view["Technician"].tolist() if "Technician" in view.columns
                     else view["name"].tolist(), index=None, placeholder="Select a technician…")

if pick:
    t = view[view["name"] == pick].iloc[0]

    @st.dialog(f"{t['name']}", width="large")
    def show_detail():
        st.caption(f"{t['station']} · {sel_area} · Fixed")
        d1, d2, d3 = st.columns(3)
        d1.metric("Hours (period)", f"{t['hours']:.1f}")
        d2.metric("Units Completed (period)", int(t["period_units"]))
        d3.metric("% of Goal — Today",
                  f"{t['pct_of_goal']}%" if pd.notna(t["pct_of_goal"]) else "—")

        st.markdown("**Rejects (internal, this period)**")
        st.write(f"{int(t['internal_remakes'])} recorded reject(s) — "
                 f"{int(t['period_rejected'])} rejected unit(s) out of "
                 f"{int(t['period_units']) + int(t['period_rejected'])} attempted.")

        st.markdown("**Goal History**")
        hist = goals_store.get_goal_history(t["tech_code"])
        if hist:
            st.dataframe(
                pd.DataFrame(hist).rename(columns={
                    "units_per_day": "Units/Day", "effective_date": "Effective From",
                    "created_at": "Set On",
                }),
                hide_index=True, use_container_width=True,
            )
        else:
            st.caption("No goal history yet.")

    show_detail()

st.markdown("<br>", unsafe_allow_html=True)

# ── Scheduling & capacity ─────────────────────────────────────────────────────
st.markdown("### Scheduling & Capacity — Fixed")
pto_col, cap_col = st.columns([1.4, 1])

AREA_CHOICES = ["Chairside", "Removables", "Splints", "Partial", "Ortho",
                "Crown & Bridge", "CAD/CAM", "Ceramics", "Fixed QC", "Model/Die"]

with pto_col:
    upcoming = goals_store.list_upcoming_pto(DASHBOARD, days=14)
    ool = goals_store.list_upcoming_out_of_lab(DASHBOARD, days=14)
    sched_rows = (
        [{"Technician": p["name"], "Area": p["area"], "Date": p["pto_date"],
          "Type": f"PTO ({p['portion']})", "Note": p["note"]} for p in upcoming]
        + [{"Technician": o["name"], "Area": o["area"], "Date": o["work_date"],
            "Type": f"Out of lab → {o['target_area']}", "Note": o["note"]} for o in ool]
    )
    if sched_rows:
        st.dataframe(pd.DataFrame(sched_rows).sort_values("Date"),
                     hide_index=True, use_container_width=True)
    else:
        st.caption("Nothing scheduled in the next 14 days.")

    with st.form("add_sched_form", clear_on_submit=True):
        st.markdown("**Schedule PTO / Out-of-Lab**")
        f1, f2, f3 = st.columns([2, 1, 1.4])
        who = f1.selectbox("Technician", techs["name"].tolist())
        sched_date = f2.date_input("Date", value=today + timedelta(days=1))
        entry_type = f3.radio("Type", ["PTO full", "PTO half", "Out of lab"], horizontal=True)
        target_area = st.selectbox("Charge labor to (out-of-lab only)", AREA_CHOICES)
        note = st.text_input("Note (optional)")
        if st.form_submit_button("Add", type="primary"):
            code = techs[techs["name"] == who]["tech_code"].iloc[0]
            if entry_type == "Out of lab":
                goals_store.add_out_of_lab(code, sched_date, target_area, note)
                st.success(f"{who} scheduled out of lab {sched_date} — labor charges to {target_area}.")
            else:
                portion = "full" if entry_type == "PTO full" else "half"
                goals_store.add_pto(code, sched_date, portion, note)
                st.success(f"Added {portion}-day PTO for {who} on {sched_date}.")
            st.rerun()

with cap_col:
    days_ahead = [today + timedelta(days=i) for i in range(7)]
    caps = [goals_store.projected_capacity_pct(DASHBOARD, d) for d in days_ahead]
    fig = go.Figure(go.Scatter(
        x=[f"{d.strftime('%a')} {d.month}/{d.day}" for d in days_ahead],
        y=caps, mode="lines+markers",
        line=dict(color=COLORS["acc"], width=2), marker=dict(size=7),
    ))
    fig.update_layout(
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"], font_color=COLORS["txt"],
        height=220, margin=dict(l=10, r=10, t=30, b=10),
        yaxis=dict(title="Capacity %", range=[0, 105], gridcolor=COLORS["bdr"]),
        xaxis=dict(gridcolor=COLORS["bdr"]),
        title=dict(text="Next 7 Days — Projected Capacity", font=dict(size=13)),
    )
    st.plotly_chart(fig, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Labor (estimated) — local-only, never leaves this machine ────────────────
st.markdown("### Labor — Estimated ($)")
labor = goals_store.get_labor_history(DASHBOARD)
if labor:
    ldf = pd.DataFrame(labor)
    today_str = today.isoformat()
    today_total = ldf[ldf["work_date"] == today_str]["dollars"].sum()
    window_total = ldf["dollars"].sum()
    lc1, lc2 = st.columns(2)
    with lc1:
        st.markdown(tile_html("Labor Today (est.)", f"${today_total:,.0f}",
                              "hourly + piece + salary/250"), unsafe_allow_html=True)
    with lc2:
        st.markdown(tile_html("Labor — Recorded Window", f"${window_total:,.0f}",
                              f"{ldf['work_date'].min()} → {ldf['work_date'].max()}"),
                   unsafe_allow_html=True)
    by_area = (ldf.groupby("area")["dollars"].sum().reset_index()
                  .rename(columns={"area": "Area", "dollars": "Labor $"})
                  .sort_values("Labor $", ascending=False))
    st.dataframe(by_area, hide_index=True, use_container_width=True,
                 column_config={"Labor $": st.column_config.NumberColumn(format="$%.0f")})
    if (ldf["source"] == "actual").any():
        st.caption("Includes reconciled payroll actuals where available; estimates elsewhere.")
    else:
        st.caption("Estimates only — payroll reconciliation not yet imported.")
else:
    st.caption("No labor data yet — set pay types in Employee Settings; the hourly "
               "pipeline records estimates from there. (Empty on the cloud copy by design.)")

st.caption("Artistic Dental Studio · Fixed Dashboard · goals, pay and scheduling persist immediately to the local store")
