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
    initial_sidebar_state="expanded",
)
st.markdown(BASE_CSS, unsafe_allow_html=True)
from env_notice import manager_tools_available, render_lan_notice
if not manager_tools_available():
    render_lan_notice("Fixed manager dashboard")
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

# Pull live goal + PTO/out-of-lab per technician, and an EFFECTIVE goal for
# today that removes capacity for people who are out: full PTO or out-of-lab →
# 0, half-day PTO → 50%. All "% of goal" math uses eff_goal so the department
# isn't measured against goals for people who aren't there today.
techs = techs.copy()
techs["goal"] = techs["tech_code"].apply(lambda c: goals_store.get_current_goal(c) or 0)
techs["pto_today"] = techs["tech_code"].apply(lambda c: goals_store.get_pto_on(c, today))
techs["out_today"] = techs["tech_code"].apply(lambda c: goals_store.get_out_of_lab_on(c, today))

def _avail_factor(r):
    if r["out_today"]:
        return 0.0
    return {"full": 0.0, "half": 0.5}.get(r["pto_today"], 1.0)
techs["avail_factor"] = techs.apply(_avail_factor, axis=1)
techs["eff_goal"] = techs["goal"] * techs["avail_factor"]
techs["pct_of_goal"] = techs.apply(
    lambda r: (round(r["today_units"] / r["eff_goal"] * 100, 1) if r["eff_goal"] > 0
               else None), axis=1
)

# ── KPI tiles ──────────────────────────────────────────────────────────────────
overall_pct = (
    round(techs["today_units"].sum() / techs["eff_goal"].sum() * 100, 1)
    if techs["eff_goal"].sum() else 0.0
)
fixed_fin = fin[fin["dashboard"] == "Fixed"] if not fin.empty else pd.DataFrame()
remake_pct = fixed_fin["remake_rate_pct"].iloc[0] if not fixed_fin.empty else 0.0
remake_disc = fixed_fin["remake_discount"].iloc[0] if not fixed_fin.empty else 0.0
net_sales = fixed_fin["net_sales"].iloc[0] if not fixed_fin.empty else 0.0

# Out today = fully unavailable (full PTO or out-of-lab); half-day still counts as present.
out_today = int((techs["avail_factor"] == 0.0).sum())
active_today = len(techs) - out_today
projected_pct = goals_store.projected_capacity_pct(DASHBOARD, today)

# Labor & materials data — computed up front so the KPI tiles can sit with
# the main KPI row; drill-down tables render further down the page.
from materials_calc import dashboard_materials
labor = goals_store.get_labor_history(DASHBOARD)
mats = dashboard_materials(DASHBOARD)   # from accumulated local history
ldf = pd.DataFrame(labor) if labor else pd.DataFrame()
today_str = today.isoformat()
week_start = today - timedelta(days=today.weekday())   # Monday

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(tile_html("Overall % of Goal — Today", f"{overall_pct}%",
                          f"{int((techs['goal'] > 0).sum())}/{len(techs)} techs with a goal set",
                          status_color(overall_pct)), unsafe_allow_html=True)
with c2:
    st.markdown(tile_html("Remake Rate ($)", f"{remake_pct}%",
                          f"${remake_disc:,.0f} of ${net_sales:,.0f} in sales",
                          status_color(100 - remake_pct, good=95, warn=90)),
               unsafe_allow_html=True)
with c3:
    st.markdown(tile_html("Techs Active Today", f"{active_today} / {len(techs)}",
                          f"{out_today} out (PTO/out-of-lab)"), unsafe_allow_html=True)
with c4:
    st.markdown(tile_html("Projected Output Today", f"{projected_pct}%",
                          "of full-roster capacity, PTO-adjusted",
                          status_color(projected_pct)), unsafe_allow_html=True)

# Labor & Materials both expressed as % of DEPT SALES over the same window as
# the sales report (period_start..period_end from prod_by_dept). Set that report
# to a YTD range and these become YTD %s automatically.
p_start = fixed_fin["period_start"].iloc[0] if ("period_start" in fixed_fin.columns and not fixed_fin.empty) else ""
p_end   = fixed_fin["period_end"].iloc[0]   if ("period_end" in fixed_fin.columns and not fixed_fin.empty) else ""

# Labor windows: YTD uses the exact YTD dept sales (the report is YTD); the
# 30-day figure pro-rates YTD dept sales to a 30-day window (no separate 30-day
# per-dept sales exists), so its sales denominator is approximate (~).
_ytd_start = date(today.year, 1, 1)
days_ytd = max((today - _ytd_start).days + 1, 1)
labor_ytd = ldf[ldf["work_date"] >= _ytd_start.isoformat()]["dollars"].sum() if not ldf.empty else 0.0
_30d_cut = (today - timedelta(days=30)).isoformat()
labor_30d = ldf[ldf["work_date"] >= _30d_cut]["dollars"].sum() if not ldf.empty else 0.0
sales_30d = net_sales * 30 / days_ytd if net_sales else 0.0

mat_period = 0.0
if not mats.empty and p_start and p_end:
    _m = (mats["issue_date"].dt.date >= pd.to_datetime(p_start).date()) & \
         (mats["issue_date"].dt.date <= pd.to_datetime(p_end).date())
    mat_period = mats[_m]["issued_value"].sum()

# Labor & Materials KPI row — directly under the production KPIs
lc1, lc2, lc3, lc4 = st.columns(4)
with lc1:
    pct = (labor_30d / sales_30d * 100) if sales_30d else 0.0
    st.markdown(tile_html("Labor % — Last 30 Days", f"{pct:.1f}%",
                          f"${labor_30d:,.0f} labor / ~${sales_30d:,.0f} sales"),
               unsafe_allow_html=True)
with lc2:
    pct = (labor_ytd / net_sales * 100) if net_sales else 0.0
    st.markdown(tile_html("Labor % — YTD", f"{pct:.1f}%",
                          f"${labor_ytd:,.0f} labor / ${net_sales:,.0f} sales"),
               unsafe_allow_html=True)
with lc3:
    pct = (mat_period / net_sales * 100) if net_sales else 0.0
    st.markdown(tile_html("Materials % of Sales", f"{pct:.1f}%",
                          f"${mat_period:,.0f} mat / ${net_sales:,.0f} sales"),
               unsafe_allow_html=True)
with lc4:
    v = mats[mats["issue_date"].dt.year == today.year]["issued_value"].sum() if not mats.empty else 0
    st.markdown(tile_html("Materials — YTD $", f"${v:,.0f}", f"{today.year} to date"),
               unsafe_allow_html=True)

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
    a_pct = (round(sub["today_units"].sum() / sub["eff_goal"].sum() * 100, 1)
             if sub["eff_goal"].sum() else 0.0)

    station_html = ""
    stations = sub.groupby("station")["today_units"].sum()
    station_goals = sub.groupby("station")["eff_goal"].sum()
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
view["pto_detail"] = view["tech_code"].apply(lambda c: goals_store.get_pto_detail_on(c, today))
view["PTO / Out"] = view.apply(
    lambda r: f"Out — {r['out_today']}" if r["out_today"]
    else (f"{'PTO' if r['pto_detail']['paid'] else 'Unpaid'} "
          f"{r['pto_detail']['portion'].title()}" if r["pto_detail"] else "—"), axis=1)

display_df = view.rename(columns={
    "name": "Technician", "station": "Station", "goal": "Today's Goal",
    "today_units": "Completed", "pct_of_goal": "% of Goal",
})

st.dataframe(
    display_df[["Technician", "Station", "Today's Goal", "Completed", "% of Goal", "PTO / Out"]],
    hide_index=True,
    use_container_width=True,
)

with st.expander("⚙️ Employee Settings — pay types, task rates & goals"):
    from settings_page import render_settings_body
    render_settings_body("Fixed", "Fixed")

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

with pto_col:
    upcoming = goals_store.list_upcoming_pto(DASHBOARD, days=14)
    ool = goals_store.list_upcoming_out_of_lab(DASHBOARD, days=14)
    sched_rows = (
        [{"Technician": p["name"], "Area": p["area"], "Date": p["pto_date"],
          "Type": f"{'PTO' if p.get('paid', 1) else 'Unpaid'} ({p['portion']})",
          "Note": p["note"]} for p in upcoming]
        + [{"Technician": o["name"], "Area": o["area"], "Date": o["work_date"],
            "Type": f"Out of lab → {o['target_area']}", "Note": o["note"]} for o in ool]
    )
    if sched_rows:
        st.dataframe(pd.DataFrame(sched_rows).sort_values("Date"),
                     hide_index=True, use_container_width=True)
    else:
        st.caption("Nothing scheduled in the next 14 days.")
    st.page_link("pages/7_Team_Schedule.py",
                 label="📅 Add PTO / Out-of-Lab → Team Schedule (no password needed)")

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

# ── Labor & Materials drill-down — local-only, never leaves this machine ─────
st.markdown("### Labor & Materials — Drill-down")

# Drill-down level 1: by area — labor & materials as % of dept sales
area_parts = []
if not ldf.empty:
    area_parts.append(ldf.groupby("area")["dollars"].sum().rename("Labor $"))
if not mats.empty:
    area_parts.append(mats.groupby("area")["issued_value"].sum().rename("Materials $"))
if area_parts:
    by_area = pd.concat(area_parts, axis=1).fillna(0).reset_index().rename(columns={"index": "Area", "area": "Area"})
    if net_sales:
        if "Labor $" in by_area.columns:
            by_area["Labor % Sales"] = (by_area["Labor $"] / net_sales * 100).round(1)
        if "Materials $" in by_area.columns:
            by_area["Materials % Sales"] = (by_area["Materials $"] / net_sales * 100).round(1)
    st.markdown(f"**By area** — % against dept sales (${net_sales:,.0f})")
    st.dataframe(by_area.sort_values(by_area.columns[1], ascending=False),
                 hide_index=True, use_container_width=True,
                 column_config={
                     **{c: st.column_config.NumberColumn(format="$%.0f")
                        for c in ("Labor $", "Materials $") if c in by_area.columns},
                     **{c: st.column_config.NumberColumn(format="%.1f%%")
                        for c in ("Labor % Sales", "Materials % Sales") if c in by_area.columns}})

# Drill-down level 2: by employee
emp_l, emp_m = st.columns(2)
with emp_l:
    st.markdown("**Labor by employee** — % of dept sales")
    if not ldf.empty:
        name_map = dict(zip(techs["tech_code"], techs["name"]))
        le = (ldf.groupby(["tech_code", "pay_type"])["dollars"].sum().reset_index())
        le["Employee"] = le["tech_code"].map(name_map).fillna(le["tech_code"])
        le = le.rename(columns={"pay_type": "Pay Type", "dollars": "Labor $"})
        if net_sales:
            le["% of Sales"] = (le["Labor $"] / net_sales * 100).round(2)
        cols = ["Employee", "Pay Type", "Labor $"] + (["% of Sales"] if net_sales else [])
        st.dataframe(le[cols].sort_values("Labor $", ascending=False),
                     hide_index=True, use_container_width=True,
                     column_config={"Labor $": st.column_config.NumberColumn(format="$%.2f"),
                                    "% of Sales": st.column_config.NumberColumn(format="%.2f%%")})
    else:
        st.caption("No labor data yet — set pay types in ⚙️ Employee Settings, or import Proliant actuals.")
with emp_m:
    st.markdown("**Materials by employee (requester)**")
    if not mats.empty:
        me = (mats.groupby("matched_name")
                  .agg(**{"Materials $": ("issued_value", "sum"), "Items": ("qty", "sum")})
                  .reset_index().rename(columns={"matched_name": "Employee"})
                  .sort_values("Materials $", ascending=False))
        st.dataframe(me, hide_index=True, use_container_width=True,
                     column_config={"Materials $": st.column_config.NumberColumn(format="$%.2f")})
    else:
        st.caption("No Clixon materials export in C:\\MT_Reports_Local — schedule the "
                   "'Issued' report to enable materials tracking.")

if not ldf.empty and (ldf["source"] == "actual").any():
    st.caption("Labor includes reconciled payroll actuals where available; estimates elsewhere.")
st.caption("Materials are credited to a technician only when the requester matches a current "
           "active employee; everything else rolls into 'Department (general)'.")

st.caption("Artistic Dental Studio · Fixed Dashboard · goals, pay and scheduling persist immediately to the local store")
