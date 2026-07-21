"""
Artistic Dental Studio — Removable Dashboard
==============================================
Manager workspace for Removables / Chairside / Splints / Partial / Ortho /
Rem QC. Mirrors pages/5_Fixed_Dashboard.py's pattern exactly — editable
technician goals (goals_store.py, SQLite, effective-dated) and PTO entry,
both reflected immediately since production numbers come from
cache/latest/tech_production.csv while goals/PTO are read live from
goals_store.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import goals_store
from manager_theme import COLORS, BASE_CSS, tile_html, meter_html, status_color

BASE_DIR   = Path(__file__).parent.parent
LATEST_DIR = BASE_DIR / "cache" / "latest"
DASHBOARD  = "Removable"

st.set_page_config(
    page_title="Removable Dashboard — Artistic Dental",
    page_icon="🦶",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(BASE_CSS, unsafe_allow_html=True)


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
  &nbsp;›&nbsp;<b style="color:{COLORS['txt']};">Removable Dashboard</b>
</div>
<h1 style="margin-top:0;">Removable — Removables · Chairside · Splints · Partial · Ortho</h1>
<p style="color:{COLORS['txt2']};font-size:14px;margin-top:-6px;">
  Manager view · period {period} · goals are editable below, PTO adjusts projected capacity automatically
</p>
""", unsafe_allow_html=True)

if techs.empty:
    st.warning(
        "No Removable technician data found yet. Run `py production_pipeline.py` "
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
rem_fin = fin[fin["dashboard"] == "Removable"] if not fin.empty else pd.DataFrame()
remake_pct = rem_fin["remake_rate_pct"].iloc[0] if not rem_fin.empty else 0.0
remake_disc = rem_fin["remake_discount"].iloc[0] if not rem_fin.empty else 0.0
net_sales = rem_fin["net_sales"].iloc[0] if not rem_fin.empty else 0.0

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
AREA_ORDER = ["Removables", "Chairside", "Splints", "Partial", "Ortho", "Rem QC", "Implants", "Surgical Guides"]
areas_present = [a for a in AREA_ORDER if a in techs["area"].unique()]

if "removable_area" not in st.session_state:
    st.session_state.removable_area = None  # None = show all Removable technicians

if st.session_state.removable_area is not None:
    if st.button("← Show all Removable technicians"):
        st.session_state.removable_area = None
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
        active_flag = "border:1px solid " + COLORS["acc"] + ";" if st.session_state.removable_area == area else ""
        st.markdown(f"""
        <div class="mgr-card" style="{active_flag}">
          <div class="name">{area}</div>
          <div class="sub">{len(sub)} technicians</div>
          {meter_html(a_pct)}
          <div style="text-align:right;font-size:13px;font-weight:700;margin-top:4px;">{a_pct}%</div>
          {station_html}
        </div>
        """, unsafe_allow_html=True)
        if st.button(f"View {area}", key=f"btn_rem_{area}", use_container_width=True):
            st.session_state.removable_area = area
            st.rerun()

st.markdown("<br>", unsafe_allow_html=True)

# ── Technician table ─────────────────────────────────────────────────────────
sel_area = st.session_state.removable_area
st.markdown(f"### {sel_area} — Technicians" if sel_area else f"### All Removable — Technicians ({len(techs)})")

view = techs[techs["area"] == sel_area].copy() if sel_area else techs.copy()
view = view.sort_values("name")
view["PTO"] = view["pto_today"].apply(lambda p: {"full": "Full day", "half": "Half day"}.get(p, "—"))
view["New Goal"] = view["goal"]

display_df = view.rename(columns={
    "name": "Technician", "station": "Station", "goal": "Today's Goal",
    "today_units": "Completed", "pct_of_goal": "% of Goal",
})

edited = st.data_editor(
    display_df,
    column_order=["Technician", "Station", "Today's Goal", "Completed", "% of Goal", "PTO", "New Goal"],
    disabled=["Technician", "Station", "Today's Goal", "Completed", "% of Goal", "PTO"],
    hide_index=True,
    use_container_width=True,
    key="removable_tech_editor",
)

if st.button("Save Goal Changes", type="primary", key="save_removable_goals"):
    changed = 0
    for _, row in edited.iterrows():
        if row["New Goal"] != row["Today's Goal"] and row["New Goal"] > 0:
            goals_store.set_goal(row["tech_code"], float(row["New Goal"]))
            changed += 1
    if changed:
        st.cache_data.clear()
        st.success(f"Updated {changed} goal(s).")
        st.rerun()
    else:
        st.info("No goal changes to save.")

# ── Drill-down ─────────────────────────────────────────────────────────────────
st.markdown("### Technician Detail")
pick = st.selectbox("View details for:", view["Technician"].tolist() if "Technician" in view.columns
                     else view["name"].tolist(), index=None, placeholder="Select a technician…",
                     key="removable_detail_pick")

if pick:
    t = view[view["name"] == pick].iloc[0]

    @st.dialog(f"{t['name']}", width="large")
    def show_detail():
        st.caption(f"{t['station']} · {sel_area} · Removable")
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

# ── PTO & capacity ─────────────────────────────────────────────────────────────
st.markdown("### PTO & Capacity — Removable")
pto_col, cap_col = st.columns([1.4, 1])

with pto_col:
    upcoming = goals_store.list_upcoming_pto(DASHBOARD, days=14)
    if upcoming:
        st.dataframe(
            pd.DataFrame(upcoming)[["name", "area", "pto_date", "portion", "note"]]
              .rename(columns={"name": "Technician", "area": "Area",
                               "pto_date": "Date", "portion": "Portion", "note": "Note"}),
            hide_index=True, use_container_width=True,
        )
    else:
        st.caption("No upcoming PTO scheduled in the next 14 days.")

    with st.form("add_pto_form_removable", clear_on_submit=True):
        st.markdown("**Add PTO**")
        f1, f2, f3 = st.columns([2, 1, 1])
        who = f1.selectbox("Technician", techs["name"].tolist(), key="removable_pto_who")
        pto_date_input = f2.date_input("Date", value=today + timedelta(days=1), key="removable_pto_date")
        portion = f3.radio("Portion", ["full", "half"], horizontal=True, key="removable_pto_portion")
        note = st.text_input("Note (optional)", key="removable_pto_note")
        if st.form_submit_button("Add PTO", type="primary"):
            code = techs[techs["name"] == who]["tech_code"].iloc[0]
            goals_store.add_pto(code, pto_date_input, portion, note)
            st.success(f"Added {portion}-day PTO for {who} on {pto_date_input}.")
            st.rerun()

with cap_col:
    days_ahead = [today + timedelta(days=i) for i in range(7)]
    caps = [goals_store.projected_capacity_pct(DASHBOARD, d) for d in days_ahead]
    fig = go.Figure(go.Scatter(
        x=[f"{d.strftime('%a')} {d.month}/{d.day}" for d in days_ahead],
        y=caps, mode="lines+markers",
        line=dict(color=COLORS["pur"], width=2), marker=dict(size=7),
    ))
    fig.update_layout(
        paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"], font_color=COLORS["txt"],
        height=220, margin=dict(l=10, r=10, t=30, b=10),
        yaxis=dict(title="Capacity %", range=[0, 105], gridcolor=COLORS["bdr"]),
        xaxis=dict(gridcolor=COLORS["bdr"]),
        title=dict(text="Next 7 Days — Projected Capacity", font=dict(size=13)),
    )
    st.plotly_chart(fig, use_container_width=True)

st.caption("Artistic Dental Studio · Removable Dashboard · goals and PTO persist immediately to goals_store.py")
