"""
Artistic Dental Studio — Logistics Dashboard
=============================================
Drop this file into pages/ alongside 1_Executive_Dashboard.py and
2_Doctor_Retention.py.

Reads from:
  cache/latest/cases_logistics.csv
  cache/latest/logistics_summary.csv

Both are produced by pipeline.py via pipeline_logistics.compute_logistics().

The page degrades gracefully if data is missing or if pipeline_logistics.py
hasn't been integrated yet — shows an info message rather than crashing.
"""

import math
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ── Locate cache ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
LATEST_DIR = BASE_DIR / "cache" / "latest"


# ── Hardened formatters (same pattern as Exec Dashboard fix) ──────────────────
def _to_float(val, default=0.0):
    if val is None:
        return default
    try:
        f = float(val)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(f) else f


def fmt_currency(val):
    v = _to_float(val)
    if v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def fmt_int(val):
    return f"{int(_to_float(val)):,}"


# ── Brand palette (mirrors dashboard.py) ──────────────────────────────────────
COLORS = {
    "navy":  "#1a2744",
    "teal":  "#0a8f8f",
    "gold":  "#d4a843",
    "green": "#2ecc71",
    "red":   "#e74c3c",
    "light": "#f0f4f8",
    "muted": "#6c7a8a",
}

# ── Page config + CSS ─────────────────────────────────────────────────────────
st.set_page_config(page_title="Logistics — Artistic Dental",
                   page_icon="📦", layout="wide")

st.markdown(f"""
<style>
  .kpi-card {{
      background: white; border-radius: 12px;
      padding: 1.0rem 1.2rem;
      box-shadow: 0 2px 8px rgba(26,39,68,.07);
      border-left: 4px solid {COLORS['teal']};
      margin-bottom: 0.4rem;
  }}
  .kpi-label {{ color: {COLORS['muted']}; font-size: 0.72rem; font-weight: 600;
                letter-spacing: .05em; text-transform: uppercase; margin-bottom: 3px; }}
  .kpi-value {{ color: {COLORS['navy']}; font-size: 1.6rem; font-weight: 600; line-height:1; }}
  .kpi-sub   {{ color: {COLORS['muted']}; font-size: 0.72rem; margin-top: 3px; }}
  .kpi-warn  {{ border-left-color: {COLORS['red']}; }}
  .kpi-ok    {{ border-left-color: {COLORS['green']}; }}
  .section-head {{
      font-family: 'DM Serif Display', serif;
      color: {COLORS['navy']};
      font-size: 1.1rem;
      margin: 1.2rem 0 .5rem;
      padding-bottom: .25rem;
      border-bottom: 2px solid {COLORS['teal']};
  }}
</style>
""", unsafe_allow_html=True)


def kpi_card(label, value, sub="", status="neutral"):
    cls = {"ok": "kpi-ok", "warn": "kpi-warn"}.get(status, "")
    st.markdown(f"""
    <div class="kpi-card {cls}">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      <div class="kpi-sub">{sub}</div>
    </div>""", unsafe_allow_html=True)


def section(title):
    st.markdown(f'<div class="section-head">{title}</div>', unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)  # 10-minute cache
def load_logistics_data():
    cases_path = LATEST_DIR / "cases_logistics.csv"
    summary_path = LATEST_DIR / "logistics_summary.csv"
    if not cases_path.exists():
        return None, None
    cases = pd.read_csv(cases_path,
                        parse_dates=["Cases_DateIn", "Cases_DueDate"],
                        low_memory=False)
    summary = pd.read_csv(summary_path) if summary_path.exists() else pd.DataFrame()
    return cases, summary


# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_refresh = st.columns([4, 1])
with col_title:
    st.markdown("## 📦 Case Flow & Backlog")
    st.caption("Open cases, where they're stuck, and what's behind schedule.")
with col_refresh:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()


cases, summary = load_logistics_data()

if cases is None or cases.empty:
    st.warning(
        "**Logistics data not yet available.**\n\n"
        "This page needs `cache/latest/cases_logistics.csv` to be present. "
        "If you've just integrated `pipeline_logistics.py`, run `py pipeline.py` "
        "once to generate the cache, then refresh."
    )
    st.stop()


# ── KPI row ───────────────────────────────────────────────────────────────────
s = summary.iloc[0] if not summary.empty else {}

cols = st.columns(6)
with cols[0]:
    kpi_card("Open Cases", fmt_int(s.get("open_cases", len(cases))), "currently in-flight")
with cols[1]:
    overdue = int(s.get("overdue_cases", 0) or 0)
    kpi_card("Past Due Date", fmt_int(overdue),
             "Cases_DueDate has passed",
             status="warn" if overdue > 0 else "ok")
with cols[2]:
    stuck = int(s.get("stuck_cases", 0) or 0)
    kpi_card("Stuck at Station", fmt_int(stuck),
             "exceeds dept threshold",
             status="warn" if stuck > 0 else "ok")
with cols[3]:
    behind = int(s.get("behind_total", 0) or 0)
    kpi_card("Behind (any reason)", fmt_int(behind),
             "union of all flags",
             status="warn" if behind > 0 else "ok")
with cols[4]:
    kpi_card("$ at Risk", fmt_currency(s.get("value_at_risk", 0)),
             "TotalCharge of all behind cases")
with cols[5]:
    oldest = int(s.get("oldest_age_days", 0) or 0)
    kpi_card("Oldest Case", f"{oldest} d",
             f"median age: {int(s.get('median_age_days',0) or 0)} d",
             status="warn" if oldest > 30 else "neutral")


# ── Filters ───────────────────────────────────────────────────────────────────
section("Filters")
f1, f2, f3 = st.columns([2, 2, 1])
with f1:
    depts_available = sorted(cases["pseudo_dept"].dropna().unique().tolist())
    selected_depts = st.multiselect("Department", depts_available, default=depts_available)
with f2:
    statuses_available = sorted(cases["Cases_Status"].dropna().unique().tolist())
    selected_statuses = st.multiselect("Status", statuses_available, default=statuses_available)
with f3:
    show_behind_only = st.checkbox("Behind only", value=False)

view = cases[
    cases["pseudo_dept"].isin(selected_depts) &
    cases["Cases_Status"].isin(selected_statuses)
].copy()
if show_behind_only:
    view = view[view["is_behind"]]


# ── Department breakdown ──────────────────────────────────────────────────────
section("Department breakdown")

dept_summary = (
    view.groupby("pseudo_dept")
    .agg(open_count=("Cases_CaseNumber", "count"),
         overdue=("flag_past_due", "sum"),
         stuck=("flag_stuck", "sum"),
         behind=("is_behind", "sum"),
         median_age=("age_days", "median"),
         value_at_risk=("Cases_TotalCharge", lambda x: x[view.loc[x.index, "is_behind"]].sum()))
    .reset_index()
    .sort_values("open_count", ascending=False)
)

c1, c2 = st.columns([2, 3])
with c1:
    if not dept_summary.empty:
        fig = go.Figure(go.Bar(
            x=dept_summary["open_count"],
            y=dept_summary["pseudo_dept"],
            orientation="h",
            marker=dict(
                color=dept_summary["behind"] / dept_summary["open_count"].replace(0, 1),
                colorscale=[[0, COLORS["teal"]], [0.3, COLORS["gold"]], [1, COLORS["red"]]],
                showscale=True,
                colorbar=dict(title="% behind", thickness=10, ticksuffix="%",
                              tickformat=".0%")
            ),
            text=dept_summary.apply(
                lambda r: f"  {int(r['open_count'])} ({int(r['behind'])} behind)", axis=1),
            textposition="outside",
        ))
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                          yaxis=dict(autorange="reversed"),
                          margin=dict(l=10, r=40, t=20, b=10),
                          height=320,
                          xaxis_title="Open cases")
        st.plotly_chart(fig, use_container_width=True)

with c2:
    if not dept_summary.empty:
        display = dept_summary.copy()
        display["value_at_risk"] = display["value_at_risk"].apply(fmt_currency)
        display["median_age"] = display["median_age"].apply(lambda v: f"{int(v) if pd.notna(v) else 0}d")
        display = display.rename(columns={
            "pseudo_dept": "Department",
            "open_count": "Open",
            "overdue": "Past Due",
            "stuck": "Stuck",
            "behind": "Behind",
            "median_age": "Median Age",
            "value_at_risk": "$ at Risk",
        })
        st.dataframe(display, use_container_width=True, height=320, hide_index=True)


# ── Top stuck locations ───────────────────────────────────────────────────────
section("Top stuck locations")

behind = view[view["is_behind"]]
if not behind.empty:
    loc_summary = (
        behind.groupby("Cases_LastLocation")
        .agg(cases=("Cases_CaseNumber", "count"),
             avg_days_late=("days_overdue", "mean"),
             max_days_late=("days_overdue", "max"),
             avg_days_at_station=("days_at_station", "mean"),
             value=("Cases_TotalCharge", "sum"))
        .reset_index()
        .sort_values("cases", ascending=False)
        .head(20)
    )
    display = loc_summary.copy()
    display["avg_days_late"] = display["avg_days_late"].round(1)
    display["avg_days_at_station"] = display["avg_days_at_station"].round(1)
    display["max_days_late"] = display["max_days_late"].astype(int)
    display["value"] = display["value"].apply(fmt_currency)
    display = display.rename(columns={
        "Cases_LastLocation": "Location",
        "cases": "Behind",
        "avg_days_late": "Avg Days Late",
        "max_days_late": "Max Days Late",
        "avg_days_at_station": "Avg Days at Station",
        "value": "Value",
    })
    st.dataframe(display, use_container_width=True, hide_index=True)
else:
    st.success("No cases flagged behind in the current filter.")


# ── Aging waterfall ───────────────────────────────────────────────────────────
section("Aging waterfall — open cases by age bucket")

def bucket(age):
    if age <= 3:  return "0-3d"
    if age <= 7:  return "4-7d"
    if age <= 14: return "8-14d"
    if age <= 30: return "15-30d"
    return "30+d"

if not view.empty:
    waterfall = view.copy()
    waterfall["bucket"] = waterfall["age_days"].apply(bucket)
    bucket_order = ["0-3d", "4-7d", "8-14d", "15-30d", "30+d"]
    pivot = (
        waterfall.groupby(["bucket", "pseudo_dept"])
        .size()
        .reset_index(name="count")
    )
    pivot["bucket"] = pd.Categorical(pivot["bucket"], categories=bucket_order, ordered=True)
    pivot = pivot.sort_values("bucket")

    fig = px.bar(pivot, x="bucket", y="count", color="pseudo_dept",
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                      margin=dict(l=10, r=10, t=20, b=10),
                      height=300,
                      legend=dict(orientation="h", y=-0.2),
                      xaxis_title="Age in lab",
                      yaxis_title="Cases",
                      barmode="stack")
    st.plotly_chart(fig, use_container_width=True)


# ── Case detail ───────────────────────────────────────────────────────────────
section(f"Case detail ({len(view)} shown)")

display_cols = [
    ("Cases_CaseNumber", "Case #"),
    ("Cases_DoctorName", "Doctor"),
    ("Cases_CustomerID", "Customer"),
    ("Cases_PanNumber",  "Pan#"),
    ("pseudo_dept",      "Dept"),
    ("Cases_Status",     "Status"),
    ("Cases_LastLocation", "Location"),
    ("Cases_DateIn",     "Date In"),
    ("Cases_DueDate",    "Due"),
    ("age_days",         "Age"),
    ("days_at_station",  "At Loc"),
    ("days_overdue",     "Days Late"),
    ("Cases_TotalCharge","$"),
    ("is_behind",        "Behind"),
]
cols_present = [(c, n) for c, n in display_cols if c in view.columns]
view_display = view[[c for c, _ in cols_present]].copy()
view_display.columns = [n for _, n in cols_present]

# Sort by days late descending — most urgent first
if "Days Late" in view_display.columns:
    view_display = view_display.sort_values("Days Late", ascending=False)

# Format $ column
if "$" in view_display.columns:
    view_display["$"] = view_display["$"].apply(fmt_currency)

st.dataframe(view_display, use_container_width=True, height=500, hide_index=True)

# Download
csv_bytes = view.to_csv(index=False).encode("utf-8")
st.download_button("⬇ Download filtered case list (CSV)",
                   data=csv_bytes,
                   file_name=f"logistics_cases_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                   mime="text/csv")

# Footer
st.divider()
if not summary.empty and "computed_at" in summary.columns:
    st.caption(f"Last pipeline run: **{summary.iloc[0]['computed_at']}** · "
               f"Page refreshes every 10 min unless you click the Refresh button.")
