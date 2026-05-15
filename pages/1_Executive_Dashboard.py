"""
Artistic Dental Studio — Executive Dashboard (sub-page)
========================================================
Financial KPIs: revenue, WIP, remakes, profitability, Pareto, implants.
Reads pre-computed CSVs from cache/latest/ written by pipeline.py.

This is a sub-page of the multi-page app. Entry point is dashboard.py.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime

import yaml
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Setup ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent      # ← parent of pages/
LATEST_DIR = BASE_DIR / "cache" / "latest"

with open(BASE_DIR / "config.yaml") as f:
    CFG = yaml.safe_load(f)

st.set_page_config(
    page_title="Executive Dashboard — Artistic Dental",
    page_icon="📊",
    layout="wide",
)

# ── Dark Palette (matches Lexie's v3) ──────────────────────────────────────────
COLORS = {
    "bg":     "#0d1117",
    "sfc":    "#161b22",
    "sfc2":   "#1c2128",
    "bdr":    "#30363d",
    "bdr2":   "#21262d",
    "txt":    "#ffffff",
    "txt2":   "#ffffff",
    "acc":    "#58a6ff",
    "grn":    "#3fb950",
    "ylw":    "#d29922",
    "red":    "#f85149",
    "pur":    "#a371f7",
    "org":    "#f0883e",
    "gold":   "#d29922",
    "navy":   "#1a2744",
    "teal":   "#0a8f8f",
}

# ── Custom CSS (dark) ──────────────────────────────────────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@500;700;800&family=DM+Sans:wght@400;500;600&display=swap');

html, body, [class*="css"], .stApp {{
    background-color: {COLORS['bg']} !important;
    color: {COLORS['txt']};
    font-family: 'DM Sans', sans-serif;
}}
h1, h2, h3 {{
    font-family: 'Syne', sans-serif !important;
    color: {COLORS['txt']} !important;
}}
.block-container {{ padding: 1rem 1.6rem; max-width: 1320px; }}

/* KPI cards */
.kpi-card {{
    background: {COLORS['sfc']};
    border: 1px solid {COLORS['bdr']};
    border-radius: 10px;
    padding: 14px 18px;
    border-left: 3px solid {COLORS['acc']};
    margin-bottom: 6px;
}}
.kpi-label {{
    color: {COLORS['txt2']};
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.4px;
    text-transform: uppercase;
    margin-bottom: 6px;
}}
.kpi-value {{
    color: {COLORS['txt']};
    font-family: 'Syne', sans-serif;
    font-size: 24px;
    font-weight: 700;
    line-height: 1;
}}
.kpi-sub {{
    color: {COLORS['txt2']};
    font-size: 11px;
    margin-top: 6px;
}}
.kpi-ok    {{ border-left-color: {COLORS['grn']}; }}
.kpi-warn  {{ border-left-color: {COLORS['red']}; }}

/* Section headers */
.section-head {{
    font-family: 'Syne', sans-serif;
    color: {COLORS['txt']};
    font-size: 16px;
    margin: 1.2rem 0 0.6rem;
    padding-bottom: 0.3rem;
    border-bottom: 1px solid {COLORS['bdr']};
}}

/* Status badges */
.badge-ok {{
    background: rgba(63,185,80,0.18);
    color: {COLORS['grn']};
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 700;
}}
.badge-warn {{
    background: rgba(248,81,73,0.18);
    color: {COLORS['red']};
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 700;
}}

.last-updated {{ color: #ffffff; font-size: 12px; text-align: right; }}

/* Streamlit dataframe / metric tweaks for dark */
[data-testid="stMetricValue"] {{ font-size: 24px !important; color: #ffffff !important; }}
[data-testid="stMetricLabel"]  {{ color: #ffffff !important; font-size: 14px !important; }}
[data-testid="stMetricDelta"]  {{ color: #ffffff !important; }}
.stTabs [data-baseweb="tab-list"] {{ gap: 4px; }}
.stTabs [data-baseweb="tab"] {{
    background: {COLORS['sfc']};
    color: #ffffff !important;
    border-radius: 6px 6px 0 0;
    padding: 8px 16px;
    font-size: 15px !important;
}}
.stTabs [data-baseweb="tab"] p {{ color: #ffffff !important; font-size: 15px !important; }}
.stTabs [aria-selected="true"] {{
    background: {COLORS['sfc2']};
    color: {COLORS['acc']} !important;
}}
.stTabs [aria-selected="true"] p {{ color: {COLORS['acc']} !important; }}
.stPlotlyChart, .stDataFrame {{ background-color: {COLORS['sfc']}; border-radius: 10px; }}

/* ── Aggressive white-text overrides on Streamlit widgets ───────────────── */
.stRadio > label,
.stRadio label p,
.stRadio div p,
.stRadio div[role="radiogroup"] label,
.stCheckbox label, .stCheckbox label p,
.stSelectbox label, .stSelectbox label p,
.stSelectbox div[data-baseweb="select"] *,
.stMultiSelect label, .stMultiSelect label p,
.stTextInput label, .stNumberInput label,
.stDateInput label, .stSlider label,
.stTextArea label,
[data-testid="stWidgetLabel"], [data-testid="stWidgetLabel"] *,
[data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] *,
.stCaption, .stCaption *,
[data-baseweb="radio"] div, [data-baseweb="radio"] label,
.stMarkdown p, .stMarkdown span, .stMarkdown li,
.stDataFrame * {{
    color: #ffffff !important;
}}

/* ── Font-size bump ~10% on common widget text ──────────────────────────── */
.stRadio label p, .stRadio div[role="radiogroup"] label,
.stCheckbox label p,
.stSelectbox label p, .stMultiSelect label p,
.stTextInput label p, .stNumberInput label p,
[data-testid="stWidgetLabel"] p,
.stMarkdown p, .stCaption {{
    font-size: 15px !important;
}}
.section-head {{ font-size: 18px !important; }}
.kpi-label {{ font-size: 11px !important; }}
.kpi-value {{ font-size: 26px !important; }}
.kpi-sub   {{ font-size: 12px !important; }}

/* ── Selectbox / dropdown menu: dark text on its light popover ─────────── */
/* The selectbox open menu uses a light background regardless of our theme.
   Force readable dark text inside the open dropdown popover only. */
div[data-baseweb="popover"] *,
div[data-baseweb="popover"] li,
div[data-baseweb="popover"] [role="option"],
ul[role="listbox"] *,
ul[role="listbox"] li {{
    color: #0d1117 !important;
}}
/* Hover/highlight state on dropdown items */
div[data-baseweb="popover"] li:hover,
div[data-baseweb="popover"] [aria-selected="true"] {{
    color: #0d1117 !important;
}}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=0)
def load_kpi_data() -> dict[str, pd.DataFrame]:
    """Load pre-computed KPI tables from CSV files in cache/latest/."""
    if LATEST_DIR.exists() and any(LATEST_DIR.glob("*.csv")):
        return _read_csv_folder(str(LATEST_DIR))
    alt_dirs = [BASE_DIR / "cache" / "latest", BASE_DIR / "data", BASE_DIR]
    for alt in alt_dirs:
        if alt.exists() and any(alt.glob("kpi_gauges.csv")):
            return _read_csv_folder(str(alt))
    st.warning(f"No data found in {LATEST_DIR}")
    return {}


def _read_csv_folder(folder: str) -> dict[str, pd.DataFrame]:
    result = {}
    for csv_path in Path(folder).glob("*.csv"):
        try:
            result[csv_path.stem] = pd.read_csv(str(csv_path))
        except Exception:
            pass
    return result


# ── Helpers ────────────────────────────────────────────────────────────────────
def fmt_currency(val):
    """Format a number as $X.XXM / $X.XK / $X. Defensive against None, NaN, strings, pd.NA."""
    try:
        val = float(val) if val is not None else 0.0
        if val != val:   # catches NaN (NaN != NaN is True)
            val = 0.0
    except (TypeError, ValueError):
        val = 0.0
    if val == 0:
        return "—"
    if val >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:.0f}"


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


def style_plotly(fig, height=280):
    """Apply dark-theme styling to a plotly figure."""
    fig.update_layout(
        plot_bgcolor=COLORS["sfc"],
        paper_bgcolor=COLORS["sfc"],
        font=dict(color=COLORS["txt"], family="DM Sans", size=13),
        title_font_color=COLORS["txt"],
        title_font_size=15,
        margin=dict(l=10, r=10, t=40, b=10),
        height=height,
        xaxis=dict(gridcolor=COLORS["bdr2"], color=COLORS["txt"],
                   tickfont=dict(color=COLORS["txt"]),
                   title=dict(font=dict(color=COLORS["txt"]))),
        yaxis=dict(gridcolor=COLORS["bdr2"], color=COLORS["txt"],
                   tickfont=dict(color=COLORS["txt"]),
                   title=dict(font=dict(color=COLORS["txt"]))),
        legend=dict(bgcolor="rgba(0,0,0,0)",
                    font=dict(color=COLORS["txt"], size=13)),
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def render_header():
    col_title, col_refresh = st.columns([4, 1])
    with col_title:
        st.markdown("## 📊 Executive Dashboard")
    with col_refresh:
        kpi_file = LATEST_DIR / "kpi_gauges.csv"
        if kpi_file.exists():
            mtime = datetime.fromtimestamp(kpi_file.stat().st_mtime)
            st.markdown(
                f'<div class="last-updated">Last updated<br>'
                f'<strong>{mtime.strftime("%b %d, %Y %I:%M %p")}</strong></div>',
                unsafe_allow_html=True,
            )
        if st.button("🔄 Refresh", width='stretch'):
            st.cache_data.clear()
            st.rerun()


def render_kpi_row(gauges: pd.DataFrame):
    if gauges.empty:
        st.warning("No KPI data available.")
        return

    g = gauges.iloc[0]
    cols = st.columns(6)

    with cols[0]:
        kpi_card("YTD Revenue", fmt_currency(g.get("ytd_revenue", 0) or 0),
                 f"Prior year: {fmt_currency(g.get('ytd_prior_revenue', 0) or 0)}",
                 status="ok")

    growth = float(g.get("actual_growth_pct", 0) or 0)
    target = float(g.get("target_growth_pct", 7) or 7)
    with cols[1]:
        kpi_card("Revenue Growth", f"{growth:+.1f}%",
                 f"Target: +{target:.0f}%",
                 status="ok" if growth >= target else "warn")

    remake = float(g.get("remake_rate", 0) or 0)
    alert = float(g.get("remake_alert_pct", 5) or 5)
    with cols[2]:
        kpi_card("Remake Rate", f"{remake:.1f}%",
                 f"Alert threshold: {alert:.0f}%",
                 status="ok" if remake < alert else "warn")

    with cols[3]:
        kpi_card("Avg Margin", f"{float(g.get('avg_margin_pct', 0) or 0):.1f}%",
                 "Gross margin (placeholder)")

    wip_val = g.get("wip_value", 0) or 0
    wip_ov = int(g.get("wip_overdue", 0) or 0)
    with cols[4]:
        kpi_card("WIP Value", fmt_currency(wip_val),
                 f"{int(g.get('wip_count', 0) or 0)} cases · {wip_ov} overdue",
                 status="warn" if wip_ov > 0 else "ok")

    active_30 = int(g.get("active_accounts_30d", 0) or 0)
    remakes_30 = int(g.get("remakes_30d", 0) or 0)
    with cols[5]:
        kpi_card("Active Accounts", str(active_30),
                 f"last 30 days · {remakes_30} remakes")


def render_mtd(gauges: pd.DataFrame):
    section("📅 Month-to-Date & End of Month Projection")
    if gauges.empty:
        return
    g = gauges.iloc[0]
    mtd = float(g.get("mtd_revenue", 0) or 0)
    projected = float(g.get("mtd_projected_month", 0) or 0)
    days_elapsed = int(g.get("mtd_days_elapsed", 1) or 1)
    days_in_month = int(g.get("mtd_days_in_month", 30) or 30)
    days_remaining = max(days_in_month - days_elapsed, 0)
    ly_total = float(g.get("ytd_prior_revenue", 0) or 0)
    ly_monthly_avg = ly_total / 12 if ly_total else 0
    ly_target = ly_monthly_avg * 1.07
    on_pace = projected >= ly_target

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi_card("MTD Revenue", fmt_currency(mtd),
                 f"{days_elapsed} of {days_in_month} days", status="ok")
    with c2:
        kpi_card("Projected Month End", fmt_currency(projected),
                 "at current run rate",
                 status="ok" if on_pace else "warn")
    with c3:
        kpi_card("Last Year Monthly Avg", fmt_currency(ly_monthly_avg),
                 f"7% target: {fmt_currency(ly_target)}")
    with c4:
        daily_needed = (ly_target - mtd) / days_remaining if days_remaining > 0 else 0
        kpi_card("Daily Revenue Needed", fmt_currency(max(daily_needed, 0)),
                 f"to hit 7% · {days_remaining} days left",
                 status="ok" if daily_needed <= (mtd / max(days_elapsed,1)) else "warn")

    pct = min(projected / ly_target * 100, 150) if ly_target else 0
    bar_color = COLORS['grn'] if on_pace else COLORS['red']
    st.markdown(f"""
    <div style="background:{COLORS['sfc']};border:1px solid {COLORS['bdr']};
                border-radius:10px;padding:14px 18px;margin-top:8px">
      <div style="display:flex;justify-content:space-between;margin-bottom:6px">
        <span style="font-size:11px;color:{COLORS['txt2']};font-weight:600;text-transform:uppercase">Projected vs 7% Target</span>
        <span style="font-size:12px;font-weight:700;color:{bar_color}">{pct:.0f}%</span>
      </div>
      <div style="background:{COLORS['bdr2']};border-radius:5px;height:10px">
        <div style="background:{bar_color};width:{min(pct,100):.0f}%;
                    height:10px;border-radius:5px;transition:width .3s"></div>
      </div>
    </div>""", unsafe_allow_html=True)


def render_profitability(prof_df):
    section("💰 Account Profitability Rankings")
    if prof_df.empty:
        st.info("No profitability data.")
        return
    col1, col2 = st.columns([3, 2])
    with col1:
        top = prof_df.nlargest(15, "ltd_sales")
        fig = go.Figure(go.Bar(
            x=top["ltd_sales"], y=top["account_id"].astype(str),
            orientation="h",
            marker=dict(color=top["yoy_growth_pct"],
                        colorscale=[[0, COLORS['red']], [0.5, COLORS['sfc2']], [1, COLORS['acc']]],
                        showscale=True,
                        colorbar=dict(title="YoY %", thickness=12, tickfont=dict(color=COLORS['txt2']))),
            text=top.apply(lambda r: f"  ${r['ltd_sales']:,.0f}  ({r['yoy_growth_pct']:+.1f}%)", axis=1),
            textposition="outside", textfont=dict(color=COLORS['txt']),
        ))
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(style_plotly(fig, height=420), width='stretch')
    with col2:
        display = prof_df.nlargest(20, "ltd_sales").copy()
        cols_show = [c for c in ["account_id","ltd_sales","ytd_sales","ly_sales",
                                  "yoy_growth_pct","remake_rate_pct"] if c in display.columns]
        display = display[cols_show]
        rename = {"account_id":"Account", "ltd_sales":"LTD Sales", "ytd_sales":"YTD Sales",
                  "ly_sales":"Prior Year", "yoy_growth_pct":"YoY %", "remake_rate_pct":"Remake %"}
        display = display.rename(columns=rename)
        for c in ["LTD Sales","YTD Sales","Prior Year"]:
            if c in display.columns:
                display[c] = display[c].apply(lambda v: f"${v:,.0f}")
        st.dataframe(display, width='stretch', height=420, hide_index=True)


def render_pareto(pareto_df, prof_df):
    section("⭐ Top 20% Accounts — Pareto")
    if pareto_df.empty:
        st.info("No Pareto data.")
        return
    sorted_df = prof_df.sort_values("ltd_sales", ascending=False).copy()
    total = sorted_df["ltd_sales"].sum()
    sorted_df["cum_pct"] = sorted_df["ltd_sales"].cumsum() / total * 100
    top15 = sorted_df.head(15)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=top15["account_id"].astype(str), y=top15["ltd_sales"],
                         marker_color=COLORS['acc'], name="Revenue"), secondary_y=False)
    fig.add_trace(go.Scatter(x=top15["account_id"].astype(str), y=top15["cum_pct"],
                             mode="lines+markers", name="Cumulative %",
                             line=dict(color=COLORS['gold'], width=2)), secondary_y=True)
    fig.add_hline(y=80, line_dash="dash", line_color=COLORS['red'],
                  annotation_text="80% threshold", secondary_y=True)
    fig.update_yaxes(tickformat="$,.0f", secondary_y=False)
    fig.update_yaxes(ticksuffix="%", secondary_y=True)
    st.plotly_chart(style_plotly(fig, height=350), width='stretch')

    pareto_rev = pareto_df["ltd_sales"].sum()
    n = len(pareto_df)
    total_n = len(prof_df)
    st.caption(f"**{n} accounts** ({n/total_n*100:.0f}% of {total_n}) drive "
               f"**{fmt_currency(pareto_rev)}** ({pareto_rev/total*100:.0f}% of revenue)")


def render_wip(wip_summary, wip_detail):
    section("🔧 Work in Progress")
    if wip_summary.empty:
        st.info("No WIP data.")
        return
    c1, c2 = st.columns([1, 2])
    with c1:
        total_val = wip_summary["value"].sum() if "value" in wip_summary.columns else 0
        total_cnt = wip_summary["count"].sum() if "count" in wip_summary.columns else 0
        kpi_card("Total WIP Value", fmt_currency(total_val),
                 f"{int(total_cnt)} open cases", status="ok")
        st.dataframe(wip_summary, width='stretch', hide_index=True, height=200)
    with c2:
        if not wip_detail.empty:
            display = wip_detail.copy()
            if "total_charge" in display.columns:
                display["total_charge"] = display["total_charge"].apply(lambda v: f"${v:,.2f}")
            if "overdue" in display.columns:
                display["overdue"] = display["overdue"].apply(lambda v: "⚠️ Yes" if v else "✅ No")
            st.dataframe(display.head(50), width='stretch', height=400, hide_index=True)


def render_active(active_df):
    section("👥 Active Accounts (Last 30 Days)")
    if active_df.empty:
        st.info("No active account data — see pipeline note about Crystal Report column mapping.")
        return
    c1, c2 = st.columns([2, 1])
    with c1:
        top = active_df.nlargest(15, "revenue")
        fig = go.Figure(go.Bar(
            x=top["revenue"], y=top["account_id"].astype(str),
            orientation="h", marker_color=COLORS['acc'],
            text=top["revenue"].apply(lambda v: f"  ${v:,.0f}"),
            textposition="outside", textfont=dict(color=COLORS['txt']),
        ))
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(style_plotly(fig, height=400), width='stretch')
    with c2:
        display = active_df.copy()
        display["revenue"] = display["revenue"].apply(lambda v: f"${v:,.0f}")
        st.dataframe(display.head(20), width='stretch', height=400, hide_index=True)


def render_remakes(remakes_detail, reason_df, history_df=None,
                   dept_df=None, dept_reason_df=None, full_df=None):
    section("🔁 Remakes (Last 30 Days)")
    if remakes_detail.empty:
        st.info("No remakes in the last 30 days.")
    else:
        # ── Grouping toggle (Product Type vs Department) ────────────────────
        # Default to Product Type per Danny's preference. Toggle is sticky
        # in session state so it survives reruns.
        group_choice = "Product Type"
        group_col = "product_category"
        if full_df is not None and not full_df.empty:
            has_category = "product_category" in full_df.columns and \
                           full_df["product_category"].astype(str).str.strip().ne("").any()
            has_department = "product_department" in full_df.columns and \
                             full_df["product_department"].astype(str).str.strip().ne("").any()
            if has_category and has_department:
                group_choice = st.radio(
                    "Group remakes by:",
                    options=["Product Type", "Department"],
                    horizontal=True,
                    key="remake_group_toggle",
                )
                group_col = "product_category" if group_choice == "Product Type" else "product_department"
            elif has_department and not has_category:
                group_col = "product_department"
                group_choice = "Department"
            elif has_category and not has_department:
                group_col = "product_category"
                group_choice = "Product Type"

        # Compute group-level summary on the fly from full_df so toggle works.
        live_dept_df = pd.DataFrame()
        live_dept_reason_df = pd.DataFrame()
        if full_df is not None and not full_df.empty and group_col in full_df.columns:
            grp_rows = full_df.drop_duplicates(subset=["case_number", group_col]) \
                              if "case_number" in full_df.columns else full_df
            agg_kwargs = {"remake_cases": ("case_number", "nunique")} \
                         if "case_number" in grp_rows.columns else {"remake_cases": (group_col, "size")}
            if "total_charge" in grp_rows.columns:
                agg_kwargs["remake_dollars"] = ("total_charge", "sum")
            live_dept_df = (grp_rows.groupby(group_col)
                                    .agg(**agg_kwargs)
                                    .reset_index()
                                    .rename(columns={group_col: "product_department"})
                                    .sort_values("remake_cases", ascending=False))
            if "remake_reason" in grp_rows.columns:
                live_dept_reason_df = (grp_rows.groupby([group_col, "remake_reason"])
                                               .size().reset_index(name="count")
                                               .rename(columns={group_col: "product_department"})
                                               .sort_values(["product_department", "count"],
                                                            ascending=[True, False]))
        # Fall back to pre-computed (always department-based) if full_df missing
        if live_dept_df.empty and dept_df is not None and not dept_df.empty:
            live_dept_df = dept_df
        if live_dept_reason_df.empty and dept_reason_df is not None and not dept_reason_df.empty:
            live_dept_reason_df = dept_reason_df

        # Re-bind for the rest of the function (rest of code uses dept_df / dept_reason_df).
        dept_df = live_dept_df
        dept_reason_df = live_dept_reason_df
        group_label = group_choice  # "Product Type" or "Department" for chart titles

        # ── Top: by-group $ bar + overall pie ───────────────────────────────
        if dept_df is not None and not dept_df.empty:
            c1, c2 = st.columns([3, 2])
            with c1:
                dept_sorted = dept_df.sort_values("remake_dollars", ascending=True)
                fig = px.bar(
                    dept_sorted, x="remake_dollars", y="product_department",
                    orientation="h",
                    text=dept_sorted["remake_dollars"].apply(lambda v: f"${v:,.0f}"),
                    color="remake_dollars",
                    color_continuous_scale=[[0, COLORS['acc']], [1, COLORS['red']]],
                )
                fig.update_traces(textposition="outside", textfont=dict(color="white"))
                fig.update_xaxes(title_text="Remake $", tickprefix="$")
                fig.update_yaxes(title_text="")
                fig.update_layout(showlegend=False, coloraxis_showscale=False)
                st.markdown(f"**Remake $ by {group_label}**")
                st.plotly_chart(style_plotly(fig, height=300), width='stretch')
            with c2:
                if not reason_df.empty and "remake_reason" in reason_df.columns:
                    fig = px.pie(reason_df, names="remake_reason", values="count",
                                 color_discrete_sequence=[COLORS['acc'], COLORS['pur'],
                                                           COLORS['gold'], COLORS['red']])
                    fig.update_traces(textposition="inside", textinfo="percent+label",
                                      textfont=dict(color="white"))
                    fig.update_layout(showlegend=False)
                    st.markdown("**All Reasons (overall)**")
                    st.plotly_chart(style_plotly(fig, height=260), width='stretch')
                st.metric("Total Remakes", len(remakes_detail))
        else:
            # Old layout fallback if dept data isn't present
            c1, c2 = st.columns([1, 2])
            with c1:
                if not reason_df.empty and "remake_reason" in reason_df.columns:
                    fig = px.pie(reason_df, names="remake_reason", values="count",
                                 color_discrete_sequence=[COLORS['acc'], COLORS['pur'],
                                                           COLORS['gold'], COLORS['red']])
                    fig.update_traces(textposition="inside", textinfo="percent+label",
                                      textfont=dict(color="white"))
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(style_plotly(fig, height=260), width='stretch')
                st.metric("Total Remakes", len(remakes_detail))
            with c2:
                pass  # detail table rendered below regardless

        # ── Per-group reason pies (top 6 by case count) ─────────────────────
        if dept_reason_df is not None and not dept_reason_df.empty and dept_df is not None and not dept_df.empty:
            top_depts = dept_df.sort_values("remake_cases", ascending=False).head(6)
            top_dept_names = top_depts["product_department"].tolist()
            st.markdown(f"**Reason Breakdown by {group_label}**")
            cols_per_row = 3
            pie_palette = [COLORS['acc'], COLORS['pur'], COLORS['gold'],
                           COLORS['red'], "#5fa8d3", "#9eb574",
                           "#d1495b", "#e8a87c"]
            for row_start in range(0, len(top_dept_names), cols_per_row):
                cols = st.columns(cols_per_row)
                for i, dept_name in enumerate(top_dept_names[row_start:row_start+cols_per_row]):
                    sub = dept_reason_df[dept_reason_df["product_department"] == dept_name]
                    if sub.empty:
                        continue
                    case_ct = int(top_depts[top_depts["product_department"] == dept_name]["remake_cases"].iloc[0])
                    dollars = float(top_depts[top_depts["product_department"] == dept_name]["remake_dollars"].iloc[0])
                    with cols[i]:
                        fig = px.pie(sub, names="remake_reason", values="count",
                                     color_discrete_sequence=pie_palette)
                        fig.update_traces(textposition="inside",
                                          textinfo="percent+label",
                                          textfont=dict(color="white", size=10))
                        fig.update_layout(showlegend=False,
                                          title=dict(text=f"{dept_name}<br>"
                                                          f"<sub>{case_ct} cases · ${dollars:,.0f}</sub>",
                                                     font=dict(size=13)))
                        st.plotly_chart(style_plotly(fig, height=240), width='stretch')

        # ── Drill-down (follows the Product Type / Department toggle above) ─
        if full_df is not None and not full_df.empty and group_col in full_df.columns:
            st.divider()
            st.markdown(f"**🔍 Drill Into a {group_label}**")
            drill_options = ["(all)"] + sorted(
                [d for d in full_df[group_col].dropna().unique()
                 if str(d).strip()]
            )
            picked = st.selectbox(
                f"Pick a {group_label.lower()} to see its remakes — reasons + the specific lines that were remade:",
                options=drill_options,
                key=f"remake_drill_picker_{group_col}",
            )

            if picked != "(all)":
                drill = full_df[full_df[group_col] == picked].copy()
                if drill.empty:
                    st.info(f"No remakes recorded for {picked} in the last 30 days.")
                else:
                    case_count = drill["case_number"].nunique() if "case_number" in drill.columns else len(drill)
                    line_count = len(drill)
                    dollars = drill.drop_duplicates(subset=["case_number"])["total_charge"].sum() \
                              if "case_number" in drill.columns and "total_charge" in drill.columns \
                              else drill.get("total_charge", pd.Series([0])).sum()

                    m1, m2, m3 = st.columns(3)
                    m1.metric(f"{picked} — Cases", f"{case_count}")
                    m2.metric("Product Lines", f"{line_count}")
                    m3.metric("Total Remake $", f"${dollars:,.2f}")

                    # Secondary breakdown: show the "other" dimension within
                    # what was picked. If grouping by Product Type, show the
                    # departments inside that type. If grouping by Department,
                    # show the product types inside that department.
                    other_col = "product_department" if group_col == "product_category" else "product_category"
                    other_label = "Department" if other_col == "product_department" else "Product Type"

                    g1, g2 = st.columns([1, 1])
                    with g1:
                        if "remake_reason" in drill.columns:
                            reason_breakdown = (
                                drill.drop_duplicates(subset=["case_number"])
                                     .groupby("remake_reason").size()
                                     .reset_index(name="count")
                                     .sort_values("count", ascending=False)
                            )
                            fig = px.pie(reason_breakdown, names="remake_reason",
                                         values="count",
                                         color_discrete_sequence=[COLORS['acc'], COLORS['pur'],
                                                                   COLORS['gold'], COLORS['red'],
                                                                   "#5fa8d3", "#9eb574"])
                            fig.update_traces(textposition="inside",
                                              textinfo="percent+label",
                                              textfont=dict(color="white"))
                            fig.update_layout(showlegend=False,
                                              title=f"{picked} — Reasons")
                            st.plotly_chart(style_plotly(fig, height=320), width='stretch')
                    with g2:
                        if other_col in drill.columns:
                            prod_breakdown = (
                                drill.groupby(other_col).size()
                                     .reset_index(name="lines")
                                     .sort_values("lines", ascending=True)
                            )
                            fig = px.bar(prod_breakdown, x="lines", y=other_col,
                                         orientation="h",
                                         text="lines",
                                         color="lines",
                                         color_continuous_scale=[[0, COLORS['acc']],
                                                                 [1, COLORS['gold']]])
                            fig.update_traces(textposition="outside",
                                              textfont=dict(color="white"))
                            fig.update_xaxes(title_text="# Product Lines Remade")
                            fig.update_yaxes(title_text="")
                            fig.update_layout(showlegend=False,
                                              coloraxis_showscale=False,
                                              title=f"{picked} — by {other_label}")
                            st.plotly_chart(style_plotly(fig, height=320), width='stretch')

                    st.markdown(f"**{picked} — Product Line Detail**")
                    detail_cols = [c for c in ["case_number", "doctor_name", "patient_last",
                                                "date_in", "product_category", "product_department",
                                                "remake_reason", "total_charge", "status"]
                                   if c in drill.columns]
                    show = drill[detail_cols].copy()
                    if "total_charge" in show.columns:
                        show["total_charge"] = show["total_charge"].apply(lambda v: f"${v:,.2f}")
                    if "date_in" in show.columns:
                        show["date_in"] = pd.to_datetime(show["date_in"], errors="coerce").dt.strftime("%Y-%m-%d")
                    st.dataframe(show, width='stretch', height=320, hide_index=True)

        # ── Detail table (all departments) ──────────────────────────────────
        st.divider()
        st.markdown("**Case Detail (all departments)**")
        display = remakes_detail.copy()
        if "total_charge" in display.columns:
            display["total_charge"] = display["total_charge"].apply(lambda v: f"${v:,.2f}")
        st.dataframe(display, width='stretch', height=280, hide_index=True)

    # ── 13-month historical trend ────────────────────────────────────────────
    if history_df is not None and not history_df.empty:
        st.divider()
        section("📈 13-Month Remake Rate Trend")
        st.caption("Click any legend item to toggle that line on/off.")

        # Compute 3-month rolling average of remake rate
        hist = history_df.copy().sort_values("yearmonth")
        hist["remake_rate_3mo_avg"] = (
            hist["remake_rate_pct"]
            .rolling(window=3, min_periods=1)
            .mean()
            .round(2)
        )

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=hist["yearmonth"], y=hist["remake_rate_pct"],
                name="Remake Rate %", mode="lines+markers",
                line=dict(color=COLORS['red'], width=2),
                marker=dict(size=7),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=hist["yearmonth"], y=hist["remake_rate_3mo_avg"],
                name="3-Month Rolling Avg", mode="lines",
                line=dict(color=COLORS['gold'], width=2, dash="dash"),
            )
        )
        fig.update_xaxes(title_text="Month")
        fig.update_yaxes(title_text="Remake Rate %", ticksuffix="%")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(style_plotly(fig, height=420), width='stretch')


def render_daily_sales(daily_df):
    section("📅 Daily Sales — Cases In vs. Cases Out")
    if daily_df is None or daily_df.empty:
        st.info("No daily sales data yet. Re-run `py pipeline.py` to populate "
                "`cache/latest/daily_sales.csv`.")
        return

    df = daily_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date", ascending=False)
    df["yearmonth"] = df["date"].dt.to_period("M").astype(str)

    # Month picker — current month first, then previous
    months = list(df["yearmonth"].drop_duplicates())
    if not months:
        st.info("No daily data in cache.")
        return
    pick = st.selectbox("Month", months, index=0, key="daily_sales_month")
    month_df = df[df["yearmonth"] == pick].sort_values("date").reset_index(drop=True)

    # KPI strip: month totals
    in_cases  = int(month_df["cases_in"].sum())
    in_dol    = float(month_df["dollars_in"].sum())
    out_cases = int(month_df["cases_out"].sum())
    out_dol   = float(month_df["dollars_out"].sum())
    k = st.columns(4)
    with k[0]: kpi_card("Cases In",  f"{in_cases:,}",  pick)
    with k[1]: kpi_card("$ In",      fmt_currency(in_dol),  pick)
    with k[2]: kpi_card("Cases Out", f"{out_cases:,}", pick)
    with k[3]: kpi_card("$ Out",     fmt_currency(out_dol), pick)

    # Bar chart: cases in vs cases out per day
    fig = go.Figure()
    fig.add_bar(x=month_df["date"], y=month_df["cases_in"],
                name="Cases In",  marker_color=COLORS["acc"])
    fig.add_bar(x=month_df["date"], y=month_df["cases_out"],
                name="Cases Out", marker_color=COLORS["grn"])
    fig.update_layout(barmode="group", height=320,
                      margin=dict(l=10, r=10, t=10, b=10),
                      xaxis_title="", yaxis_title="Cases",
                      legend=dict(orientation="h", y=-0.2))
    style_plotly(fig, height=320)
    st.plotly_chart(fig, use_container_width=True)

    # Detail table (most recent first)
    show = month_df.sort_values("date", ascending=False).copy()
    show["Date"]       = show["date"].dt.strftime("%a %b %d")
    show["Cases In"]   = show["cases_in"].astype(int)
    show["$ In"]       = show["dollars_in"].apply(fmt_currency)
    show["Cases Out"]  = show["cases_out"].astype(int)
    show["$ Out"]      = show["dollars_out"].apply(fmt_currency)
    show["Net Cases"]  = show["cases_in"] - show["cases_out"]
    show["Net $"]      = (show["dollars_in"] - show["dollars_out"]).apply(fmt_currency)
    st.dataframe(
        show[["Date", "Cases In", "$ In", "Cases Out", "$ Out", "Net Cases", "Net $"]],
        use_container_width=True, hide_index=True, height=460,
    )


def render_product_mix(mix_df):
    section("🥧 Product Mix by Type")
    if mix_df is None or mix_df.empty:
        st.info("No product mix data yet. Re-run `py pipeline.py` to populate "
                "`cache/latest/product_type_summary.csv`.")
        return

    cols_present = [c for c in ("ytd", "ly", "lm") if c in mix_df.columns]
    labels = {"ytd": "Year-to-Date", "ly": "Last Year", "lm": "Last Month (~30d)"}
    cols = st.columns(len(cols_present))
    palette = px.colors.qualitative.Set2

    for i, period in enumerate(cols_present):
        with cols[i]:
            sub = mix_df[["product_type", period]].copy()
            sub = sub[sub[period].abs() > 0].sort_values(period, ascending=False)
            total = float(sub[period].sum())
            fig = go.Figure(go.Pie(
                labels=sub["product_type"], values=sub[period],
                hole=0.4, sort=False, direction="clockwise",
                marker=dict(colors=palette),
                textinfo="percent",
                hovertemplate="<b>%{label}</b><br>$%{value:,.0f}<br>%{percent}<extra></extra>",
            ))
            fig.update_layout(
                title=dict(text=f"{labels[period]}<br><sub>{fmt_currency(total)}</sub>",
                           x=0.5, xanchor="center", font=dict(size=14)),
                margin=dict(l=10, r=10, t=60, b=10),
                height=340,
                showlegend=False,
            )
            style_plotly(fig, height=340)
            st.plotly_chart(fig, use_container_width=True)

    # Shared legend table beneath the pies
    legend_cols = ["product_type"] + cols_present
    legend = mix_df[legend_cols].copy()
    for c in cols_present:
        legend[c] = legend[c].apply(fmt_currency)
    legend = legend.rename(columns={"product_type": "Product Type",
                                     "ytd": "YTD", "ly": "Last Year", "lm": "Last Month"})
    st.dataframe(legend, use_container_width=True, hide_index=True, height=260)


def render_implants(impl_df):
    section("🔬 Implant Pipeline")
    if impl_df.empty:
        st.info("No implant cases.")
        return
    display = impl_df.copy()
    cols_show = [c for c in ["account_id", "ytd_implant_sales", "ly_implant_sales",
                              "ytd_implant_remakes"] if c in display.columns]
    rename = {"account_id":"Account", "ytd_implant_sales":"YTD Implant $",
              "ly_implant_sales":"Prior Year $", "ytd_implant_remakes":"Remakes $"}
    display = display[cols_show].rename(columns=rename)
    for c in ["YTD Implant $","Prior Year $","Remakes $"]:
        if c in display.columns:
            display[c] = display[c].apply(lambda v: f"${v:,.0f}")
    st.dataframe(display.head(40), width='stretch', height=420, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

data = load_kpi_data()
gauges = data.get("kpi_gauges", pd.DataFrame())
prof = data.get("profitability", pd.DataFrame())
pareto = data.get("pareto_accounts", pd.DataFrame())
implants = data.get("implant_pipeline", pd.DataFrame())
wip_summary = data.get("wip_summary", pd.DataFrame())
wip_detail = data.get("wip_detail", pd.DataFrame())
active_30d = data.get("active_accounts_30d", pd.DataFrame())
remakes_detail = data.get("remakes_detail", pd.DataFrame())
remake_reason = data.get("remake_by_reason", pd.DataFrame())
remake_history = data.get("remake_history_monthly", pd.DataFrame())
remake_by_dept = data.get("remake_by_dept", pd.DataFrame())
remake_by_dept_reason = data.get("remake_by_dept_reason", pd.DataFrame())
remakes_full = data.get("remakes_full", pd.DataFrame())
daily_sales = data.get("daily_sales", pd.DataFrame())
product_mix = data.get("product_type_summary", pd.DataFrame())

render_header()
st.divider()
render_kpi_row(gauges)
st.divider()
render_mtd(gauges)
st.divider()

tabs = st.tabs([
    "📅 Daily Sales", "🥧 Product Mix",
    "💰 Profitability", "⭐ Pareto Top 20%", "🔧 WIP",
    "👥 Active Accounts", "🔁 Remakes", "🔬 Implants",
])
with tabs[0]: render_daily_sales(daily_sales)
with tabs[1]: render_product_mix(product_mix)
with tabs[2]: render_profitability(prof)
with tabs[3]: render_pareto(pareto, prof)
with tabs[4]: render_wip(wip_summary, wip_detail)
with tabs[5]: render_active(active_30d)
with tabs[6]: render_remakes(remakes_detail, remake_reason, remake_history,
                              remake_by_dept, remake_by_dept_reason, remakes_full)
with tabs[7]: render_implants(implants)

st.divider()
st.caption("Artistic Dental Studio · Executive Dashboard · Data refreshed nightly at 6 AM")
