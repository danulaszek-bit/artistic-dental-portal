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

@st.cache_data(ttl=120)
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
        ot_pct = float(g.get("on_time_pct", 0) or 0)
        ot_win = int(g.get("on_time_window_days", 90) or 90)
        ot_n   = int(g.get("on_time_cases", 0) or 0)
        kpi_card("On-Time Ship", f"{ot_pct:.1f}%",
                 f"last {ot_win}d · {ot_n:,} cases",
                 status="ok" if ot_pct >= 90 else "warn")

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
    ly_same_month = float(g.get("ly_same_month", 0) or 0)
    ly_full_year  = float(g.get("ly_full_year", 0) or 0)

    # If pipeline didn't populate ly_same_month, read it directly from the historical CSV.
    if not ly_same_month:
        try:
            import re as _re
            from datetime import date as _date
            _hist = BASE_DIR / "historical" / "Sales_2025.csv"
            if _hist.exists():
                _df = pd.read_csv(_hist, header=None, dtype=str, keep_default_na=False,
                                  on_bad_lines="skip", engine="python")
                _month = _date.today().month
                _total = 0.0
                for _, _row in _df.iterrows():
                    _dt = pd.to_datetime(str(_row.iloc[24]).strip(), errors="coerce")
                    if pd.notna(_dt) and _dt.month == _month:
                        _s = _re.sub(r"[^\d.]", "", str(_row.iloc[37]).strip())
                        try:
                            _total += float(_s)
                        except ValueError:
                            pass
                ly_same_month = _total
        except Exception:
            pass

    # Monthly target = same calendar month last year × 1.07
    # Fall back to full-year monthly avg if same-month data isn't available yet
    ly_month_base = ly_same_month if ly_same_month else (ly_full_year / 12 if ly_full_year else 0)
    ly_target     = ly_month_base * 1.07
    on_pace       = projected >= ly_target

    mtd_daily_avg = mtd / days_elapsed if days_elapsed else 0
    daily_target  = ly_target / days_in_month if days_in_month else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi_card("MTD Revenue", fmt_currency(mtd),
                 f"{days_elapsed} of {days_in_month} biz days", status="ok")
    with c2:
        kpi_card("Projected Month End", fmt_currency(projected),
                 "at current biz-day run rate",
                 status="ok" if on_pace else "warn")
    with c3:
        kpi_card("MTD Avg Daily Sales", fmt_currency(mtd_daily_avg),
                 f"Target: {fmt_currency(daily_target)}/day (7% growth)",
                 status="ok" if mtd_daily_avg >= daily_target else "warn")
    with c4:
        daily_needed = (ly_target - mtd) / days_remaining if days_remaining > 0 else 0
        kpi_card("Daily Revenue Needed", fmt_currency(max(daily_needed, 0)),
                 f"to hit 7% · {days_remaining} biz days left",
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

    # Hide accounts with $0 YTD so we only see currently active clients.
    total_before = len(prof_df)
    prof_df = prof_df[prof_df["ytd_sales"].fillna(0) > 0].copy()
    dropped = total_before - len(prof_df)
    if dropped > 0:
        st.caption(f"Hiding {dropped} inactive account(s) with no YTD sales "
                   f"to focus on currently active clients.")
    if prof_df.empty:
        st.info("No active clients with YTD sales.")
        return

    # ── LYTD = last-year revenue pro-rated to today's calendar position.
    #    (Same-period comparison; rough since seasonality isn't modeled, but
    #    fair across all accounts.)
    today = pd.Timestamp.today()
    day_of_year = today.timetuple().tm_yday
    days_in_year = 366 if today.is_leap_year else 365
    elapsed_frac = day_of_year / days_in_year
    prof_df["lytd_sales"] = prof_df["ly_sales"].fillna(0) * elapsed_frac

    # Growth vs LYTD (NaN when LYTD is 0 = brand-new account this year)
    def _growth(ytd, lytd):
        try:
            ytd = float(ytd or 0); lytd = float(lytd or 0)
            if lytd <= 0:
                return float("nan")
            return (ytd - lytd) / lytd * 100
        except Exception:
            return float("nan")
    prof_df["growth_vs_lytd_pct"] = [
        _growth(y, l) for y, l in zip(prof_df["ytd_sales"], prof_df["lytd_sales"])
    ]

    col1, col2 = st.columns([3, 2])
    with col1:
        # Rank by LAST YEAR's full-year revenue (who was big recently), then
        # show LYTD and YTD as paired bars so growth/decline is visually obvious.
        top = prof_df.nlargest(15, "ly_sales").iloc[::-1]   # reverse for top-down chart
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=top["lytd_sales"], y=top["account_id"].astype(str),
            orientation="h", name="LYTD (same period last yr)",
            marker_color=COLORS['ylw'],
            text=top["lytd_sales"].apply(lambda v: f" ${v:,.0f}"),
            textposition="outside", textfont=dict(color=COLORS['txt'], size=10),
        ))
        fig.add_trace(go.Bar(
            x=top["ytd_sales"], y=top["account_id"].astype(str),
            orientation="h", name="YTD (this year so far)",
            marker_color=COLORS['acc'],
            text=top["ytd_sales"].apply(lambda v: f" ${v:,.0f}"),
            textposition="outside", textfont=dict(color=COLORS['txt'], size=10),
        ))
        fig.update_layout(barmode='group',
                          legend=dict(orientation='h', y=-0.10,
                                      x=0.5, xanchor='center'))
        st.markdown("**Top 15 by Last Year sales — YTD vs LYTD**")
        st.plotly_chart(style_plotly(fig, height=520), width='stretch')
    with col2:
        display = prof_df.nlargest(20, "ly_sales").copy()
        cols_keep = [c for c in ["account_id", "ly_sales", "lytd_sales", "ytd_sales",
                                  "growth_vs_lytd_pct", "remake_rate_pct"]
                     if c in display.columns]
        display = display[cols_keep].rename(columns={
            "account_id": "Account",
            "ly_sales": "Last Year",
            "lytd_sales": "LYTD",
            "ytd_sales": "YTD",
            "growth_vs_lytd_pct": "YTD vs LYTD",
            "remake_rate_pct": "Remake %",
        })
        for c in ["Last Year", "LYTD", "YTD"]:
            if c in display.columns:
                display[c] = display[c].apply(lambda v: f"${v:,.0f}")
        if "YTD vs LYTD" in display.columns:
            display["YTD vs LYTD"] = display["YTD vs LYTD"].apply(
                lambda v: "—" if pd.isna(v) else f"{v:+.1f}%"
            )
        if "Remake %" in display.columns:
            display["Remake %"] = display["Remake %"].apply(
                lambda v: f"{v:.1f}%" if pd.notna(v) else "—"
            )
        st.dataframe(display, width='stretch', height=520, hide_index=True)


def render_pareto(pareto_df, prof_df):
    section("⭐ Top 20% Accounts — Pareto")
    if prof_df is None or prof_df.empty:
        st.info("No profitability data.")
        return

    # Same focus filter as Profitability: drop accounts with no YTD sales so
    # the Pareto reflects who's actually driving revenue now -- not historical
    # dead clients. Pre-computed pareto_df is from the unfiltered set, so we
    # re-derive the 80% group below from the filtered prof_df.
    total_before = len(prof_df)
    prof_df = prof_df[prof_df["ytd_sales"].fillna(0) > 0].copy()
    dropped = total_before - len(prof_df)
    if dropped > 0:
        st.caption(f"Hiding {dropped} inactive account(s) with no YTD sales "
                   f"to focus on currently active clients.")
    if prof_df.empty:
        st.info("No active clients with YTD sales.")
        return

    # Rank by LAST YEAR's sales -- the active version of the Pareto: who drove
    # most of last year's revenue, not most of all-time revenue.
    sorted_df = prof_df.sort_values("ly_sales", ascending=False).copy()
    total = sorted_df["ly_sales"].sum()
    sorted_df["cum_pct"] = sorted_df["ly_sales"].cumsum() / total * 100
    top15 = sorted_df.head(15)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=top15["account_id"].astype(str), y=top15["ly_sales"],
                         marker_color=COLORS['acc'], name="Last Year Revenue"), secondary_y=False)
    fig.add_trace(go.Scatter(x=top15["account_id"].astype(str), y=top15["cum_pct"],
                             mode="lines+markers", name="Cumulative %",
                             line=dict(color=COLORS['gold'], width=2)), secondary_y=True)
    fig.add_hline(y=80, line_dash="dash", line_color=COLORS['red'],
                  annotation_text="80% threshold", secondary_y=True)
    fig.update_yaxes(tickformat="$,.0f", secondary_y=False)
    fig.update_yaxes(ticksuffix="%", secondary_y=True)
    st.plotly_chart(style_plotly(fig, height=350), width='stretch')

    # Re-derive the Pareto 80% group from the FILTERED set (smallest set whose
    # cumulative revenue crosses 80%).
    n_under = int((sorted_df["cum_pct"] <= 80).sum())
    n_keep = min(n_under + 1, len(sorted_df))   # include the boundary-crosser
    pareto_filtered = sorted_df.head(n_keep)
    pareto_rev = pareto_filtered["ly_sales"].sum()
    n = len(pareto_filtered)
    total_n = len(prof_df)
    st.caption(f"**{n} accounts** ({n/total_n*100:.0f}% of {total_n} active accounts) drove "
               f"**{fmt_currency(pareto_rev)}** ({pareto_rev/total*100:.0f}% of last year's revenue)")


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


def _lab_holidays(year: int) -> set:
    """Lab closures — keep in sync with pipeline.py get_lab_holidays()."""
    out = set()
    for month, day in [(1, 1), (7, 4), (12, 25)]:
        d = pd.Timestamp(year=year, month=month, day=day)
        if d.weekday() == 5: d -= pd.Timedelta(days=1)
        elif d.weekday() == 6: d += pd.Timedelta(days=1)
        out.add(d.normalize())
    d = pd.Timestamp(year=year, month=5, day=31)
    while d.weekday() != 0: d -= pd.Timedelta(days=1)
    out.add(d.normalize())
    d = pd.Timestamp(year=year, month=9, day=1)
    while d.weekday() != 0: d += pd.Timedelta(days=1)
    out.add(d.normalize())
    d = pd.Timestamp(year=year, month=11, day=1)
    while d.weekday() != 3: d += pd.Timedelta(days=1)
    thx = d + pd.Timedelta(days=21)
    out.add(thx.normalize()); out.add((thx + pd.Timedelta(days=1)).normalize())
    return out


def _biz_days_elapsed_in_month(yearmonth: str) -> int:
    """Business days elapsed in the given YYYY-MM (full month if it's past,
    today-to-month-start if it's current, 0 if future)."""
    period = pd.Period(yearmonth, freq="M")
    start = period.start_time.normalize()
    today = pd.Timestamp.today().normalize()
    end = min(period.end_time.normalize(), today)
    if end < start:
        return 0
    holidays = _lab_holidays(start.year) | _lab_holidays(end.year)
    weekdays = pd.bdate_range(start, end)
    return sum(1 for d in weekdays if d.normalize() not in holidays)


def _biz_days_total_in_month(yearmonth: str) -> int:
    """Total business days in the given YYYY-MM (full month, excluding lab holidays)."""
    period = pd.Period(yearmonth, freq="M")
    start = period.start_time.normalize()
    end = period.end_time.normalize()
    holidays = _lab_holidays(start.year) | _lab_holidays(end.year)
    weekdays = pd.bdate_range(start, end)
    return sum(1 for d in weekdays if d.normalize() not in holidays)


def _project_month_end_invoiced(month_df: pd.DataFrame, yearmonth: str) -> dict:
    """Project month-end $ Invoiced using two methods:

      - run_rate: invoiced-so-far + (avg invoiced per biz day) * biz days remaining
      - trend:    linear least-squares fit on daily invoiced, extended over the
                  remaining biz days (captures momentum: up or down)

    Returns both numbers plus business-day counts so the caller can show them
    side by side. If the month is finished (no days remaining) both equal the
    actual invoiced total.
    """
    elapsed = _biz_days_elapsed_in_month(yearmonth)
    total = _biz_days_total_in_month(yearmonth)
    remaining = max(total - elapsed, 0)
    invoiced_so_far = float(month_df["dollars_invoiced"].sum())

    # Simple run-rate (daily avg holds for the rest of the month)
    avg_per_biz = invoiced_so_far / max(elapsed, 1)
    run_rate_proj = invoiced_so_far + remaining * avg_per_biz

    # Linear-trend extrapolation across days that actually invoiced
    days = (month_df[month_df["dollars_invoiced"] > 0]
            .sort_values("date").reset_index(drop=True))
    if len(days) >= 2 and remaining > 0:
        x = days.index.values.astype(float)
        y = days["dollars_invoiced"].values.astype(float)
        x_mean, y_mean = x.mean(), y.mean()
        denom = ((x - x_mean) ** 2).sum()
        slope = ((x - x_mean) * (y - y_mean)).sum() / denom if denom else 0.0
        intercept = y_mean - slope * x_mean
        predicted_remaining = sum(
            max(0.0, slope * (len(x) + i) + intercept)
            for i in range(remaining)
        )
        trend_proj = invoiced_so_far + predicted_remaining
    else:
        trend_proj = run_rate_proj   # not enough data to fit a trend

    return {
        "run_rate":           run_rate_proj,
        "trend":              trend_proj,
        "biz_days_total":     total,
        "biz_days_elapsed":   elapsed,
        "biz_days_remaining": remaining,
    }


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

    # Coerce columns we expect (older cache files may not have all of them)
    for c in ["cases_in", "dollars_in", "cases_out", "units_out",
              "dollars_invoiced", "dollars_net"]:
        if c not in df.columns:
            df[c] = 0
    df["cases_in"]   = pd.to_numeric(df["cases_in"],   errors="coerce").fillna(0).astype(int)
    df["cases_out"]  = pd.to_numeric(df["cases_out"],  errors="coerce").fillna(0).astype(int)
    df["units_out"]  = pd.to_numeric(df["units_out"],  errors="coerce").fillna(0).astype(int)
    for c in ["dollars_in", "dollars_invoiced", "dollars_net"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # Month picker
    months = list(df["yearmonth"].drop_duplicates())
    if not months:
        st.info("No daily data in cache.")
        return
    pick = st.selectbox("Month", months, index=0, key="daily_sales_month")
    month_df = df[df["yearmonth"] == pick].sort_values("date").reset_index(drop=True)

    in_cases   = int(month_df["cases_in"].sum())
    in_dol     = float(month_df["dollars_in"].sum())
    out_cases  = int(month_df["cases_out"].sum())
    out_units  = int(month_df["units_out"].sum())
    out_gross  = float(month_df["dollars_invoiced"].sum())
    out_net    = float(month_df["dollars_net"].sum())

    # Business-day divisor (weekends + lab holidays excluded)
    biz_days = _biz_days_elapsed_in_month(pick)
    div = max(biz_days, 1)

    # KPI strip — six tiles, each with per-business-day average in the subtitle
    k = st.columns(6)
    with k[0]: kpi_card("Cases In",  f"{in_cases:,}",
                        f"avg {in_cases/div:,.1f} / biz day  ·  {biz_days} days")
    with k[1]: kpi_card("$ In",      fmt_currency(in_dol),
                        f"avg {fmt_currency(in_dol/div)} / biz day")
    with k[2]: kpi_card("Cases Out", f"{out_cases:,}",
                        f"avg {out_cases/div:,.1f} / biz day")
    with k[3]: kpi_card("Units Out", f"{out_units:,}",
                        f"avg {out_units/div:,.1f} / biz day")
    with k[4]: kpi_card("$ Invoiced",fmt_currency(out_gross),
                        f"avg {fmt_currency(out_gross/div)} / biz day")
    with k[5]:
        proj = _project_month_end_invoiced(month_df, pick)
        kpi_card(
            "Projected Month End",
            fmt_currency(proj["run_rate"]),
            f"trend: {fmt_currency(proj['trend'])}  ·  "
            f"{proj['biz_days_remaining']} biz days left of {proj['biz_days_total']}",
        )

    # Daily chart: cases in vs cases out per day
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

    # Detail table
    show = month_df.sort_values("date", ascending=False).copy()
    show["Date"]         = show["date"].dt.strftime("%a %b %d")
    show["Cases In"]     = show["cases_in"]
    show["$ In"]         = show["dollars_in"].apply(fmt_currency)
    show["Cases Out"]    = show["cases_out"]
    show["Units Out"]    = show["units_out"]
    show["$ Invoiced"]   = show["dollars_invoiced"].apply(fmt_currency)
    show["$ Net Sales"]  = show["dollars_net"].apply(fmt_currency)
    st.dataframe(
        show[["Date", "Cases In", "$ In", "Cases Out", "Units Out",
              "$ Invoiced", "$ Net Sales"]],
        use_container_width=True, hide_index=True, height=460,
    )
    st.caption(
        "Out side sourced from Magic Touch's *Sales Summary By Date* report — "
        "Net Sales reconciles to the monthly sales tile. Cases In sourced from "
        "Active_30_day.csv (Cases_DateIn)."
    )


def render_product_mix(mix_df):
    section("🥧 Product Mix by Type")
    if mix_df is None or mix_df.empty:
        st.info("No product mix data yet. Re-run `py pipeline.py` to populate "
                "`cache/latest/product_type_summary.csv`.")
        return

    cols_present = [c for c in ("ytd", "lytd", "lm") if c in mix_df.columns]
    labels = {"ytd": "Year-to-Date", "lytd": "Last YTD", "lm": "Last Month (~30d)"}

    # ── Stable product → color mapping so each product keeps the SAME color
    #    across all pies AND the legend table, regardless of its rank in
    #    any individual period. Sorted alphabetically for determinism.
    products_sorted = sorted(
        str(p) for p in mix_df["product_type"].dropna().unique()
    )
    palette = (px.colors.qualitative.Set2 +
               px.colors.qualitative.Pastel +
               px.colors.qualitative.Set3)
    color_map = {prod: palette[i % len(palette)]
                 for i, prod in enumerate(products_sorted)}

    cols = st.columns(len(cols_present))

    for i, period in enumerate(cols_present):
        with cols[i]:
            sub = mix_df[["product_type", period]].copy()
            sub = sub[sub[period].abs() > 0].sort_values(period, ascending=False)
            total = float(sub[period].sum())
            # Apply the stable colors in the order of THIS pie's labels
            pie_colors = [color_map.get(str(p), "#888888")
                          for p in sub["product_type"]]
            fig = go.Figure(go.Pie(
                labels=sub["product_type"], values=sub[period],
                hole=0.4, sort=False, direction="clockwise",
                marker=dict(colors=pie_colors,
                            line=dict(color=COLORS['bg'], width=1)),
                textinfo="label",                  # product names, not %
                textposition="inside",             # keep labels off the title
                insidetextorientation="horizontal",
                textfont=dict(color="white", size=11),
                hovertemplate="<b>%{label}</b><br>$%{value:,.0f}<br>%{percent}<extra></extra>",
            ))
            fig.update_layout(
                title=dict(text=f"{labels[period]}<br><sub>{fmt_currency(total)}</sub>",
                           x=0.5, xanchor="center", font=dict(size=14)),
                margin=dict(l=10, r=10, t=80, b=10),   # bigger top margin clears the title
                height=340,
                showlegend=False,
            )
            style_plotly(fig, height=340)
            st.plotly_chart(fig, use_container_width=True)

    # ── Color-coded legend table (rows tinted to match pie colors) ──────────
    legend_cols = ["product_type"] + cols_present
    legend = mix_df[legend_cols].copy()
    for c in cols_present:
        legend[c] = legend[c].apply(fmt_currency)
    legend = legend.rename(columns={"product_type": "Product Type",
                                     "ytd": "YTD", "lytd": "Last YTD", "lm": "Last Month"})

    def _hex_to_rgba(hex_color, alpha=0.35):
        """Convert '#rrggbb' or 'rgb(r,g,b)' to rgba(r,g,b,a)."""
        h = str(hex_color).strip()
        if h.startswith("#"):
            h = h[1:]
            if len(h) == 3:
                h = "".join(c * 2 for c in h)
            r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        elif h.startswith("rgb"):
            inside = h[h.index("(") + 1: h.index(")")]
            r, g, b = (int(x.strip()) for x in inside.split(",")[:3])
        else:
            r = g = b = 128
        return f"rgba({r},{g},{b},{alpha})"

    def _row_style(row):
        prod = str(row["Product Type"])
        color = color_map.get(prod, "#888888")
        return [f"background-color: {_hex_to_rgba(color, 0.35)}"] * len(row)

    styled = legend.style.apply(_row_style, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True, height=260)


def render_monthly_sales_trend(daily_df):
    """13-month grouped bar chart: calendar months on x-axis, one series per year.
    Uses monthly_sales_history.csv which combines current + historical Sales Summary files.
    Falls back to daily_df if history file not available.
    """
    import numpy as np

    section("\u2014 Daily Sales Average by Month")

    # ── Load monthly_sales_history.csv ───────────────────────────────────────
    hist_path = BASE_DIR / "cache" / "latest" / "monthly_sales_history.csv"
    if hist_path.exists():
        try:
            hist = pd.read_csv(hist_path)
            hist["year"]  = hist["year"].astype(int)
            hist["month"] = hist["month"].astype(int)
            hist["daily_avg"] = pd.to_numeric(hist["daily_avg"], errors="coerce").fillna(0)
        except Exception:
            hist = pd.DataFrame()
    else:
        hist = pd.DataFrame()

    if hist.empty:
        st.info("No sales history data yet — run the pipeline to generate it.")
        return

    today       = pd.Timestamp.today().normalize()
    cur_year    = today.year
    cur_month   = today.month
    years       = sorted(hist["year"].unique())

    # ── Projection for current month ─────────────────────────────────────────
    def _total_biz(year, month):
        import datetime
        s = datetime.date(int(year), int(month), 1)
        nm = (int(month) % 12) + 1
        ny = int(year) + (1 if int(month) == 12 else 0)
        return max(int(np.busday_count(s, datetime.date(ny, nm, 1))), 1)

    def _elapsed_biz(year, month):
        import datetime
        s = datetime.date(int(year), int(month), 1)
        e = min(today.date(), datetime.date(
            int(year) + (1 if int(month) == 12 else 0),
            (int(month) % 12) + 1, 1))
        return max(int(np.busday_count(s, e)), 1)

    proj_daily = None
    proj_total = None
    proj_note  = ""
    ly_proj    = None

    cur_row = hist[(hist["year"] == cur_year) & (hist["month"] == cur_month)]
    if not cur_row.empty:
        cur       = cur_row.iloc[0]
        actual_so = float(cur["total_net"]) if "total_net" in cur else float(cur["daily_avg"]) * _elapsed_biz(cur_year, cur_month)
        elapsed   = _elapsed_biz(cur_year, cur_month)
        total_biz = _total_biz(cur_year, cur_month)
        remaining = max(total_biz - elapsed, 0)

        run_daily = actual_so / elapsed
        run_proj  = actual_so + run_daily * remaining

        # LY same month
        ly_row = hist[(hist["year"] == cur_year - 1) & (hist["month"] == cur_month)]
        if not ly_row.empty:
            ly_biz   = _total_biz(cur_year - 1, cur_month)
            ly_total = float(ly_row.iloc[0]["total_net"]) if "total_net" in ly_row.iloc[0] else float(ly_row.iloc[0]["daily_avg"]) * ly_biz
            ly_proj  = ly_total / ly_biz * total_biz
            ly_daily = ly_total / ly_biz
        else:
            ly_daily = None

        # Trend: last 6-9 complete months same year + prior year
        complete = hist[~((hist["year"] == cur_year) & (hist["month"] == cur_month))].copy()
        complete = complete.sort_values(["year", "month"]).tail(9)
        if len(complete) >= 3:
            xs = np.arange(len(complete), dtype=float)
            ys = complete["daily_avg"].values.astype(float)
            slope, intercept = np.polyfit(xs, ys, 1)
            trend_daily = slope * len(complete) + intercept
            trend_proj  = trend_daily * total_biz
        else:
            trend_daily = run_daily
            trend_proj  = run_proj

        if ly_proj is not None:
            blended_total = run_proj * 0.50 + ly_proj * 0.25 + trend_proj * 0.25
            proj_note = "50% run-rate \u00b7 25% LY \u00b7 25% trend"
        else:
            blended_total = run_proj * 0.65 + trend_proj * 0.35
            proj_note = "65% run-rate \u00b7 35% trend"

        proj_daily = blended_total / total_biz
        proj_total = blended_total

    # ── KPI cards ────────────────────────────────────────────────────────────
    if proj_total is not None and not cur_row.empty:
        elapsed_n = _elapsed_biz(cur_year, cur_month)
        total_n   = _total_biz(cur_year, cur_month)
        actual_so_disp = float(cur_row.iloc[0]["total_net"]) if "total_net" in cur_row.iloc[0] else proj_daily * elapsed_n
        c1, c2, c3 = st.columns(3)
        with c1:
            kpi_card("MTD Net Sales", f"${actual_so_disp:,.0f}",
                     f"{elapsed_n} of {total_n} biz days ({elapsed_n/total_n*100:.0f}%)")
        with c2:
            kpi_card("Projected Month Total", f"${proj_total:,.0f}", proj_note)
        with c3:
            if ly_proj is not None:
                delta = proj_total - ly_proj
                sign  = "+" if delta >= 0 else ""
                kpi_card("vs Same Month LY", f"{sign}${delta:,.0f}",
                         f"LY same month: ${ly_proj:,.0f}")
            else:
                kpi_card("Projected Daily Avg", f"${proj_daily:,.0f}", "")

    # ── Build grouped bar chart ───────────────────────────────────────────────
    MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    YEAR_COLORS = [COLORS["acc"], COLORS["teal"], COLORS["grn"], COLORS["pur"],
                   COLORS["org"], COLORS["ylw"]]

    fig = go.Figure()

    for i, year in enumerate(years):
        ydata = hist[hist["year"] == year].set_index("month")["daily_avg"]
        y_vals = []
        labels = []
        for m in range(1, 13):
            # Skip future months of current year
            if year == cur_year and m > cur_month:
                y_vals.append(None)
            else:
                y_vals.append(float(ydata.get(m, 0)) if m in ydata.index else None)
            labels.append(MONTHS[m - 1])

        color = YEAR_COLORS[i % len(YEAR_COLORS)]

        # Current year current month: split actual vs projected
        if year == cur_year and proj_daily is not None:
            cur_m_idx = cur_month - 1
            actual_avg = float(ydata.get(cur_month, 0)) if cur_month in ydata.index else 0
            y_vals[cur_m_idx] = actual_avg   # actual so far
            fig.add_trace(go.Bar(
                x=MONTHS, y=y_vals,
                name=str(year),
                marker_color=color,
                hovertemplate="<b>" + str(year) + " %{x}</b><br>Daily Avg: $%{y:,.0f}<extra></extra>",
            ))
            # Projected delta stacked on top
            proj_delta = [None] * 12
            if proj_daily > actual_avg:
                proj_delta[cur_m_idx] = proj_daily - actual_avg
            fig.add_trace(go.Bar(
                x=MONTHS, y=proj_delta,
                name=f"{year} proj",
                marker_color=color.replace(")", ", 0.30)").replace("rgb", "rgba") if "rgb" in color else color + "4d",
                marker_line=dict(color=color, width=1),
                showlegend=False,
                hovertemplate="<b>Projected add\'l</b><br>+$%{y:,.0f}/day<extra></extra>",
            ))
            # Dashed line at projected level
            if proj_daily > 0:
                fig.add_shape(type="line",
                    x0=MONTHS[cur_m_idx], x1=MONTHS[cur_m_idx],
                    y0=actual_avg, y1=proj_daily,
                    xref="x", yref="y",
                    line=dict(color=color, width=2, dash="dot"),
                )
                fig.add_annotation(
                    x=MONTHS[cur_m_idx], y=proj_daily,
                    text=f"  proj: ${proj_daily:,.0f}",
                    showarrow=False, yanchor="bottom", xanchor="left",
                    font=dict(color=color, size=10),
                )
        else:
            fig.add_trace(go.Bar(
                x=MONTHS, y=y_vals,
                name=str(year),
                marker_color=color,
                hovertemplate="<b>" + str(year) + " %{x}</b><br>Daily Avg: $%{y:,.0f}<extra></extra>",
            ))

    fig.update_layout(barmode="group")
    style_plotly(fig, height=460)
    fig.update_layout(
        yaxis=dict(tickprefix="$", tickformat=",.0f"),
        xaxis_title="",
        yaxis_title="Avg Daily Net Sales",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=40, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)
    if proj_note:
        st.caption(f"Daily avg = monthly net sales \u00f7 business days. Projection: {proj_note}.")


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


# ============================================================================
#  MAIN
# ============================================================================

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
    "\U0001f4c5 Daily Sales", "\U0001f4ca Sales Trend", "\U0001f967 Product Mix",
    "\U0001f4b0 Profitability", "\u2b50 Pareto Top 20%", "\U0001f527 WIP",
    "\U0001f465 Active Accounts", "\U0001f501 Remakes",
])
with tabs[0]: render_daily_sales(daily_sales)
with tabs[1]: render_monthly_sales_trend(daily_sales)
with tabs[2]: render_product_mix(product_mix)
with tabs[3]: render_profitability(prof)
with tabs[4]: render_pareto(pareto, prof)
with tabs[5]: render_wip(wip_summary, wip_detail)
with tabs[6]: render_active(active_30d)
with tabs[7]: render_remakes(remakes_detail, remake_reason, remake_history,
                              remake_by_dept, remake_by_dept_reason, remakes_full)

st.divider()
st.caption("Artistic Dental Studio - Executive Dashboard - Data refreshed nightly at 6 AM")
