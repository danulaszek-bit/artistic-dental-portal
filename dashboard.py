"""
Artistic Dental Studio — Partner Portal (Landing Page)
=======================================================
Run locally  : streamlit run dashboard.py
Deploy       : Streamlit Cloud (entry point = dashboard.py)

This is the landing page of a multi-page Streamlit app:
    dashboard.py                              ← you are here
    pages/1_Executive_Dashboard.py        ← financial KPIs
    pages/2_Doctor_Retention.py           ← retention tracking
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st

# ── Setup ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LATEST_DIR = BASE_DIR / "cache" / "latest"

st.set_page_config(
    page_title="Artistic Dental Studio — Partner Portal",
    page_icon="🦷",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Dark Palette (matches Lexie's v3) ──────────────────────────────────────────
COLORS = {
    "bg":     "#0d1117",
    "sfc":    "#161b22",
    "sfc2":   "#1c2128",
    "bdr":    "#30363d",
    "txt":    "#e6edf3",
    "txt2":   "#7d8590",
    "acc":    "#58a6ff",
    "grn":    "#3fb950",
    "ylw":    "#d29922",
    "red":    "#f85149",
    "pur":    "#a371f7",
}

# ── Custom CSS ─────────────────────────────────────────────────────────────────
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
.block-container {{ padding-top: 1.2rem; max-width: 1240px; }}

/* Hero header */
.hero {{
    background: linear-gradient(135deg, {COLORS['sfc']} 0%, {COLORS['sfc2']} 100%);
    border: 1px solid {COLORS['bdr']};
    border-radius: 16px;
    padding: 28px 32px;
    margin-bottom: 24px;
}}
.hero-title {{
    font-family: 'Syne', sans-serif;
    font-size: 28px;
    font-weight: 800;
    margin-bottom: 4px;
    color: {COLORS['txt']};
}}
.hero-sub {{
    font-size: 14px;
    color: {COLORS['txt2']};
}}

/* Navigation cards */
.nav-card {{
    background: {COLORS['sfc']};
    border: 1px solid {COLORS['bdr']};
    border-radius: 12px;
    padding: 24px;
    height: 100%;
    transition: all 0.2s;
    cursor: pointer;
    text-decoration: none !important;
    color: {COLORS['txt']} !important;
    display: block;
}}
a.nav-card, a.nav-card:hover, a.nav-card:visited {{
    text-decoration: none !important;
    color: {COLORS['txt']} !important;
}}
.nav-card:hover {{
    border-color: {COLORS['acc']};
    transform: translateY(-2px);
}}
.nav-card.disabled {{
    opacity: 0.5;
}}
.nav-icon {{
    font-size: 28px;
    margin-bottom: 12px;
}}
.nav-title {{
    font-family: 'Syne', sans-serif;
    font-size: 18px;
    font-weight: 700;
    color: {COLORS['txt']};
    margin-bottom: 8px;
}}
.nav-desc {{
    font-size: 13px;
    color: {COLORS['txt2']};
    line-height: 1.5;
    margin-bottom: 12px;
    min-height: 60px;
}}
.nav-coming-soon {{
    display: inline-block;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    background: rgba(125,133,144,0.18);
    color: {COLORS['txt2']};
}}

/* Mini KPI strip */
.kpi-strip {{
    background: {COLORS['sfc']};
    border: 1px solid {COLORS['bdr']};
    border-radius: 12px;
    padding: 18px 24px;
    margin-top: 16px;
}}
.kpi-strip-title {{
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    color: {COLORS['txt2']};
    margin-bottom: 12px;
}}
.kpi-mini {{
    display: inline-block;
    margin-right: 36px;
}}
.kpi-mini-val {{
    font-family: 'Syne', sans-serif;
    font-size: 22px;
    font-weight: 700;
    color: {COLORS['txt']};
    line-height: 1;
}}
.kpi-mini-lbl {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    color: {COLORS['txt2']};
    margin-top: 4px;
}}

/* Hide Streamlit chrome we don't want */
#MainMenu, footer {{ visibility: hidden; }}
[data-testid="stSidebarNav"] li a {{ font-size: 14px; }}

/* Streamlit's page_link buttons styled as full-width */
[data-testid="stPageLink"] {{ width: 100%; }}
</style>
""", unsafe_allow_html=True)

# ── Helpers ────────────────────────────────────────────────────────────────────
def fmt_currency(v):
    """Format a number as $X.XXM / $X.XK / $X. Defensive against None, NaN, strings, pd.NA."""
    try:
        v = float(v) if v is not None else 0.0
        if v != v:   # catches NaN (NaN != NaN is True)
            v = 0.0
    except (TypeError, ValueError):
        v = 0.0
    if v == 0:
        return "—"
    if v >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


@st.cache_data(ttl=300)
def load_summary() -> dict:
    """Pull headline KPIs from cache/latest/ to surface on the landing page."""
    out = {}
    gauges_path = LATEST_DIR / "kpi_gauges.csv"
    if gauges_path.exists():
        df = pd.read_csv(gauges_path)
        if not df.empty:
            out["gauges"] = df.iloc[0].to_dict()

    rp_path = LATEST_DIR / "retention_periods.csv"
    if rp_path.exists():
        rp = pd.read_csv(rp_path)
        out["retention"] = {
            "periods": len(rp),
            "avg_pct": round(rp["retained_pct"].mean(), 1) if not rp.empty else 0,
            "latest_period": rp.iloc[-1]["m3_label"] if not rp.empty else "—",
            "latest_pct": rp.iloc[-1]["retained_pct"] if not rp.empty else 0,
        }

    if gauges_path.exists():
        out["last_updated"] = datetime.fromtimestamp(
            gauges_path.stat().st_mtime
        ).strftime("%b %d, %Y · %I:%M %p")
    return out


# ── Page Body ──────────────────────────────────────────────────────────────────
data = load_summary()

st.markdown(f"""
<div class="hero">
  <div class="hero-title">🦷 Artistic Dental Studio — Partner Portal</div>
  <div class="hero-sub">Unified business intelligence for partners, managers, and marketing.
  {f'· Data refreshed {data["last_updated"]}' if data.get("last_updated") else ""}</div>
</div>
""", unsafe_allow_html=True)

# Navigation cards — 2x2 grid
row1_c1, row1_c2 = st.columns(2)
row2_c1, row2_c2 = st.columns(2)

with row1_c1:
    st.markdown("""
    <a class="nav-card" href="/Executive_Dashboard" target="_self">
      <div class="nav-icon">📊</div>
      <div class="nav-title">Executive Dashboard</div>
      <div class="nav-desc">Revenue, growth vs target, profitability rankings, WIP, remakes,
      implant pipeline, Pareto analysis. Live from the nightly Magic Touch pipeline.</div>
      <div style="margin-top:14px;color:#58a6ff;font-size:13px;font-weight:600">Open →</div>
    </a>
    """, unsafe_allow_html=True)

with row1_c2:
    st.markdown("""
    <a class="nav-card" href="/Doctor_Retention" target="_self">
      <div class="nav-icon">🦷</div>
      <div class="nav-title">Doctor Retention</div>
      <div class="nav-desc">Track which doctors are sending cases, at risk, or lost. Per-period
      retention windows (3M / 6M / 12M), new vs returning split, master doctor list.</div>
      <div style="margin-top:14px;color:#58a6ff;font-size:13px;font-weight:600">Open →</div>
    </a>
    """, unsafe_allow_html=True)

with row2_c1:
    st.markdown("""
    <a class="nav-card" href="/Logistics" target="_self">
      <div class="nav-icon">🚚</div>
      <div class="nav-title">Logistics</div>
      <div class="nav-desc">Open cases, where they're stuck, and what's falling behind schedule.
      Department breakdown, aging buckets, and a filterable case detail table.</div>
      <div style="margin-top:14px;color:#58a6ff;font-size:13px;font-weight:600">Open →</div>
    </a>
    """, unsafe_allow_html=True)

with row2_c2:
    st.markdown("""
    <div class="nav-card disabled">
      <div class="nav-icon">🔧</div>
      <div class="nav-title">Production Manager</div>
      <div class="nav-desc">Lab production tracking, technician performance, turnaround times
      by department. Coming soon.</div>
      <div class="nav-coming-soon">Coming Soon</div>
    </div>
    """, unsafe_allow_html=True)

# Headline KPI strip
g = data.get("gauges", {})
r = data.get("retention", {})
if g or r:
    parts = []
    if g.get("ytd_revenue"):
        parts.append(("YTD Revenue", fmt_currency(g["ytd_revenue"])))
    if g.get("actual_growth_pct") is not None:
        parts.append(("Growth vs LY", f"{g['actual_growth_pct']:+.1f}%"))
    if g.get("wip_value"):
        parts.append(("WIP Value", fmt_currency(g["wip_value"])))
    if g.get("remake_rate") is not None:
        parts.append(("Remake Rate", f"{g['remake_rate']:.1f}%"))
    if r.get("periods"):
        parts.append((f"Retention ({r['latest_period']})", f"{r['latest_pct']:.0f}%"))
        parts.append(("Periods Tracked", str(r["periods"])))

    if parts:
        kpi_html = '<div class="kpi-strip"><div class="kpi-strip-title">At a glance</div>'
        for lbl, val in parts:
            kpi_html += f'<span class="kpi-mini"><div class="kpi-mini-val">{val}</div><div class="kpi-mini-lbl">{lbl}</div></span>'
        kpi_html += '</div>'
        st.markdown(kpi_html, unsafe_allow_html=True)

st.caption("Artistic Dental Studio · Partner Analytics · Data refreshed nightly at 6 AM")
