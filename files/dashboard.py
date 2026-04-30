"""
Artistic Dental Studio — Partner Dashboard
==========================================
Run locally  : streamlit run dashboard.py
Deploy       : streamlit share / Streamlit Cloud (connect to same Google Drive)

The dashboard reads from the cache/latest_kpis.xlsx file written by pipeline.py.
It also supports pulling directly from Google Drive if a local cache isn't present.
"""

import os
import pickle
import logging
from pathlib import Path
from datetime import datetime

import yaml
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
with open(BASE_DIR / "config.yaml") as f:
    CFG = yaml.safe_load(f)

CACHE_DIR = BASE_DIR / CFG["output"]["local_cache_dir"]
LATEST_FILE = CACHE_DIR / "latest_kpis.xlsx"

# ── Page Setup ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Artistic Dental Studio",
    page_icon="🦷",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Brand Palette ──────────────────────────────────────────────────────────────
COLORS = {
    "navy":   "#1a2744",
    "teal":   "#0a8f8f",
    "gold":   "#d4a843",
    "green":  "#2ecc71",
    "red":    "#e74c3c",
    "light":  "#f0f4f8",
    "muted":  "#6c7a8a",
}

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap');

  html, body, [class*="css"] {{
      font-family: 'DM Sans', sans-serif;
      background-color: #f7f9fb;
  }}
  h1, h2, h3 {{
      font-family: 'DM Serif Display', serif;
      color: {COLORS['navy']};
  }}
  .block-container {{ padding: 1.5rem 2.5rem; }}

  /* KPI cards */
  .kpi-card {{
      background: white;
      border-radius: 12px;
      padding: 1.1rem 1.4rem;
      box-shadow: 0 2px 8px rgba(26,39,68,.07);
      border-left: 4px solid {COLORS['teal']};
      margin-bottom: 0.5rem;
  }}
  .kpi-label {{ color: {COLORS['muted']}; font-size: 0.78rem; font-weight: 600;
                letter-spacing: .05em; text-transform: uppercase; margin-bottom: 4px; }}
  .kpi-value {{ color: {COLORS['navy']}; font-size: 1.7rem; font-weight: 600; line-height:1; }}
  .kpi-sub   {{ color: {COLORS['muted']}; font-size: 0.78rem; margin-top: 4px; }}
  .kpi-ok    {{ border-left-color: {COLORS['green']}; }}
  .kpi-warn  {{ border-left-color: {COLORS['red']}; }}

  /* Section headers */
  .section-head {{
      font-family: 'DM Serif Display', serif;
      color: {COLORS['navy']};
      font-size: 1.15rem;
      margin: 1.4rem 0 .6rem;
      padding-bottom: .3rem;
      border-bottom: 2px solid {COLORS['teal']};
  }}

  /* Sidebar */
  section[data-testid="stSidebar"] {{
      background-color: {COLORS['navy']};
  }}
  section[data-testid="stSidebar"] * {{ color: white !important; }}

  /* Status badges */
  .badge-ok   {{ background:{COLORS['green']}22; color:{COLORS['green']};
                 padding:2px 8px; border-radius:20px; font-size:.75rem; font-weight:600; }}
  .badge-warn {{ background:{COLORS['red']}22; color:{COLORS['red']};
                 padding:2px 8px; border-radius:20px; font-size:.75rem; font-weight:600; }}

  .last-updated {{ color:{COLORS['muted']}; font-size:.75rem; text-align:right; }}
  div[data-testid="stMetricValue"] {{ font-size: 1.5rem !important; }}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=1800)   # refresh every 30 min
def load_kpi_data() -> dict[str, pd.DataFrame]:
    """
    Load pre-computed KPI tables.
    Priority: local cache → Google Drive download.
    """
    if LATEST_FILE.exists():
        return _read_excel_sheets(str(LATEST_FILE))

    # Fallback: pull from Google Drive
    st.info("Local cache not found — pulling from Google Drive …")
    drive_path = _download_from_drive()
    if drive_path:
        return _read_excel_sheets(drive_path)

    st.warning("No data found. Run pipeline.py first to generate data.")
    return {}


def _read_excel_sheets(path: str) -> dict[str, pd.DataFrame]:
    xl = pd.ExcelFile(path)
    return {sheet: xl.parse(sheet) for sheet in xl.sheet_names}


def _download_from_drive() -> str | None:
    """Pull latest_kpis.xlsx from Google Drive to local cache."""
    try:
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
        import io

        token_path = BASE_DIR / CFG["google_drive"]["token_file"]
        if not token_path.exists():
            return None
        with open(token_path, "rb") as f:
            creds = pickle.load(f)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())

        service = build("drive", "v3", credentials=creds)
        folder_id = CFG["google_drive"]["folder_id"]
        q = f"name='latest_kpis.xlsx' and '{folder_id}' in parents and trashed=false"
        results = service.files().list(q=q, fields="files(id)").execute()
        files = results.get("files", [])
        if not files:
            return None

        fid = files[0]["id"]
        request = service.files().get_media(fileId=fid)
        local = str(LATEST_FILE)
        CACHE_DIR.mkdir(exist_ok=True)
        with io.FileIO(local, "wb") as fh:
            dl = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = dl.next_chunk()
        return local
    except Exception as exc:
        logging.warning("Drive download failed: %s", exc)
        return None


# ── Helper: format currency ─────────────────────────────────────────────────
def fmt_currency(val):
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


# ══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD LAYOUT
# ══════════════════════════════════════════════════════════════════════════════

def render_header(kpis: dict):
    col_title, col_refresh = st.columns([4, 1])
    with col_title:
        st.markdown("## 🦷 Artistic Dental Studio — Partner Dashboard")
    with col_refresh:
        if LATEST_FILE.exists():
            mtime = datetime.fromtimestamp(LATEST_FILE.stat().st_mtime)
            st.markdown(
                f'<div class="last-updated">Last updated<br>'
                f'<strong>{mtime.strftime("%b %d, %Y %I:%M %p")}</strong></div>',
                unsafe_allow_html=True,
            )
        if st.button("🔄 Refresh"):
            st.cache_data.clear()
            st.rerun()


def render_kpi_row(gauges: pd.DataFrame):
    """Top KPI summary row — 6 cards."""
    if gauges.empty:
        st.warning("No KPI data available.")
        return

    g = gauges.iloc[0]
    cols = st.columns(6)

    with cols[0]:
        kpi_card("YTD Revenue",
                 fmt_currency(g.get("ytd_revenue", 0)),
                 f"Prior year: {fmt_currency(g.get('ytd_prior_revenue', 0))}",
                 status="ok")

    growth = g.get("actual_growth_pct", 0)
    target = g.get("target_growth_pct", 7)
    with cols[1]:
        kpi_card("Revenue Growth",
                 f"{growth:+.1f}%",
                 f"Target: +{target:.0f}%",
                 status="ok" if growth >= target else "warn")

    remake = g.get("remake_rate", 0)
    alert = g.get("remake_alert_pct", 5)
    with cols[2]:
        kpi_card("Remake Rate",
                 f"{remake:.1f}%",
                 f"Alert threshold: {alert:.0f}%",
                 status="ok" if remake < alert else "warn")

    with cols[3]:
        kpi_card("Avg Margin",
                 f"{g.get('avg_margin_pct', 0):.1f}%",
                 "Gross margin across all accounts")

    overdue = int(g.get("implant_overdue", 0))
    total_impl = int(g.get("total_implants_open", 0))
    with cols[4]:
        kpi_card("Implants Open",
                 str(total_impl),
                 f"{overdue} overdue",
                 status="warn" if overdue > 0 else "ok")

    pareto = int(g.get("pareto_account_count", 0))
    total_acc = int(g.get("total_account_count", 1))
    with cols[5]:
        kpi_card("Top-20% Accounts",
                 str(pareto),
                 f"of {total_acc} total accounts",
                 status="neutral")


def render_ytd_vs_target(ytd_df: pd.DataFrame, monthly_df: pd.DataFrame):
    section("📈 YTD Revenue vs 7% Growth Target")

    if ytd_df.empty:
        st.info("No YTD data.")
        return

    g = ytd_df.iloc[0]
    current_yr = int(g.get("current_year", datetime.now().year))
    prior_yr = current_yr - 1
    ytd_cur = g.get("ytd_current", 0)
    ytd_tgt = g.get("ytd_target", 0)
    ytd_pri = g.get("ytd_prior", 0)
    pct = g.get("actual_growth_pct", 0)
    on_track = bool(g.get("on_track", False))

    c1, c2 = st.columns([1, 2])

    with c1:
        badge = '<span class="badge-ok">✓ On Track</span>' if on_track else '<span class="badge-warn">⚠ Behind Target</span>'
        st.markdown(f"""
        <div style="background:white;border-radius:12px;padding:1.4rem;
                    box-shadow:0 2px 8px rgba(26,39,68,.07);">
          <div style="font-size:.8rem;color:{COLORS['muted']};font-weight:600;
                      text-transform:uppercase;letter-spacing:.05em;">Growth Status</div>
          <div style="font-size:2.5rem;font-weight:700;color:{COLORS['navy']};
                      margin:.4rem 0;">{pct:+.1f}%</div>
          {badge}
          <hr style="margin:.8rem 0;border-color:#eee">
          <div style="font-size:.85rem;color:{COLORS['muted']}">
            YTD Current: <strong style="color:{COLORS['navy']}">{fmt_currency(ytd_cur)}</strong><br>
            YTD Target: <strong style="color:{COLORS['teal']}">{fmt_currency(ytd_tgt)}</strong><br>
            YTD Prior:  <strong style="color:{COLORS['navy']}">{fmt_currency(ytd_pri)}</strong>
          </div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        if not monthly_df.empty:
            monthly_df["month_label"] = monthly_df.apply(
                lambda r: f"{int(r['year'])}-{int(r['month']):02d}", axis=1)
            cur = monthly_df[monthly_df["year"] == current_yr]
            pri = monthly_df[monthly_df["year"] == prior_yr]
            target_vals = pri.copy()
            target_vals["revenue"] = target_vals["revenue"] * (1 + 0.07)

            fig = go.Figure()
            fig.add_trace(go.Bar(x=cur["month_label"], y=cur["revenue"],
                                 name=str(current_yr),
                                 marker_color=COLORS["teal"], opacity=0.9))
            fig.add_trace(go.Scatter(x=pri["month_label"], y=pri["revenue"],
                                     name=str(prior_yr), mode="lines+markers",
                                     line=dict(color=COLORS["muted"], dash="dot")))
            fig.add_trace(go.Scatter(x=target_vals["month_label"],
                                     y=target_vals["revenue"],
                                     name="+7% Target", mode="lines",
                                     line=dict(color=COLORS["gold"], width=2, dash="dash")))
            fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                              margin=dict(l=10, r=10, t=20, b=10),
                              legend=dict(orientation="h", y=-0.15),
                              yaxis=dict(tickformat="$,.0f"),
                              height=260)
            st.plotly_chart(fig, use_container_width=True)


def render_profitability(prof_df: pd.DataFrame):
    section("💰 Account Profitability Rankings")
    if prof_df.empty:
        st.info("No profitability data.")
        return

    col1, col2 = st.columns([3, 2])
    with col1:
        top = prof_df.nlargest(15, "gross_profit")
        label_col = "account_name" if "account_name" in top.columns else "account_id"
        fig = go.Figure(go.Bar(
            x=top["gross_profit"],
            y=top[label_col].astype(str),
            orientation="h",
            marker=dict(
                color=top["margin_pct"],
                colorscale=[[0, "#e8f5f5"], [0.5, COLORS["teal"]], [1, COLORS["navy"]]],
                showscale=True,
                colorbar=dict(title="Margin %", thickness=12)
            ),
            text=top.apply(lambda r: f"  ${r['gross_profit']:,.0f}  ({r['margin_pct']:.0f}%)", axis=1),
            textposition="outside"
        ))
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                          yaxis=dict(autorange="reversed"),
                          margin=dict(l=10, r=20, t=20, b=10), height=420,
                          xaxis=dict(tickformat="$,.0f"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        display = prof_df.copy()
        display = display.nlargest(20, "gross_profit")
        label_col = "account_name" if "account_name" in display.columns else "account_id"
        display = display[[label_col, "revenue", "gross_profit", "margin_pct", "orders"]]
        display.columns = ["Account", "Revenue", "Gross Profit", "Margin %", "Orders"]
        display["Revenue"] = display["Revenue"].apply(lambda v: f"${v:,.0f}")
        display["Gross Profit"] = display["Gross Profit"].apply(lambda v: f"${v:,.0f}")
        display["Margin %"] = display["Margin %"].apply(lambda v: f"{v:.1f}%")
        st.dataframe(display, use_container_width=True, height=400, hide_index=True)


def render_pareto(pareto_df: pd.DataFrame, prof_df: pd.DataFrame):
    section("⭐ Top 20% Accounts — Pareto Analysis")
    if pareto_df.empty:
        st.info("No Pareto data.")
        return

    label_col = "account_name" if "account_name" in prof_df.columns else "account_id"
    sorted_df = prof_df.sort_values("revenue", ascending=False).copy()
    sorted_df["cum_revenue"] = sorted_df["revenue"].cumsum()
    total = sorted_df["revenue"].sum()
    sorted_df["cum_pct"] = sorted_df["cum_revenue"] / total * 100
    sorted_df["rank"] = range(1, len(sorted_df) + 1)

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    top15 = sorted_df.head(15)
    fig.add_trace(go.Bar(
        x=top15[label_col].astype(str),
        y=top15["revenue"],
        marker_color=COLORS["teal"],
        name="Revenue"
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=sorted_df[label_col].astype(str).head(15),
        y=sorted_df["cum_pct"].head(15),
        mode="lines+markers",
        name="Cumulative %",
        line=dict(color=COLORS["gold"], width=2),
        marker=dict(size=6)
    ), secondary_y=True)

    fig.add_hline(y=80, line_dash="dash", line_color=COLORS["red"],
                  annotation_text="80% threshold", secondary_y=True)
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                      margin=dict(l=10, r=10, t=20, b=60),
                      legend=dict(orientation="h", y=-0.25),
                      height=350,
                      xaxis=dict(tickangle=-30))
    fig.update_yaxes(tickformat="$,.0f", secondary_y=False)
    fig.update_yaxes(tickformat=".0f", ticksuffix="%", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    n = len(pareto_df)
    total_n = len(prof_df)
    pareto_rev = pareto_df["revenue"].sum()
    pct_rev = pareto_rev / total * 100 if total else 0
    st.caption(
        f"**{n} accounts** ({n/total_n*100:.0f}% of {total_n} total) "
        f"drive **{fmt_currency(pareto_rev)}** ({pct_rev:.0f}% of revenue)"
    )


def render_remakes(remake_df: pd.DataFrame):
    section("🔁 Remake Trends")
    if remake_df.empty:
        st.info("No remake data.")
        return

    alert = CFG["targets"]["remake_alert_pct"]
    fig = go.Figure()
    fig.add_hrect(y0=alert, y1=remake_df["remake_rate"].max() * 1.2,
                  fillcolor=COLORS["red"], opacity=0.06, line_width=0)
    fig.add_hline(y=alert, line_dash="dash", line_color=COLORS["red"],
                  annotation_text=f"Alert: {alert}%")
    fig.add_trace(go.Scatter(
        x=remake_df["yearmonth"], y=remake_df["remake_rate"],
        mode="lines+markers",
        fill="tozeroy",
        fillcolor=f"{COLORS['teal']}22",
        line=dict(color=COLORS["teal"], width=2.5),
        marker=dict(size=7,
                    color=[COLORS["red"] if v >= alert else COLORS["teal"]
                           for v in remake_df["remake_rate"]]),
        text=remake_df["remake_rate"].apply(lambda v: f"{v:.1f}%"),
        hovertemplate="%{x}<br>Remake Rate: %{y:.2f}%<extra></extra>"
    ))
    fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                      margin=dict(l=10, r=10, t=20, b=10), height=280,
                      yaxis=dict(ticksuffix="%"))
    st.plotly_chart(fig, use_container_width=True)

    c1, c2, c3 = st.columns(3)
    c1.metric("Latest Month", f"{remake_df['remake_rate'].iloc[-1]:.1f}%")
    c2.metric("3-Month Avg",
              f"{remake_df['remake_rate'].tail(3).mean():.1f}%")
    c3.metric("YTD Total Remakes", f"{int(remake_df['remakes'].sum())}")


def render_implant_pipeline(impl_df: pd.DataFrame):
    section("🔬 Implant Pipeline")
    if impl_df.empty:
        st.info("No open implant cases.")
        return

    turnaround = CFG["targets"]["implant_turnaround_days"]
    c1, c2 = st.columns([2, 3])

    with c1:
        stage_col = "stage" if "stage" in impl_df.columns else None
        if stage_col:
            stage_counts = impl_df.groupby("stage").size().reset_index(name="count")
            fig = px.pie(stage_counts, names="stage", values="count",
                         color_discrete_sequence=[COLORS["navy"], COLORS["teal"],
                                                   COLORS["gold"], COLORS["green"],
                                                   COLORS["muted"]])
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=20, b=0),
                               paper_bgcolor="white", height=260)
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        display = impl_df.copy()
        if "days_in_progress" in display.columns:
            display["Status"] = display["days_in_progress"].apply(
                lambda d: "⚠️ Overdue" if d > turnaround else "✅ On Time"
            )
        cols_show = [c for c in ["case_id", "account_id", "stage", "days_in_progress",
                                   "due_date", "Status"] if c in display.columns]
        st.dataframe(display[cols_show].head(25), use_container_width=True,
                     height=280, hide_index=True)


def render_gauge_row(gauges: pd.DataFrame):
    section("🎯 KPI Gauges")
    if gauges.empty:
        return

    g = gauges.iloc[0]
    growth = g.get("actual_growth_pct", 0)
    target = g.get("target_growth_pct", 7)
    remake = g.get("remake_rate", 0)
    remake_alert = g.get("remake_alert_pct", 5)
    margin = g.get("avg_margin_pct", 0)

    def gauge(title, value, min_val, max_val, threshold, suffix="%",
              invert=False, color_ok=None, color_warn=None):
        ok = value >= threshold if not invert else value <= threshold
        bar_color = (color_ok or COLORS["teal"]) if ok else (color_warn or COLORS["red"])
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=value,
            number={"suffix": suffix, "font": {"size": 28}},
            delta={"reference": threshold, "suffix": suffix},
            gauge={
                "axis": {"range": [min_val, max_val], "ticksuffix": suffix},
                "bar": {"color": bar_color, "thickness": .35},
                "bgcolor": "white",
                "borderwidth": 0,
                "steps": [
                    {"range": [min_val, threshold], "color": "#f0f4f8"},
                    {"range": [threshold, max_val], "color": "#e8f5f5"},
                ],
                "threshold": {
                    "line": {"color": COLORS["gold"], "width": 3},
                    "thickness": 0.75,
                    "value": threshold
                }
            },
            title={"text": title, "font": {"size": 13, "family": "DM Sans"}},
        ))
        fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=0),
                          paper_bgcolor="white")
        return fig

    cols = st.columns(3)
    with cols[0]:
        st.plotly_chart(gauge("Revenue Growth", growth, -10, 20, target,
                              suffix="%"), use_container_width=True)
    with cols[1]:
        st.plotly_chart(gauge("Remake Rate", remake, 0, 10, remake_alert,
                              suffix="%", invert=True,
                              color_ok=COLORS["teal"], color_warn=COLORS["red"]),
                        use_container_width=True)
    with cols[2]:
        st.plotly_chart(gauge("Avg Gross Margin", margin, 0, 60, 30,
                              suffix="%"), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    data = load_kpi_data()

    gauges      = data.get("kpi_gauges", pd.DataFrame())
    prof        = data.get("profitability", pd.DataFrame())
    pareto      = data.get("pareto_accounts", pd.DataFrame())
    remakes     = data.get("remake_trends", pd.DataFrame())
    ytd_summary = data.get("ytd_summary", pd.DataFrame())
    monthly_rev = data.get("monthly_revenue", pd.DataFrame())
    implants    = data.get("implant_pipeline", pd.DataFrame())

    render_header(data)
    st.divider()
    render_kpi_row(gauges)
    st.divider()
    render_gauge_row(gauges)
    st.divider()
    render_ytd_vs_target(ytd_summary, monthly_rev)
    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs([
        "💰 Profitability Rankings",
        "⭐ Top 20% Accounts",
        "🔁 Remake Trends",
        "🔬 Implant Pipeline",
    ])
    with tab1:
        render_profitability(prof)
    with tab2:
        render_pareto(pareto, prof)
    with tab3:
        render_remakes(remakes)
    with tab4:
        render_implant_pipeline(implants)

    # ── Footer ─────────────────────────────────────────────────────────────
    st.divider()
    st.caption(
        "Artistic Dental Studio · Partner Analytics · "
        "Data refreshed nightly at 6 AM · "
        "Questions? Contact the lab administrator."
    )


if __name__ == "__main__":
    main()
