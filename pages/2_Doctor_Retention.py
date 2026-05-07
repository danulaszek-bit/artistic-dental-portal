"""
Artistic Dental Studio — Doctor Retention Dashboard (sub-page)
==============================================================
Python/Streamlit port of Lexie's v3 HTML dashboard.

Reads from cache/latest/:
    retention_periods.csv  — one row per loaded period
    retention_doctors.csv  — one row per (period, doctor)
    retention_master.csv   — one row per unique doctor
    case_history.csv       — long-lived deduped case history

Status rules (matches Lexie):
    retained = sent ≥1 REAL case in M3
    at_risk  = real case in M2, none in M3
    lost     = no real cases in M2 or M3
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, date

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# ── Setup ──────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
LATEST_DIR = BASE_DIR / "cache" / "latest"
CACHE_DIR = BASE_DIR / "cache"

st.set_page_config(
    page_title="Doctor Retention — Artistic Dental",
    page_icon="🦷",
    layout="wide",
)

MONTH_ABBR = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

# ── Dark Palette (matches Lexie's v3 exactly) ─────────────────────────────────
COLORS = {
    "bg":   "#0d1117", "sfc":  "#161b22", "sfc2": "#1c2128",
    "bdr":  "#30363d", "bdr2": "#21262d",
    "txt":  "#e6edf3", "txt2": "#7d8590",
    "acc":  "#58a6ff", "grn":  "#3fb950", "ylw": "#d29922",
    "red":  "#f85149", "pur":  "#a371f7", "org": "#f0883e",
    "teal": "#39c5cf",
}

# ── Custom CSS (matches Lexie's v3 visual language) ───────────────────────────
st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@500;700;800&family=DM+Sans:wght@400;500;600&display=swap');

html, body, [class*="css"], .stApp {{
    background-color: {COLORS['bg']} !important;
    color: {COLORS['txt']};
    font-family: 'DM Sans', sans-serif;
}}
h1, h2, h3 {{ font-family: 'Syne', sans-serif !important; color: {COLORS['txt']} !important; }}
.block-container {{ padding: 1rem 1.5rem; max-width: 1400px; }}

/* Stat pills (the row of clickable counters) */
.spill {{
    background: {COLORS['sfc']};
    border: 1px solid {COLORS['bdr']};
    border-radius: 8px;
    padding: 10px 14px;
    display: inline-block;
    margin-right: 8px;
    margin-bottom: 6px;
    min-width: 90px;
}}
.snum {{
    font-family: 'Syne', sans-serif;
    font-size: 22px;
    font-weight: 700;
    line-height: 1;
    color: {COLORS['txt']};
}}
.slbl {{
    font-size: 9px;
    color: {COLORS['txt2']};
    margin-top: 2px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    font-weight: 600;
}}
.sdot {{ width: 7px; height: 7px; border-radius: 50%; display: inline-block; margin-right: 6px; }}
.sdot-g {{ background: {COLORS['grn']}; box-shadow: 0 0 5px rgba(63,185,80,.5); }}
.sdot-y {{ background: {COLORS['ylw']}; box-shadow: 0 0 5px rgba(210,153,34,.5); }}
.sdot-r {{ background: {COLORS['red']}; box-shadow: 0 0 5px rgba(248,81,73,.5); }}
.sdot-b {{ background: {COLORS['acc']}; }}
.sdot-p {{ background: {COLORS['pur']}; }}

/* Retention breakdown cards (Overall / New / Returning) */
.bk-row {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-bottom: 16px; }}
.bk-card {{
    background: {COLORS['sfc']};
    border: 1px solid {COLORS['bdr']};
    border-radius: 10px;
    padding: 16px 18px;
}}
.bk-card.overall {{ border-top: 2px solid {COLORS['acc']}; }}
.bk-card.new     {{ border-top: 2px solid {COLORS['pur']}; }}
.bk-card.ret     {{ border-top: 2px solid {COLORS['teal']}; }}
.bk-lbl {{
    font-size: 10px; color: {COLORS['txt2']};
    text-transform: uppercase; letter-spacing: 0.4px; font-weight: 700;
    margin-bottom: 10px;
}}
.bk-rate-row {{ display: flex; align-items: center; gap: 8px; padding: 6px 0; border-top: 1px solid {COLORS['bdr2']}; }}
.bk-rate-row:first-of-type {{ border-top: none; padding-top: 0; }}
.bk-rate-lbl {{ font-size: 10px; color: {COLORS['txt2']}; font-weight: 700; width: 32px; }}
.bk-rate-pct {{
    font-family: 'Syne', sans-serif;
    font-size: 16px; font-weight: 700;
    width: 48px; color: {COLORS['txt']};
}}
.bk-rate-bar {{ flex: 1; height: 5px; background: {COLORS['bdr2']}; border-radius: 3px; overflow: hidden; }}
.bk-rate-fill {{ height: 100%; border-radius: 3px; }}
.bk-rate-ct  {{ font-size: 10px; color: {COLORS['txt2']}; width: 42px; text-align: right; }}
.bk-bd-row   {{ font-size: 11px; color: {COLORS['txt2']}; margin-top: 8px; padding-top: 8px;
                border-top: 1px solid {COLORS['bdr2']}; }}

/* Status badges in tables */
.bdg {{
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px;
}}
.b-new {{ background: rgba(163,113,247,0.18); color: {COLORS['pur']}; }}
.b-ret {{ background: rgba(88,166,255,0.18);  color: {COLORS['acc']}; }}
.b-dig {{ background: rgba(63,185,80,0.18);   color: {COLORS['grn']}; }}

.stl-retained {{ color: {COLORS['grn']}; font-weight: 700; }}
.stl-at_risk  {{ color: {COLORS['ylw']}; font-weight: 700; }}
.stl-lost     {{ color: {COLORS['red']}; font-weight: 700; }}

/* Doctor table */
.dr-table-wrap {{
    background: {COLORS['sfc']};
    border: 1px solid {COLORS['bdr']};
    border-radius: 10px;
    overflow: hidden;
    margin-top: 14px;
}}
.dr-table-head {{
    padding: 12px 18px;
    border-bottom: 1px solid {COLORS['bdr']};
    background: {COLORS['sfc']};
}}
.dr-table-title {{
    font-family: 'Syne', sans-serif;
    font-size: 16px;
    font-weight: 700;
    color: {COLORS['txt']};
}}
.dr-table-sub {{
    font-size: 12px;
    color: {COLORS['txt2']};
    margin-top: 2px;
}}
.dr-table-info {{
    font-size: 11px; color: {COLORS['acc']};
    padding: 8px 18px; background: rgba(88,166,255,0.06);
    border-bottom: 1px solid {COLORS['bdr2']};
}}
.dr-table-warn {{
    font-size: 11px; color: {COLORS['ylw']};
    padding: 8px 18px; background: rgba(210,153,34,0.06);
    border-bottom: 1px solid {COLORS['bdr2']};
}}
table.dr-grid {{
    width: 100%; border-collapse: collapse; font-size: 12px; color: {COLORS['txt']};
}}
table.dr-grid th {{
    text-align: left; padding: 8px 10px;
    font-size: 9px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.4px;
    color: {COLORS['txt2']};
    background: {COLORS['sfc2']};
    border-bottom: 1px solid {COLORS['bdr']};
    white-space: nowrap;
}}
table.dr-grid th.c, table.dr-grid td.c {{ text-align: center; }}
table.dr-grid td {{
    padding: 8px 10px;
    border-bottom: 1px solid {COLORS['bdr2']};
    vertical-align: middle;
}}
table.dr-grid tr:hover td {{ background: rgba(88,166,255,0.04); }}
table.dr-grid tr:last-child td {{ border-bottom: none; }}

.th-grp-3m  {{ background: rgba(63,185,80,0.10) !important;  color: {COLORS['grn']} !important; }}
.th-grp-6m  {{ background: rgba(88,166,255,0.10) !important; color: {COLORS['acc']} !important; }}
.th-grp-12m {{ background: rgba(163,113,247,0.10) !important; color: {COLORS['pur']} !important; }}

.dot-real {{ display:inline-block; width:10px; height:10px; border-radius:2px; background:{COLORS['grn']}; }}
.dot-rmk  {{ display:inline-block; width:10px; height:10px; border-radius:2px; background:{COLORS['ylw']}; }}
.dot-none {{ display:inline-block; width:10px; height:10px; border-radius:2px; background:{COLORS['bdr2']}; }}

/* Checkmark month indicators (matches Lexie's v3 mok / mno) */
.mok   {{ color: {COLORS['grn']};  font-size: 15px; font-weight: 700; }}
.mno   {{ color: {COLORS['bdr']};  font-size: 16px; font-weight: 400; }}

/* Filter pills (matches Lexie's v3 .fbtn / fg / fy / fr / fb / fp) */
.fbtn-row {{
    display: flex; gap: 7px; flex-wrap: wrap;
    margin: 14px 0 6px;
}}
.fbtn {{
    padding: 5px 14px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    cursor: pointer;
    border: 1px solid {COLORS['bdr']};
    background: transparent;
    color: {COLORS['txt2']};
    text-transform: uppercase;
    letter-spacing: 0.3px;
    text-decoration: none !important;
    transition: all 0.15s;
}}
.fbtn:hover {{ color: {COLORS['txt']}; border-color: {COLORS['txt2']}; }}
.fbtn.on  {{ background: {COLORS['sfc']}; color: {COLORS['txt']}; border-color: {COLORS['acc']}; }}
.fbtn.fb.on {{ background: rgba(88,166,255,0.12);  color: {COLORS['acc']}; border-color: {COLORS['acc']}; }}
.fbtn.fg.on {{ background: rgba(63,185,80,0.12);   color: {COLORS['grn']}; border-color: {COLORS['grn']}; }}
.fbtn.fy.on {{ background: rgba(210,153,34,0.12);  color: {COLORS['ylw']}; border-color: {COLORS['ylw']}; }}
.fbtn.fr.on {{ background: rgba(248,81,73,0.12);   color: {COLORS['red']}; border-color: {COLORS['red']}; }}
.fbtn.fp.on {{ background: rgba(163,113,247,0.12); color: {COLORS['pur']}; border-color: {COLORS['pur']}; }}
.fbtn.ft.on {{ background: rgba(57,197,207,0.12);  color: {COLORS['teal']}; border-color: {COLORS['teal']}; }}

/* Streamlit tweaks */
.stTabs [data-baseweb="tab-list"] {{ gap: 4px; border-bottom: 1px solid {COLORS['bdr']}; }}
.stTabs [data-baseweb="tab"] {{
    background: transparent; color: {COLORS['txt2']};
    padding: 10px 16px; font-weight: 500;
}}
.stTabs [aria-selected="true"] {{
    background: transparent; color: {COLORS['acc']} !important;
    border-bottom: 2px solid {COLORS['acc']};
}}
[data-testid="stMetricValue"] {{ color: {COLORS['txt']} !important; font-family: 'Syne', sans-serif; }}
[data-testid="stMetricLabel"] {{ color: {COLORS['txt2']} !important; }}

/* Empty state */
.empty {{
    padding: 60px 20px; text-align: center; color: {COLORS['txt2']};
    background: {COLORS['sfc']}; border: 1px dashed {COLORS['bdr']};
    border-radius: 12px; margin: 20px 0;
}}
.empty-ico {{ font-size: 40px; margin-bottom: 12px; opacity: 0.5; }}
.empty-ttl {{ font-size: 16px; font-weight: 600; color: {COLORS['txt']}; margin-bottom: 6px; }}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_retention_data():
    """Load retention CSVs from cache/latest/. NOTE: doctor_activity.csv is the
    PHI-free aggregated version of case_history. case_history stays local only."""
    out = {
        "periods":  pd.DataFrame(),
        "doctors":  pd.DataFrame(),
        "master":   pd.DataFrame(),
        "activity": pd.DataFrame(),
    }
    pp = LATEST_DIR / "retention_periods.csv"
    pd_ = LATEST_DIR / "retention_doctors.csv"
    pm = LATEST_DIR / "retention_master.csv"
    pa = LATEST_DIR / "doctor_activity.csv"
    if pp.exists(): out["periods"]  = pd.read_csv(pp)
    if pd_.exists(): out["doctors"]  = pd.read_csv(pd_, dtype={"customer_id": str})
    if pm.exists(): out["master"]   = pd.read_csv(pm,  dtype={"customer_id": str})
    if pa.exists(): out["activity"] = pd.read_csv(pa,  dtype={"account_id": str})
    return out


def doctor_monthly_activity(activity_df: pd.DataFrame) -> dict:
    """
    Convert the aggregated doctor_activity.csv DataFrame into the lookup
    structure the doctor table needs: {account_id: {(month, year): status}}.
    """
    if activity_df is None or activity_df.empty:
        return {}
    out: dict = {}
    for _, row in activity_df.iterrows():
        cid = str(row["account_id"])
        if cid not in out:
            out[cid] = {}
        out[cid][(int(row["month"]), int(row["year"]))] = row["status"]
    return out


def empty_state(icon: str, title: str, body: str = ""):
    st.markdown(f"""
    <div class="empty">
      <div class="empty-ico">{icon}</div>
      <div class="empty-ttl">{title}</div>
      <div>{body}</div>
    </div>""", unsafe_allow_html=True)


def fmt_pct(num: int, den: int) -> str:
    if den == 0:
        return "—"
    return f"{num/den*100:.0f}%"


def status_pill(status: str) -> str:
    label = {"retained": "Retained", "at_risk": "At Risk", "lost": "Lost", "": "—"}.get(status, status)
    cls = f"stl-{status}" if status else ""
    dot = {"retained": "g", "at_risk": "y", "lost": "r"}.get(status, "")
    dot_html = f'<span class="sdot sdot-{dot}"></span>' if dot else ""
    return f'{dot_html}<span class="{cls}">{label}</span>'


def month_dot(activity: str | None) -> str:
    """Match Lexie's v3 mok() exactly: green check for active (real cases), grey dash otherwise.
    Remake-only months are treated as inactive — no separate marker (matches v3 screenshot)."""
    if activity == "real":
        return '<span class="mok" title="Sent real case">✓</span>'
    return '<span class="mno" title="No real cases">—</span>'


def window_months(m1_month: int, m1_year: int, start_offset: int, count: int):
    """Build (month, year) tuples starting at m1+start_offset."""
    out = []
    m = m1_month + start_offset
    y = m1_year
    for _ in range(count):
        while m > 11:
            m -= 12; y += 1
        out.append((m, y))
        m += 1
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER: BREAKDOWN CARDS (Overall / New / Returning, with 3M/6M/12M bars)
# ══════════════════════════════════════════════════════════════════════════════

def _bar_row(label: str, num: int, den: int, color: str) -> str:
    pct = (num / den * 100) if den else 0
    return (f'<div class="bk-rate-row">'
            f'<span class="bk-rate-lbl">{label}</span>'
            f'<span class="bk-rate-pct">{pct:.0f}%</span>'
            f'<div class="bk-rate-bar"><div class="bk-rate-fill" '
            f'style="background:{color};width:{pct:.0f}%"></div></div>'
            f'<span class="bk-rate-ct">{num}/{den}</span>'
            f'</div>')


def render_breakdown_cards(rows_overall: pd.DataFrame,
                           rows_new: pd.DataFrame,
                           rows_ret: pd.DataFrame,
                           include_6m_12m: bool = True):
    """Render the three retention breakdown cards (Overall / New / Returning)."""
    def card(cls: str, label: str, df: pd.DataFrame, color: str) -> str:
        n = len(df)
        if n == 0:
            return (f'<div class="bk-card {cls}">'
                    f'<div class="bk-lbl">{label}</div>'
                    f'<div style="color:{COLORS["txt2"]};font-size:12px">No doctors</div>'
                    f'</div>')
        ret_3m = (df["status_3m"] == "retained").sum()
        bars = _bar_row("3M", ret_3m, n, color)
        if include_6m_12m and "status_6m" in df.columns and df["status_6m"].notna().any():
            ret_6m = (df["status_6m"] == "retained").sum()
            ret_12m = (df["status_12m"] == "retained").sum()
            den_6m = (df["status_6m"] != "").sum()
            den_12m = (df["status_12m"] != "").sum()
            bars += _bar_row("6M", int(ret_6m), int(den_6m), color)
            bars += _bar_row("12M", int(ret_12m), int(den_12m), color)
        ari = (df["status_3m"] == "at_risk").sum()
        lost = (df["status_3m"] == "lost").sum()
        legend = (f'<div class="bk-bd-row">'
                  f'<span class="sdot sdot-g"></span>{ret_3m} retained &nbsp;'
                  f'<span class="sdot sdot-y"></span>{ari} at risk &nbsp;'
                  f'<span class="sdot sdot-r"></span>{lost} lost</div>')
        return (f'<div class="bk-card {cls}">'
                f'<div class="bk-lbl">{label}</div>'
                f'{bars}'
                f'{legend}'
                f'</div>')

    html = '<div class="bk-row">'
    html += card("overall", "Overall Retention", rows_overall, COLORS['acc'])
    html += card("new",     "New Doctor Retention", rows_new, COLORS['pur'])
    html += card("ret",     "Returning Doctor Retention", rows_ret, COLORS['teal'])
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER: STAT PILL ROW
# ══════════════════════════════════════════════════════════════════════════════

def render_stat_pills(rows: pd.DataFrame):
    n = len(rows)
    if n == 0:
        return
    ret = (rows["status_3m"] == "retained").sum()
    ari = (rows["status_3m"] == "at_risk").sum()
    lost = (rows["status_3m"] == "lost").sum()
    new = (rows["type"].str.lower() == "new").sum() if "type" in rows.columns else 0
    returning = (rows["type"].str.lower() == "returning").sum() if "type" in rows.columns else 0
    digital = rows["digital"].astype(str).isin(("True","true","1")).sum() if "digital" in rows.columns else 0

    pills = [
        ("",   n,         "All Tracked"),
        ("g",  int(ret),  "Retained"),
        ("y",  int(ari),  "At Risk"),
        ("r",  int(lost), "Lost"),
        ("p",  int(new),  "New"),
        ("b",  int(returning), "Returning"),
        ("",   int(digital),   "Digital"),
    ]
    html = ""
    for color, val, lbl in pills:
        dot = f'<span class="sdot sdot-{color}"></span>' if color else ""
        html += f'<div class="spill">{dot}<span class="snum">{val}</span><div class="slbl">{lbl}</div></div>'
    st.markdown(html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER: DOCTOR TABLE WITH 3M/6M/12M GROUPED HEADERS
# ══════════════════════════════════════════════════════════════════════════════

def render_doctor_table(rows: pd.DataFrame,
                        period: dict | None = None,
                        activity: pd.DataFrame | None = None,
                        title: str = "Doctors",
                        subtitle: str = ""):
    """Render the period doctor table with grouped 3M/6M/12M columns."""
    if rows.empty:
        empty_state("👥", "No doctors match your filter")
        return

    # Compute monthly columns from the period's M1
    months_3m = months_6m = months_12m = []
    activity_map = {}
    if period and isinstance(period.get("m1_month"), (int, float)):
        m1m = int(period["m1_month"]); m1y = int(period["m1_year"])
        months_3m  = window_months(m1m, m1y, 0, 3)
        months_6m  = window_months(m1m, m1y, 3, 3)
        months_12m = window_months(m1m, m1y, 6, 6)
        if activity is not None and not activity.empty:
            activity_map = doctor_monthly_activity(activity)

    def m_label(my): return f"{MONTH_ABBR[my[0]]}'{str(my[1])[2:]}"

    # Build table HTML
    html = f"""
    <div class="dr-table-wrap">
      <div class="dr-table-head">
        <div class="dr-table-title">{title}</div>
        {f'<div class="dr-table-sub">{subtitle}</div>' if subtitle else ''}
      </div>
      <div class="dr-table-info">📊 Activity from Case List. Remake-100% and adjustment cases excluded.
      6M = months 4–6 from start, 12M = months 7–12 from start.</div>
      <table class="dr-grid">
        <colgroup><col><col><col><col>"""
    for _ in months_3m + months_6m + months_12m:
        html += "<col>"
    html += "<col><col><col></colgroup><thead>"

    # Two-row header (groups + month labels)
    if months_3m:
        html += "<tr>"
        html += '<th colspan="4"></th>'
        html += f'<th colspan="{len(months_3m)+1}" class="th-grp-3m c">3-Month</th>'
        html += f'<th colspan="{len(months_6m)+1}" class="th-grp-6m c">6-Month</th>'
        html += f'<th colspan="{len(months_12m)+1}" class="th-grp-12m c">12-Month</th>'
        html += "</tr>"
    html += "<tr>"
    html += "<th>Customer ID</th><th>Doctor Name</th><th>Type</th><th>Digital</th>"
    for my in months_3m: html += f'<th class="th-grp-3m c">{m_label(my)}</th>'
    if months_3m: html += '<th class="th-grp-3m c">3M</th>'
    for my in months_6m: html += f'<th class="th-grp-6m c">{m_label(my)}</th>'
    if months_6m: html += '<th class="th-grp-6m c">6M</th>'
    for my in months_12m: html += f'<th class="th-grp-12m c">{m_label(my)}</th>'
    if months_12m: html += '<th class="th-grp-12m c">12M</th>'
    html += "</tr></thead><tbody>"

    # Body rows
    for _, r in rows.iterrows():
        cid = str(r.get("customer_id", ""))
        name = str(r.get("name", ""))
        dtype = str(r.get("type", ""))
        type_badge = ""
        if dtype.lower() == "new":
            type_badge = '<span class="bdg b-new">New</span>'
        elif dtype.lower() == "returning":
            type_badge = '<span class="bdg b-ret">Returning</span>'
        else:
            type_badge = dtype
        is_digital = str(r.get("digital", "")).lower() in ("true","1")
        digital_html = '<span class="bdg b-dig">Digital</span>' if is_digital else 'Non-Digital'

        act = activity_map.get(cid, {}) if activity_map else {}

        html += f"<tr><td><code style='color:{COLORS['acc']};font-size:11px'>{cid}</code></td>"
        html += f"<td style='font-weight:500'>{name}</td>"
        html += f"<td>{type_badge}</td><td>{digital_html}</td>"
        for my in months_3m:
            html += f'<td class="c">{month_dot(act.get(my))}</td>'
        if months_3m:
            html += f'<td class="c">{status_pill(str(r.get("status_3m","")))}</td>'
        for my in months_6m:
            html += f'<td class="c">{month_dot(act.get(my))}</td>'
        if months_6m:
            html += f'<td class="c">{status_pill(str(r.get("status_6m","")))}</td>'
        for my in months_12m:
            html += f'<td class="c">{month_dot(act.get(my))}</td>'
        if months_12m:
            html += f'<td class="c">{status_pill(str(r.get("status_12m","")))}</td>'
        html += "</tr>"
    html += "</tbody></table></div>"
    st.markdown(html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER: OVERVIEW TAB
# ══════════════════════════════════════════════════════════════════════════════

def render_overview(periods_df: pd.DataFrame, doctors_df: pd.DataFrame):
    if periods_df.empty:
        empty_state("📋", "No retention periods loaded",
                    "Run the pipeline (or import_case_history.py) to seed data, "
                    "then add retention period XLS files via the pipeline.")
        return

    # Top KPI row
    n_periods = len(periods_df)
    total = doctors_df["status_3m"].count() if not doctors_df.empty else 0
    avg_ret_pct = round(periods_df["retained_pct"].mean(), 1) if not periods_df.empty else 0

    new_rows = doctors_df[doctors_df["type"].str.lower() == "new"]
    ret_rows = doctors_df[doctors_df["type"].str.lower() == "returning"]
    new_avg = round(
        100 * (new_rows["status_3m"] == "retained").sum() / max(len(new_rows), 1), 0)
    ret_avg = round(
        100 * (ret_rows["status_3m"] == "retained").sum() / max(len(ret_rows), 1), 0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Periods Loaded", str(n_periods))
    c2.metric("Avg Overall Retention", f"{avg_ret_pct:.0f}%")
    c3.metric("Avg New Doctor", f"{new_avg:.0f}%")
    c4.metric("Avg Returning Doctor", f"{ret_avg:.0f}%")

    st.markdown("### 📈 Monthly Retention Trend")

    # Build stacked bar chart per period
    pdf = periods_df.copy().sort_values(["year", "period_id"]).reset_index(drop=True)
    labels = [f"{m3} ({t})" for m3, t in zip(pdf["m3_label"], pdf["total"])]

    fig = go.Figure()
    fig.add_trace(go.Bar(name="Retained", x=labels, y=pdf["retained"],
                         marker_color=COLORS['grn']))
    fig.add_trace(go.Bar(name="At Risk",  x=labels, y=pdf["at_risk"],
                         marker_color=COLORS['ylw']))
    fig.add_trace(go.Bar(name="Lost",     x=labels, y=pdf["lost"],
                         marker_color=COLORS['red']))
    fig.update_layout(
        barmode="stack",
        plot_bgcolor=COLORS["sfc"], paper_bgcolor=COLORS["sfc"],
        font=dict(color=COLORS["txt"], family="DM Sans"),
        margin=dict(l=10, r=10, t=20, b=80),
        height=380,
        legend=dict(orientation="h", y=-0.25, bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(gridcolor=COLORS["bdr2"], color=COLORS["txt2"], tickangle=-30),
        yaxis=dict(gridcolor=COLORS["bdr2"], color=COLORS["txt2"], title="Doctors"),
    )
    # Add % annotations on top of each stack
    for i, row in pdf.iterrows():
        fig.add_annotation(x=labels[i], y=row["total"],
                           text=f"{row['retained_pct']:.0f}%",
                           showarrow=False, yshift=12,
                           font=dict(color=COLORS['txt'], size=11))
    st.plotly_chart(fig, width='stretch')

    # Per-period new/returning split
    st.markdown("### Per-period breakdown")
    sub_html = '<div style="display:flex;gap:8px;flex-wrap:wrap">'
    for _, p in pdf.iterrows():
        sub_html += f"""<div style="background:{COLORS['sfc']};border:1px solid {COLORS['bdr']};
                        border-radius:8px;padding:10px 14px;min-width:140px">
          <div style="font-size:10px;color:{COLORS['txt2']};text-transform:uppercase;
                     letter-spacing:0.3px;margin-bottom:4px">{p['m3_label']}</div>
          <div style="font-size:13px;color:{COLORS['txt']}">
            <strong style="color:{COLORS['grn']}">{p['retained_pct']:.0f}%</strong> retained
          </div>
          <div style="font-size:11px;color:{COLORS['txt2']};margin-top:2px">
            {int(p['total'])} doctors tracked
          </div>
        </div>"""
    sub_html += "</div>"
    st.markdown(sub_html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER: MASTER TAB
# ══════════════════════════════════════════════════════════════════════════════

def render_master(master_df: pd.DataFrame, doctors_df: pd.DataFrame):
    if master_df.empty:
        empty_state("⭐", "No master doctor data available")
        return

    st.markdown(f"### ⭐ Master Doctor List · {len(master_df)} doctors")
    search = st.text_input("🔍 Search doctor, ID, or city",
                           placeholder="e.g. AUSMIC or Aust", label_visibility="collapsed")

    df = master_df.copy()
    if search:
        s = search.lower()
        df = df[df["customer_id"].str.lower().str.contains(s, na=False) |
                df["name"].str.lower().str.contains(s, na=False) |
                df["city"].str.lower().str.contains(s, na=False)]

    # Display columns
    show = pd.DataFrame({
        "Customer ID": df["customer_id"],
        "Doctor Name": df["name"],
        "City": df["city"],
        "Type": df["type"],
        "Latest Period": df["m3_label"] if "m3_label" in df.columns else "",
        "3M Status": df["status_3m"].map(
            {"retained":"🟢 Retained","at_risk":"🟡 At Risk","lost":"🔴 Lost"}).fillna("—"),
        "6M Status": df.get("status_6m", "").map(
            {"retained":"🟢 Retained","lost":"🔴 Lost"}).fillna("—"),
        "12M Status": df.get("status_12m", "").map(
            {"retained":"🟢 Retained","lost":"🔴 Lost"}).fillna("—"),
        "Last Case": df["last_case_date"] if "last_case_date" in df.columns else "",
    })
    st.dataframe(show, width='stretch', hide_index=True, height=600)


# ══════════════════════════════════════════════════════════════════════════════
#  RENDER: SINGLE PERIOD VIEW
# ══════════════════════════════════════════════════════════════════════════════

def render_period(period_row: pd.Series, doctors_df: pd.DataFrame, activity: pd.DataFrame):
    pid = period_row["period_id"]
    rows = doctors_df[doctors_df["period_id"] == pid].copy()
    if rows.empty:
        empty_state("📭", "No doctors in this period")
        return

    # Period header
    sub = f"Tracking: {period_row['m1_label']} → {period_row['m3_label']}"
    st.markdown(f"### {period_row['m3_label']} Retention · {len(rows)} doctors")
    st.caption(sub)

    # Breakdown cards (Overall / New / Returning)
    new_rows = rows[rows["type"].str.lower() == "new"]
    ret_rows = rows[rows["type"].str.lower() == "returning"]
    render_breakdown_cards(rows, new_rows, ret_rows)

    # Stat pill row
    render_stat_pills(rows)

    # Filter pills — HTML anchors with query-param state, semantic colors per Lexie's v3
    qp_key = f"filt_{pid}"
    filt = st.query_params.get(qp_key, "all")
    options = [
        ("all",       "All",       "fb"),
        ("retained",  "Retained",  "fg"),
        ("at_risk",   "At Risk",   "fy"),
        ("lost",      "Lost",      "fr"),
        ("new",       "New",       "fp"),
        ("returning", "Returning", "ft"),
        ("digital",   "Digital",   "fg"),
    ]
    pill_html = '<div class="fbtn-row">'
    for key, label, color_cls in options:
        active = "on" if filt == key else ""
        # Build URL preserving other params, override just this one
        params = dict(st.query_params)
        params[qp_key] = key
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        pill_html += f'<a class="fbtn {color_cls} {active}" href="?{qs}" target="_self">{label}</a>'
    pill_html += "</div>"
    st.markdown(pill_html, unsafe_allow_html=True)

    # Apply filter
    filtered = rows.copy()
    if filt in ("retained", "at_risk", "lost"):
        filtered = filtered[filtered["status_3m"] == filt]
    elif filt == "new":
        filtered = filtered[filtered["type"].str.lower() == "new"]
    elif filt == "returning":
        filtered = filtered[filtered["type"].str.lower() == "returning"]
    elif filt == "digital":
        filtered = filtered[filtered["digital"].astype(str).str.lower().isin(("true","1"))]

    # Doctor table — augment period_row with M1 month/year for the table renderer
    period_dict = {
        "m1_month": MONTH_ABBR.index(period_row["m1_label"].split()[0]),
        "m1_year":  int(period_row["m1_label"].split()[1]),
    }
    render_doctor_table(filtered, period=period_dict, activity=activity,
                        title=f"{period_row['m3_label']} Doctors",
                        subtitle=sub)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN — TABS / NAVIGATION
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("# 🦷 Doctor Retention")

data = load_retention_data()
periods_df = data["periods"]
doctors_df = data["doctors"]
master_df = data["master"]
activity = data["activity"]

if periods_df.empty:
    empty_state("📋", "No retention data yet",
                "Run <code>import_case_history.py</code> + <code>pipeline.py</code> on your PC, "
                "then push <code>cache/latest/</code> to GitHub.")
    st.stop()

# Build tab list: Overview | Master | (per year)
years = sorted(periods_df["year"].unique().tolist())
tab_labels = ["📊 Overview", "⭐ Master"] + [f"📅 {y}" for y in years]
tabs = st.tabs(tab_labels)

with tabs[0]:
    render_overview(periods_df, doctors_df)

with tabs[1]:
    render_master(master_df, doctors_df)

for i, year in enumerate(years, start=2):
    with tabs[i]:
        year_periods = periods_df[periods_df["year"] == year].sort_values("period_id")
        if len(year_periods) == 1:
            render_period(year_periods.iloc[0], doctors_df, activity)
        else:
            month_labels = [p["m3_label"].split()[0] for _, p in year_periods.iterrows()]
            sub_tabs = st.tabs(month_labels)
            for j, (_, prow) in enumerate(year_periods.iterrows()):
                with sub_tabs[j]:
                    render_period(prow, doctors_df, activity)

st.divider()
st.caption("Doctor Retention · Powered by retention.py + pipeline.py · Refreshed nightly at 6 AM")
