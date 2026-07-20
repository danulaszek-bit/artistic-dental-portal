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
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    LAB_TZ = ZoneInfo("America/Chicago")     # lab-local time (handles CST/CDT)
except Exception:
    LAB_TZ = timezone(timedelta(hours=-5))   # fallback: Central Daylight Time

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None                    # page still works without the package

# Highlight the data-freshness badge red once data is older than this.
STALE_AFTER_MIN = 15
# Re-run this page in place on this cadence (keeps it on-screen, no navigation).
AUTO_REFRESH_SEC = 30

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
  .freshness {{
      display: inline-flex; align-items: center; gap: 8px;
      padding: 5px 12px; border-radius: 999px;
      font-size: 0.78rem; font-weight: 600;
      border: 1px solid transparent;
  }}
  .freshness .dot {{ width: 8px; height: 8px; border-radius: 50%; }}
  .freshness.fresh {{ background: rgba(46,204,113,.12); color: #1e8449; border-color: rgba(46,204,113,.35); }}
  .freshness.fresh .dot {{ background: {COLORS['green']}; }}
  .freshness.stale {{ background: rgba(231,76,60,.14); color: #c0392b; border-color: rgba(231,76,60,.5); }}
  .freshness.stale .dot {{ background: {COLORS['red']}; }}
  .freshness.unknown {{ background: rgba(108,122,138,.12); color: {COLORS['muted']}; border-color: rgba(108,122,138,.35); }}
  .freshness.unknown .dot {{ background: {COLORS['muted']}; }}
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


def render_freshness(computed_at):
    """Show how old the pipeline data is; red once older than STALE_AFTER_MIN.

    `computed_at` is the pipeline's stamp (logistics_summary.csv). Naive stamps
    are treated as lab-local (Chicago) so the age is correct even when this app
    runs on Streamlit Cloud (UTC).
    """
    if not computed_at or (isinstance(computed_at, float) and math.isnan(computed_at)):
        st.markdown('<div class="freshness unknown"><span class="dot"></span>'
                    'Data age unknown</div>', unsafe_allow_html=True)
        return
    try:
        dt = datetime.fromisoformat(str(computed_at))
    except ValueError:
        st.markdown('<div class="freshness unknown"><span class="dot"></span>'
                    'Data age unknown</div>', unsafe_allow_html=True)
        return
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LAB_TZ)

    age_min = (datetime.now(timezone.utc) - dt).total_seconds() / 60.0
    stale = age_min > STALE_AFTER_MIN

    if age_min < 1:
        age_txt = "just now"
    elif age_min < 60:
        age_txt = f"{int(round(age_min))} min ago"
    elif age_min < 60 * 24:
        age_txt = f"{age_min / 60:.1f} hr ago"
    else:
        age_txt = f"{int(age_min // (60 * 24))} d ago"

    # %-I (no leading zero) is a Linux extension; fall back on Windows.
    try:
        clock = dt.astimezone(LAB_TZ).strftime("%-I:%M %p")
    except ValueError:
        clock = dt.astimezone(LAB_TZ).strftime("%I:%M %p").lstrip("0")

    cls = "stale" if stale else "fresh"
    label = f"Data updated {age_txt} · {clock}"
    if stale:
        label += f"  ⚠ over {STALE_AFTER_MIN} min old"
    st.markdown(f'<div class="freshness {cls}"><span class="dot"></span>{label}</div>',
                unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_case_product_lines():
    p = LATEST_DIR / "case_product_lines.csv"
    if not p.exists():
        return pd.DataFrame(columns=["Cases_CaseNumber","Products_Type","Products_ProductID"])
    return pd.read_csv(p, dtype={"Cases_CaseNumber": str})


@st.cache_data(ttl=AUTO_REFRESH_SEC)  # re-read data each auto-refresh cycle
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


# ── Auto-refresh in place ─────────────────────────────────────────────────────
# Re-runs this page every AUTO_REFRESH_SEC without navigating away, so a wall
# display stays on the Logistics dashboard and keeps its data + freshness badge
# current hands-free.
if st_autorefresh is not None:
    st_autorefresh(interval=AUTO_REFRESH_SEC * 1000, key="logistics_autorefresh")


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

render_freshness(s.get("computed_at") if not summary.empty else None)

cols = st.columns(5)
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
    # Pull on-time numbers from kpi_gauges.csv (written by pipeline)
    try:
        _g = pd.read_csv(LATEST_DIR / "kpi_gauges.csv").iloc[0].to_dict()
        ot_pct = float(_g.get("on_time_pct", 0) or 0)
        ot_n   = int(_g.get("on_time_cases", 0) or 0)
        ot_win = int(_g.get("on_time_window_days", 90) or 90)
    except Exception:
        ot_pct, ot_n, ot_win = 0.0, 0, 90
    kpi_card("On-Time Ship", f"{ot_pct:.1f}%",
             f"last {ot_win}d · {ot_n:,} cases",
             status="ok" if ot_pct >= 90 else "warn")


# ── Filters ───────────────────────────────────────────────────────────────────
section("Filters")

depts_available = sorted(cases["pseudo_dept"].dropna().unique().tolist())

# Departments that should start UNCHECKED on first load
# (cases not physically in the lab — usually noise when scanning WIP)
DEPTS_DEFAULT_OFF = {"At Doctor"}

# Initialize per-department checkbox state
for d in depts_available:
    key = f"dept_chk_{d}"
    if key not in st.session_state:
        st.session_state[key] = (d not in DEPTS_DEFAULT_OFF)

# Header row: label + All / None convenience buttons
hdr = st.columns([6, 1, 1])
with hdr[0]:
    st.markdown("**Departments** &nbsp;<span style='color:#7d8590;font-size:12px'>"
                "click a checkbox to toggle</span>", unsafe_allow_html=True)
with hdr[1]:
    if st.button("All", use_container_width=True, key="dept_all_btn"):
        for d in depts_available:
            st.session_state[f"dept_chk_{d}"] = True
        st.rerun()
with hdr[2]:
    if st.button("None", use_container_width=True, key="dept_none_btn"):
        for d in depts_available:
            st.session_state[f"dept_chk_{d}"] = False
        st.rerun()

# One checkbox per department in a horizontal row
dept_cols = st.columns(max(len(depts_available), 1))
for i, d in enumerate(depts_available):
    with dept_cols[i]:
        st.checkbox(d, key=f"dept_chk_{d}")
selected_depts = [d for d in depts_available
                  if st.session_state.get(f"dept_chk_{d}", True)]

# Status + behind-only on the next row
f2, f3 = st.columns([4, 1])
with f2:
    statuses_available = sorted(cases["Cases_Status"].dropna().unique().tolist())
    selected_statuses = st.multiselect("Status", statuses_available,
                                       default=statuses_available)
with f3:
    show_behind_only = st.checkbox("Behind only", value=False,
                                    key="behind_only_chk")

view = cases[
    cases["pseudo_dept"].isin(selected_depts) &
    cases["Cases_Status"].isin(selected_statuses)
].copy()
if show_behind_only:
    view = view[view["is_behind"]]


# ── Ship-status helpers (needed by Tab 1) ────────────────────────────────────
def _lab_holidays(year):
    out = []
    for month, day in [(1, 1), (7, 4), (12, 25)]:
        d = pd.Timestamp(year=year, month=month, day=day)
        if d.weekday() == 5: d -= pd.Timedelta(days=1)
        elif d.weekday() == 6: d += pd.Timedelta(days=1)
        out.append(d.normalize())
    d = pd.Timestamp(year=year, month=5, day=31)
    while d.weekday() != 0: d -= pd.Timedelta(days=1)
    out.append(d.normalize())
    d = pd.Timestamp(year=year, month=9, day=1)
    while d.weekday() != 0: d += pd.Timedelta(days=1)
    out.append(d.normalize())
    d = pd.Timestamp(year=year, month=11, day=1)
    while d.weekday() != 3: d += pd.Timedelta(days=1)
    thx = d + pd.Timedelta(days=21)
    out.append(thx.normalize()); out.append((thx + pd.Timedelta(days=1)).normalize())
    return set(out)

def _next_business_day(from_date):
    d = pd.Timestamp(from_date).normalize() + pd.Timedelta(days=1)
    hol = _lab_holidays(d.year) | _lab_holidays(d.year + 1)
    while d.weekday() >= 5 or d in hol:
        d += pd.Timedelta(days=1)
    return d

today   = pd.Timestamp.today().normalize()
next_biz = _next_business_day(today)

def _ship_status(ship):
    if pd.isna(ship): return ""
    d = pd.Timestamp(ship).normalize()
    if d < today:     return "Past Due"
    if d == today:    return "Due Today"
    if d == next_biz: return "Due Next Biz Day"
    return ""

view = view.copy()
if "Cases_ShipDate" in view.columns:
    view["ship_status"] = pd.to_datetime(view["Cases_ShipDate"], errors="coerce").apply(_ship_status)
else:
    view["Cases_ShipDate"] = pd.NaT
    if "Cases_DueDate" in view.columns:
        view["ship_status"] = pd.to_datetime(view["Cases_DueDate"], errors="coerce").apply(_ship_status)
    else:
        view["ship_status"] = ""

# Load on-time data once (used in Tab 4)
try:
    _ot = pd.read_csv(LATEST_DIR / "on_time_ship.csv")
except Exception:
    _ot = pd.DataFrame()


# ── Case dig-in dialog (defined at module level, called inside Tab 1) ─────────
@st.dialog("🦷 Case Detail", width="large")
def show_case_detail(row):
    case_no  = row.get("Cases_CaseNumber", "—")
    doctor   = row.get("Cases_DoctorName", "—") or "—"
    customer = row.get("Cases_CustomerID", "—") or "—"
    pan      = row.get("Cases_PanNumber", "—") or "—"
    status   = row.get("Cases_Status", "—") or "—"
    dept     = row.get("pseudo_dept", "—") or "—"
    loc      = row.get("Cases_LastLocation", "") or "(no station)"
    age      = int(row.get("age_days", 0) or 0)
    at_loc   = int(row.get("days_at_station", 0) or 0)
    overdue  = int(row.get("days_overdue", 0) or 0)
    ship_st  = row.get("ship_status", "") or "On schedule"
    carrier  = row.get("Cases_Carrier", "") or "—"
    notes    = row.get("Cases_Notes",   "") or ""

    def _fmt_date(v):
        d = pd.to_datetime(v, errors="coerce")
        return d.strftime("%a %b %d, %Y") if pd.notna(d) else "—"

    st.markdown(f"### Case #{case_no}  ·  {doctor}")
    st.caption(f"Customer ID {customer}  ·  Pan {pan}  ·  Status {status}  ·  Dept {dept}")
    icon_map = {"Past Due": "🔴", "Due Today": "🟡", "Due Next Biz Day": "🟢"}
    st.markdown(f"**{icon_map.get(ship_st, '🟦')} {ship_st}**")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Schedule**")
        st.write(f"Date In: **{_fmt_date(row.get('Cases_DateIn'))}**")
        st.write(f"Due: **{_fmt_date(row.get('Cases_DueDate'))}**")
        st.write(f"Ship: **{_fmt_date(row.get('Cases_ShipDate'))}**")
        st.write(f"Age in lab: **{age} days**")
        if overdue > 0:
            st.write(f"Days past due: **{overdue}**")
    with c2:
        st.markdown("**Location & Shipping**")
        st.write(f"Last Location: **{loc}**")
        st.write(f"Days at this station: **{at_loc}**")
        st.write(f"Route / Carrier: **{carrier}**")
        flags = []
        if bool(row.get("flag_past_due", False)):        flags.append("Past due")
        if bool(row.get("flag_stuck", False)):            flags.append("Stuck at station")
        if bool(row.get("flag_high_value_aged", False)): flags.append("High-value aged")
        st.write(f"Flags: **{', '.join(flags) if flags else 'none'}**")
    if notes:
        st.divider()
        st.markdown("**Delivery Notes**")
        st.info(notes)


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📋 Case Detail",
    "📊 Dept Breakdown",
    "📍 Top Stuck Locations",
    "✅ On-Time Ship Rate",
    "📈 Aging Waterfall",
])


# ── Tab 1: Case Detail ────────────────────────────────────────────────────────
with tab1:
    ctl = st.columns([1, 1, 1, 2])
    with ctl[0]:
        show_past_due  = st.checkbox("🔴 Past Due (ship)",  value=False, key="filt_past_due")
    with ctl[1]:
        show_due_today = st.checkbox("🟡 Ships Today",      value=False, key="filt_due_today")
    with ctl[2]:
        show_due_next  = st.checkbox(
            f"🟢 Ships Next Biz Day ({next_biz.strftime('%a %b %d')})",
            value=False, key="filt_due_next")
    with ctl[3]:
        case_search = st.text_input("🔍 Search by Case #", value="", key="case_search",
                                     placeholder="e.g. 448962")

    _all_routes = sorted(view["Cases_Carrier"].fillna("").str.strip().unique().tolist())
    _all_routes = [r for r in _all_routes if r]
    selected_routes = st.multiselect(
        "🚚 Filter by Route",
        options=_all_routes,
        default=[],
        placeholder="All routes shown — select one or more to filter",
        key="filt_routes",
    )

    active_buckets = []
    if show_past_due:  active_buckets.append("Past Due")
    if show_due_today: active_buckets.append("Due Today")
    if show_due_next:  active_buckets.append("Due Next Biz Day")

    table_view = view.copy()
    if active_buckets:
        table_view = table_view[table_view["ship_status"].isin(active_buckets)]
    if selected_routes:
        table_view = table_view[table_view["Cases_Carrier"].isin(selected_routes)]
    if case_search.strip():
        q = case_search.strip()
        table_view = table_view[
            table_view["Cases_CaseNumber"].fillna("").astype(str).str.contains(q, case=False, regex=False)
        ]

    section(f"Case detail ({len(table_view)} shown of {len(view)})")

    display_cols = [
        ("Cases_CaseNumber",  "Case #"),
        ("ship_status",       "Ship Status"),
        ("Cases_DoctorName",  "Doctor"),
        ("Cases_PanNumber",   "Pan#"),
        ("pseudo_dept",       "Dept"),
        ("Cases_Status",      "Status"),
        ("Cases_LastLocation","Location"),
        ("Cases_DateIn",      "Date In"),
        ("Cases_DueDate",     "Due"),
        ("Cases_ShipDate",    "Ship"),
        ("age_days",          "Age"),
        ("days_at_station",   "At Loc"),
        ("days_overdue",      "Days Late"),
        ("Cases_Carrier",     "Route"),
        ("is_behind",         "Behind"),
    ]
    cols_present = [(c, n) for c, n in display_cols if c in table_view.columns]
    view_display = table_view[[c for c, _ in cols_present]].copy()
    view_display.columns = [n for _, n in cols_present]

    # Sort: ship date ascending, then directional routes before UPS/GL
    _sort_df = pd.DataFrame(index=table_view.index)
    _sort_df["_ship"] = pd.to_datetime(table_view.get("Cases_ShipDate"), errors="coerce")
    _carrier_str = table_view.get("Cases_Carrier", pd.Series("", index=table_view.index)).fillna("").str.upper()
    _sort_df["_carrier_tier"] = _carrier_str.str.contains(r"\bUPS\b|^GL\b", regex=True).astype(int)
    sort_order   = _sort_df.sort_values(["_ship", "_carrier_tier"], ascending=[True, True]).index
    view_display = view_display.loc[sort_order]
    table_view   = table_view.loc[sort_order]
    view_display = view_display.reset_index(drop=True)
    table_view   = table_view.reset_index(drop=True)

    for dcol in ("Date In", "Due", "Ship"):
        if dcol in view_display.columns:
            view_display[dcol] = pd.to_datetime(view_display[dcol], errors="coerce").dt.strftime("%m/%d/%y")
    if "Ship Status" in view_display.columns:
        _icons = {"Past Due": "🔴", "Due Today": "🟡", "Due Next Biz Day": "🟢"}
        view_display["Ship Status"] = view_display["Ship Status"].apply(
            lambda s: f"{_icons.get(s,'')} {s}".strip() if s else ""
        )

    _col_widths = {
        "Case #": "small", "Ship Status": "medium", "Doctor": "medium",
        "Pan#": "small", "Dept": "small", "Status": "small", "Location": "medium",
        "Date In": "small", "Due": "small", "Ship": "small",
        "Age": "small", "At Loc": "small", "Days Late": "small",
        "Route": "small", "Behind": "small",
    }
    column_config = {
        name: st.column_config.Column(width=w)
        for name, w in _col_widths.items() if name in view_display.columns
    }

    selection = st.dataframe(
        view_display,
        use_container_width=True, height=500, hide_index=True,
        column_config=column_config,
        selection_mode="single-row",
        on_select="rerun",
        key="case_detail_table",
    )

    _sel = (selection.selection.rows
            if selection is not None and hasattr(selection, "selection") else [])
    if _sel:
        # Use case number from view_display (not raw row index) to guard against
        # stale session-state index pointing at the wrong row after a sort change.
        _row_pos = _sel[0]
        if _row_pos < len(view_display) and "Case #" in view_display.columns:
            _case_num = str(view_display.iloc[_row_pos]["Case #"]).strip()
            _match = table_view[table_view["Cases_CaseNumber"].astype(str).str.strip() == _case_num]
            _detail_row = _match.iloc[0] if not _match.empty else table_view.iloc[_row_pos]
        else:
            _detail_row = table_view.iloc[_row_pos]
        show_case_detail(_detail_row)

    csv_bytes = table_view.to_csv(index=False).encode("utf-8")
    st.download_button("⬇ Download filtered case list (CSV)",
                       data=csv_bytes,
                       file_name=f"logistics_cases_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                       mime="text/csv")


# ── Tab 2: Dept Breakdown ─────────────────────────────────────────────────────
with tab2:
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
                    colorbar=dict(title="% behind", thickness=10, ticksuffix="%", tickformat=".0%"),
                ),
                text=dept_summary.apply(
                    lambda r: f"  {int(r['open_count'])} ({int(r['behind'])} behind)", axis=1),
                textposition="outside",
            ))
            fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                              yaxis=dict(autorange="reversed"),
                              margin=dict(l=10, r=40, t=20, b=10),
                              height=320, xaxis_title="Open cases")
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        if not dept_summary.empty:
            disp = dept_summary.copy()
            disp["value_at_risk"] = disp["value_at_risk"].apply(fmt_currency)
            disp["median_age"] = disp["median_age"].apply(lambda v: f"{int(v) if pd.notna(v) else 0}d")
            disp = disp.rename(columns={
                "pseudo_dept": "Department", "open_count": "Open",
                "overdue": "Past Due", "stuck": "Stuck", "behind": "Behind",
                "median_age": "Median Age", "value_at_risk": "$ at Risk",
            })
            st.dataframe(disp, use_container_width=True, height=320, hide_index=True)


# ── Tab 3: Top Stuck Locations ────────────────────────────────────────────────
with tab3:
    behind_df = view[view["is_behind"]]
    if not behind_df.empty:
        loc_summary = (
            behind_df.groupby("Cases_LastLocation")
            .agg(cases=("Cases_CaseNumber", "count"),
                 avg_days_late=("days_overdue", "mean"),
                 max_days_late=("days_overdue", "max"),
                 avg_days_at_station=("days_at_station", "mean"),
                 value=("Cases_TotalCharge", "sum"))
            .reset_index()
            .sort_values("cases", ascending=False)
            .head(20)
        )
        disp = loc_summary.copy()
        disp["avg_days_late"]       = disp["avg_days_late"].round(1)
        disp["avg_days_at_station"] = disp["avg_days_at_station"].round(1)
        disp["max_days_late"]       = disp["max_days_late"].astype(int)
        disp["value"]               = disp["value"].apply(fmt_currency)
        disp = disp.rename(columns={
            "Cases_LastLocation": "Location", "cases": "Behind",
            "avg_days_late": "Avg Days Late", "max_days_late": "Max Days Late",
            "avg_days_at_station": "Avg Days at Station", "value": "Value",
        })
        st.dataframe(disp, use_container_width=True, hide_index=True)
    else:
        st.success("No cases flagged behind in the current filter.")


# ── Tab 4: On-Time Ship Rate ──────────────────────────────────────────────────
with tab4:
    if not _ot.empty:
        c_chart, c_table = st.columns([3, 2])
        with c_chart:
            fig = go.Figure()
            fig.add_bar(
                x=_ot["month"], y=_ot["on_time_pct"],
                marker_color=[COLORS["green"] if v >= 90 else COLORS["gold"] if v >= 80 else COLORS["red"]
                              for v in _ot["on_time_pct"]],
                text=_ot["on_time_pct"].apply(lambda v: f"{v:.1f}%"),
                textposition="outside")
            fig.update_layout(yaxis=dict(range=[0, 105], title="On-time %"),
                              xaxis_title="",
                              margin=dict(l=10, r=10, t=10, b=10),
                              height=300,
                              plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)
        with c_table:
            show_ot = _ot.copy()
            show_ot["On-Time"]   = show_ot["on_time"].astype(int)
            show_ot["Late"]      = show_ot["late"].astype(int)
            show_ot["Cases"]     = show_ot["cases"].astype(int)
            show_ot["On-Time %"] = show_ot["on_time_pct"].apply(lambda v: f"{v:.1f}%")
            st.dataframe(show_ot[["month", "Cases", "On-Time", "Late", "On-Time %"]]
                           .rename(columns={"month": "Month"}),
                         hide_index=True, use_container_width=True, height=300)
    else:
        st.info("On-time ship data not yet available — run the pipeline to generate it.")


# ── Tab 5: Aging Waterfall ────────────────────────────────────────────────────
with tab5:
    def _bucket(age):
        if age <= 3:  return "0-3d"
        if age <= 7:  return "4-7d"
        if age <= 14: return "8-14d"
        if age <= 30: return "15-30d"
        return "30+d"

    if not view.empty:
        wf = view.copy()
        wf["bucket"] = wf["age_days"].apply(_bucket)
        bucket_order = ["0-3d", "4-7d", "8-14d", "15-30d", "30+d"]
        pivot = (
            wf.groupby(["bucket", "pseudo_dept"])
            .size().reset_index(name="count")
        )
        pivot["bucket"] = pd.Categorical(pivot["bucket"], categories=bucket_order, ordered=True)
        pivot = pivot.sort_values("bucket")
        fig = px.bar(pivot, x="bucket", y="count", color="pseudo_dept",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                          margin=dict(l=10, r=10, t=20, b=10),
                          height=400,
                          legend=dict(orientation="h", y=-0.2),
                          xaxis_title="Age in lab", yaxis_title="Cases",
                          barmode="stack")
        st.plotly_chart(fig, use_container_width=True)


# ── Footer (outside tabs) ─────────────────────────────────────────────────────
st.divider()
if not summary.empty and "computed_at" in summary.columns:
    st.caption(f"Last pipeline run: **{summary.iloc[0]['computed_at']}** · "
               f"Page refreshes every 10 min unless you click the Refresh button.")
