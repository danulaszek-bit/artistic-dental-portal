"""
manager_theme.py
=================
Shared dark-theme constants + small HTML helpers for the new GM Summary and
Fixed Dashboard pages, so the two are visually consistent with each other.
Roughly matches dashboard.py's landing-page palette (the entry point most
people will see first) rather than the two other, divergent palettes already
in use on the Executive Dashboard and Logistics pages — reconciling those is
out of scope for this pass.
"""

COLORS = {
    "bg":   "#0d1117",
    "sfc":  "#161b22",
    "sfc2": "#1c2128",
    "bdr":  "#30363d",
    "txt":  "#e6edf3",
    "txt2": "#7d8590",
    "acc":  "#58a6ff",
    "good": "#3fb950",
    "warn": "#d29922",
    "bad":  "#f85149",
    "pur":  "#a371f7",
}

BASE_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@500;700;800&family=DM+Sans:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp {{
    background-color: {COLORS['bg']} !important;
    color: {COLORS['txt']};
    font-family: 'DM Sans', sans-serif;
}}
h1, h2, h3 {{ font-family: 'Syne', sans-serif !important; color: {COLORS['txt']} !important; }}
.block-container {{ padding-top: 1.2rem; max-width: 1240px; }}
#MainMenu, footer {{ visibility: hidden; }}

.mgr-tile {{
    background: {COLORS['sfc']}; border: 1px solid {COLORS['bdr']}; border-radius: 12px;
    padding: 16px 18px;
}}
.mgr-tile .lbl {{ font-size: 11px; text-transform: uppercase; letter-spacing: .04em; color: {COLORS['txt2']}; margin-bottom: 6px; }}
.mgr-tile .val {{ font-family: 'Syne', sans-serif; font-size: 28px; font-weight: 700; line-height: 1; }}
.mgr-tile .foot {{ font-size: 12px; color: {COLORS['txt2']}; margin-top: 6px; }}

.mgr-card {{
    background: {COLORS['sfc']}; border: 1px solid {COLORS['bdr']}; border-radius: 12px;
    padding: 18px 20px;
}}
.mgr-card .name {{ font-family: 'Syne', sans-serif; font-weight: 700; font-size: 16px; margin-bottom: 2px; }}
.mgr-card .sub {{ font-size: 12px; color: {COLORS['txt2']}; margin-bottom: 12px; }}

.mgr-meter-track {{ height: 8px; border-radius: 4px; background: {COLORS['bdr']}; overflow: hidden; }}
.mgr-meter-fill {{ height: 100%; border-radius: 4px; }}

.mgr-pill {{
    display: inline-block; padding: 2px 9px; border-radius: 999px; font-weight: 700; font-size: 12px;
}}
.mgr-badge {{
    font-size: 11px; font-weight: 700; letter-spacing: .04em; text-transform: uppercase;
    padding: 3px 8px; border-radius: 999px; background: {COLORS['acc']}; color: white;
}}
</style>
"""


def status_color(pct: float, good: float = 95, warn: float = 80) -> str:
    if pct >= good:
        return COLORS["good"]
    if pct >= warn:
        return COLORS["warn"]
    return COLORS["bad"]


def tile_html(label: str, value: str, foot: str = "", color: str | None = None) -> str:
    style = f"color:{color};" if color else ""
    return f"""
    <div class="mgr-tile">
      <div class="lbl">{label}</div>
      <div class="val" style="{style}">{value}</div>
      <div class="foot">{foot}</div>
    </div>
    """


def meter_html(pct: float, good: float = 95, warn: float = 80) -> str:
    pct_clamped = max(0, min(100, pct))
    color = status_color(pct, good, warn)
    return (
        f'<div class="mgr-meter-track">'
        f'<div class="mgr-meter-fill" style="width:{pct_clamped}%;background:{color}"></div>'
        f"</div>"
    )
