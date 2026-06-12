"""
Artistic Dental Studio — Production Manager Dashboard
=====================================================
Hosts the self-contained Production Dashboard (assets/production_dashboard.html)
inside the portal as a full-bleed embedded page.

If cache/latest/production_data.json exists (produced by production_pipeline.py
from the Magic Touch reports in live_exports/), this page injects that data into
the dashboard's global state and skips the upload screen entirely — the manager
lands straight on the data.

If the JSON is missing, the page falls back to the original behaviour: the
upload modal, where the manager drops reports manually.
"""

import json
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

BASE_DIR = Path(__file__).parent.parent
HTML_PATH = BASE_DIR / "assets" / "production_dashboard.html"
DATA_PATH = BASE_DIR / "cache" / "latest" / "production_data.json"

st.set_page_config(
    page_title="Production Manager — Artistic Dental",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Strip Streamlit's default padding so the embedded app renders edge-to-edge.
st.markdown(
    """
    <style>
      .block-container { padding: 0 !important; max-width: 100% !important; }
      header[data-testid="stHeader"] { background: transparent; }
      #MainMenu, footer { visibility: hidden; }
      [data-testid="stSidebarNav"] li a { font-size: 14px; }
    </style>
    """,
    unsafe_allow_html=True,
)

if not HTML_PATH.exists():
    st.error(
        f"Production dashboard asset not found at {HTML_PATH}. "
        "Make sure assets/production_dashboard.html is present."
    )
    st.stop()

html = HTML_PATH.read_text(encoding="utf-8")

# If pre-computed data exists, inject it into the dashboard's global `S` object
# and call its own closeUpload() (which hides the modal and builds the views).
preloaded = False
if DATA_PATH.exists():
    data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    payload = json.dumps(data)
    inject = f"""
<script>
(function preload() {{
  if (typeof S === 'undefined' || typeof buildDashboard !== 'function') {{
    return setTimeout(preload, 40);   // wait for the main script to define them
  }}
  try {{
    const D = {payload};
    S.depts = D.depts || {{}};
    S.techs = D.techs || [];
    S.reasons = D.reasons || [];
    S.period = D.period || '';
    S.daily = D.daily || [];
    S.weekly = D.weekly || [];
    S.monthly = D.monthly || [];
    S.deptWeekly = D.deptWeekly || [];
    S.deptMonthly = D.deptMonthly || [];
    closeUpload();   // hides the upload overlay + runs buildDashboard()
  }} catch (e) {{
    console.error('Production preload failed:', e);
  }}
}})();
</script>
"""
    html = html.replace("</body>", inject + "\n</body>", 1)
    preloaded = True

components.html(html, height=900, scrolling=True)
