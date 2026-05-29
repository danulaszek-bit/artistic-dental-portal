"""
Artistic Dental Studio — Production Manager Dashboard
=====================================================
Hosts the self-contained Production Dashboard (assets/production_dashboard.html)
inside the portal as a full-bleed embedded page.

The HTML is a standalone client-side app: the manager uploads (or drag-drops)
Magic Touch reports — Production/Units In, Technician Performance, Employee
Productivity, Remake Report — and JavaScript parses them in-browser. A built-in
"Load your data (sample)" button renders a full demo view with no upload.

Step 1 of integration: embed as-is so it's live in the portal.
Later steps will pre-feed the reports we can derive from the existing pipeline
(Units In + Remakes from AllCasesByDateIn) so fewer manual uploads are needed.
"""

from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

BASE_DIR = Path(__file__).parent.parent
HTML_PATH = BASE_DIR / "assets" / "production_dashboard.html"

st.set_page_config(
    page_title="Production Manager — Artistic Dental",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Strip Streamlit's default padding so the embedded app renders edge-to-edge,
# matching how it looks standalone.
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

# Size the iframe to roughly one screen height. The embedded app's upload
# screen is a position:fixed overlay that centers within the iframe's own
# viewport — so the iframe must be ~screen-height for it to appear where the
# user can see it (a too-tall iframe pushes the overlay far down the page).
# The dashboard's own content scrolls inside the frame (scrolling=True), and
# it has a sticky topbar that stays pinned.
components.html(html, height=900, scrolling=True)
