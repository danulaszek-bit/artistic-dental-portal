"""
Artistic Dental Studio — Production Manager Dashboard
Serves pre-baked live HTML (written by production_pipeline.py).
Falls back to the template if the live file doesn't exist yet.
"""
from pathlib import Path
import streamlit as st
import streamlit.components.v1 as components

BASE_DIR   = Path(__file__).parent.parent
LIVE_PATH  = BASE_DIR / "assets" / "production_dashboard_live.html"
HTML_PATH  = BASE_DIR / "assets" / "production_dashboard.html"

st.set_page_config(
    page_title="Production Manager — Artistic Dental",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  .block-container { padding: 0 !important; max-width: 100% !important; }
  header[data-testid="stHeader"] { background: transparent; }
  #MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

serve = LIVE_PATH if LIVE_PATH.exists() else HTML_PATH

if not serve.exists():
    st.error(f"Production dashboard not found at {serve}.")
    st.stop()

components.html(serve.read_text(encoding="utf-8"), height=900, scrolling=True)
