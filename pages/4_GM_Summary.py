"""
Artistic Dental Studio — GM Summary
====================================
Serves the pre-baked live HTML (written by production_pipeline.py's
build_gm_summary_html(), reusing Danny's original hand-tuned dashboard
template — assets/production_dashboard.html, same file as
_scar_dash_review/artistic-dental-dashboard_38.html). Falls back to the
unbaked template if the live file doesn't exist yet.

GM Summary is read-only, so it doesn't need the Streamlit-native rebuild the
Fixed Dashboard requires for editable goals/PTO — this restores the richer
original rendering for this page specifically.
"""
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

BASE_DIR  = Path(__file__).parent.parent
LIVE_PATH = BASE_DIR / "assets" / "production_dashboard_live.html"
HTML_PATH = BASE_DIR / "assets" / "production_dashboard.html"

st.set_page_config(
    page_title="GM Summary — Artistic Dental",
    page_icon="📋",
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
    st.error(f"GM Summary dashboard not found at {serve}. Run `py production_pipeline.py` first.")
    st.stop()

components.html(serve.read_text(encoding="utf-8"), height=1400, scrolling=True)
