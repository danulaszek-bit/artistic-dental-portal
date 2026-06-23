"""
Artistic Dental Studio — Production Manager Dashboard
"""
import json
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

BASE_DIR  = Path(__file__).parent.parent
HTML_PATH = BASE_DIR / "assets" / "production_dashboard.html"
DATA_PATH = BASE_DIR / "cache" / "latest" / "production_data.json"

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

if not HTML_PATH.exists():
    st.error(f"Production dashboard asset not found at {HTML_PATH}.")
    st.stop()

html = HTML_PATH.read_text(encoding="utf-8")

preloaded = False
if DATA_PATH.exists():
    try:
        data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = None

    if data:
        payload = json.dumps(data)
        inject = "<script>\n(function preload() {\n"
        inject += "  if (typeof S === 'undefined' || typeof buildDashboard !== 'function') {\n"
        inject += "    return setTimeout(preload, 40);\n  }\n  try {\n"
        inject += "    const D = " + payload + ";\n"
        inject += "    S.depts = D.depts || {};\n"
        inject += "    S.techs = D.techs || [];\n"
        inject += "    S.reasons = D.reasons || [];\n"
        inject += "    S.period = D.period || '';\n"
        inject += "    S.daily = D.daily || [];\n"
        inject += "    S.weekly = D.weekly || [];\n"
        inject += "    S.monthly = D.monthly || [];\n"
        inject += "    S.deptWeekly = D.deptWeekly || [];\n"
        inject += "    S.deptMonthly = D.deptMonthly || [];\n"
        inject += "    closeUpload();\n"
        inject += "  } catch (e) { console.error('Production preload failed:', e); }\n"
        inject += "})();\n</script>"
        html = html.replace("</body>", inject + "\n</body>", 1)
        preloaded = True

components.html(html, height=900, scrolling=True)
