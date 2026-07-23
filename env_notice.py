"""
env_notice.py
=============
Helpers for the manager tools (roster / goals / PTO / scheduling), which are
LAN-only by design: they need the read-write SQLite DB and the MagicTouch
source folder, neither of which exists on the public Streamlit Cloud copy.

`manager_tools_available()` is True only on the lab PC (source folder present).
On the cloud copy the manager pages call `render_lan_notice()` and stop,
pointing users to the lab network instead of showing a misleading
"run the pipeline" error.
"""
from __future__ import annotations

import socket
from pathlib import Path

import streamlit as st

try:
    import yaml
    _CFG = yaml.safe_load((Path(__file__).parent / "config.yaml").read_text())
    MT_FOLDER = Path(_CFG["data_source"]["csv"]["watch_folder"])
except Exception:
    MT_FOLDER = Path("C:/MT_Reports_Local")


def manager_tools_available() -> bool:
    """True on the lab PC (MagicTouch source folder present) — the only place
    the read-write manager tools are meant to run."""
    return MT_FOLDER.exists()


def lab_lan_url(port: int = 8501) -> str:
    """Best-effort LAN URL of the machine actually running this app."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))          # no packets sent; just picks the LAN NIC
        ip = s.getsockname()[0]
        s.close()
        return f"http://{ip}:{port}"
    except Exception:
        return f"http://<lab-pc-ip>:{port}"


def render_lan_notice(tool_name: str = "manager tools") -> None:
    """Friendly stop-notice shown when the page is opened on the cloud copy."""
    st.info(
        f"🔒 **The {tool_name} run on the lab network, not this public app.**\n\n"
        "Roster, goals, PTO, scheduling and pay live in a read-write database on "
        "the lab PC — that data (and pay especially) never leaves the building.\n\n"
        "Open the portal from any device **connected to the lab network**, using the "
        "lab PC's address on port **8501** (ask whoever set it up for the exact "
        "`http://…:8501` address). This cloud copy is for the GM Summary and the "
        "business-intelligence pages only."
    )
    st.stop()
