"""
materials_calc.py
==================
Materials cost views for the manager dashboards, from the Clixon issued-
materials export (Issued (29).csv). Read live in-page from MT_Reports_Local —
like labor, computed figures never reach committed files.

Clixon reports department by material category, not the dashboard's Fixed/
Removable split — CLIXON_MAP applies the allocation Danny specified during
design (Ingots & Discs → Fixed, Implants/Attachments → Removable, etc.).
Distribution / General Supplies / Unassigned are excluded from department
KPIs (packaging + catch-alls, not production materials).

Employee attribution: Clixon has no employee code, only a free-text
"Requested By" name ("John O'Hale"). We match against the roster's
"Last, First" names; unmatched requesters still count toward the department
totals and appear under their raw name in the by-employee view.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

import goals_store
from mt_reports_parser import load_materials_issued

# Clixon "Issued Dept." → (dashboard, area). Absent keys are excluded.
CLIXON_MAP = {
    "CAD/CAM":                  ("Fixed", "CAD/CAM"),
    "Ceramics":                 ("Fixed", "Ceramics"),
    "Crown and Bridge":         ("Fixed", "Crown & Bridge"),
    "Ingots and Discs":         ("Fixed", "Ingots & Discs"),
    "Removable":                ("Removable", "Removables"),
    "Chairside":                ("Removable", "Chairside"),
    "Implants and Attachments": ("Removable", "Implants & Attachments"),
    "Model and Die":            ("GM", "Model/Die"),
    # Excluded: Distribution, General Supplies, Unassigned
}


def _name_key(s: str) -> tuple[str, str] | None:
    """('first','last') key from either 'Last, First (X)' or 'First Last'."""
    s = re.sub(r"\(.*?\)", "", str(s or "")).strip()
    if not s:
        return None
    if "," in s:
        last, first = s.split(",", 1)
        first = first.strip().split()
        return (first[0].lower(), last.strip().split()[-1].lower()) if first else None
    parts = s.split()
    if len(parts) < 2:
        return None
    return (parts[0].lower(), parts[-1].lower())


def load_materials(mt_folder: Path, dashboard: str) -> pd.DataFrame:
    """
    Issued-materials rows for one dashboard, with columns:
    issue_date, area, requested_by, matched_name (roster name or raw
    requester), tech_code (or ''), qty, issued_value.
    Empty DataFrame when the Clixon export isn't present (e.g. cloud).
    """
    df = load_materials_issued(mt_folder)
    if df.empty:
        return pd.DataFrame()

    df["bucket"] = df["department"].map(CLIXON_MAP)
    df = df.dropna(subset=["bucket"]).copy()
    df["dashboard"] = df["bucket"].apply(lambda b: b[0])
    df["area"] = df["bucket"].apply(lambda b: b[1])
    df = df[df["dashboard"] == dashboard].copy()
    if df.empty:
        return pd.DataFrame()

    roster = goals_store.list_technicians(active_only=True)
    by_key = {}
    for t in roster:
        k = _name_key(t["name"])
        if k:
            by_key[k] = t

    def _match(requested_by: str):
        k = _name_key(requested_by)
        t = by_key.get(k) if k else None
        if t:
            return pd.Series([t["name"], t["tech_code"]])
        return pd.Series([str(requested_by).strip() or "(unattributed)", ""])

    df[["matched_name", "tech_code"]] = df["requested_by"].apply(_match)
    return df[["issue_date", "area", "requested_by", "matched_name",
               "tech_code", "qty", "issued_value"]].reset_index(drop=True)
