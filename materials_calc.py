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
    by_code = {t["tech_code"]: t for t in roster}
    by_key = {}
    for t in roster:
        k = _name_key(t["name"])
        if k:
            by_key[k] = t
    aliases = goals_store.get_requester_aliases()   # {raw_name: tech_code}

    def _match(requested_by: str):
        raw = str(requested_by).strip()
        # 1. Explicit manager-set alias wins.
        t = by_code.get(aliases.get(raw)) if raw in aliases else None
        # 2. Fall back to (first, last) name-key match against the roster.
        if not t:
            k = _name_key(raw)
            t = by_key.get(k) if k else None
        if t:
            return pd.Series([t["name"], t["tech_code"]])
        return pd.Series([raw or "(unattributed)", ""])

    df[["matched_name", "tech_code"]] = df["requested_by"].apply(_match)
    return df[["issue_date", "area", "requested_by", "matched_name",
               "tech_code", "qty", "issued_value"]].reset_index(drop=True)


def dashboard_materials(dashboard: str) -> pd.DataFrame:
    """
    Materials for the dashboard's Labor & Materials section, read from the
    accumulated local history (materials_history) rather than the current raw
    file — so the dashboard shows full history, not just the latest export's
    window. Columns match load_materials for drop-in use: issue_date
    (datetime), area, requested_by, matched_name, tech_code, qty, issued_value.
    """
    rows = goals_store.get_materials_history(dashboard=dashboard)
    if not rows:
        return pd.DataFrame(columns=["issue_date", "area", "requested_by",
                                     "matched_name", "tech_code", "qty", "issued_value"])
    df = pd.DataFrame(rows).rename(columns={"dollars": "issued_value"})
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    return df[["issue_date", "area", "requested_by", "matched_name",
               "tech_code", "qty", "issued_value"]]


def unmatched_requesters(mt_folder: Path) -> list[str]:
    """Distinct Clixon requester names from the current export that don't map
    to a roster tech (across both dashboards) — surfaced in the alias editor."""
    out: set[str] = set()
    for dash in ("Fixed", "Removable"):
        m = load_materials(mt_folder, dash)
        if not m.empty:
            out |= set(m[m["tech_code"] == ""]["requested_by"].str.strip())
    out.discard("")
    return sorted(out)


def persist_materials(mt_folder: Path) -> tuple[int, int]:
    """
    Ingest the current Clixon export into materials_history using replace-by-
    week (the newest week-to-date file is the most complete for its week).
    Returns (weeks_written, rows_written). Local-only — never a committed file.
    """
    import pandas as _pd

    frames = []
    for d in ("Fixed", "Removable"):
        f = load_materials(mt_folder, d)
        if not f.empty:
            f = f.copy()
            f["dashboard"] = d
            frames.append(f)
    if not frames:
        return 0, 0
    m = _pd.concat(frames, ignore_index=True)
    m["issue_date"] = _pd.to_datetime(m["issue_date"], errors="coerce")
    m = m.dropna(subset=["issue_date"])
    if m.empty:
        return 0, 0

    # Monday of each row's week
    m["week_start"] = (m["issue_date"] - _pd.to_timedelta(m["issue_date"].dt.weekday, unit="D")).dt.date

    weeks, total = 0, 0
    for wk, grp in m.groupby("week_start"):
        rows = [{
            "issue_date":   r["issue_date"].date().isoformat(),
            "dashboard":    r["dashboard"] if "dashboard" in grp.columns else "",
            "area":         r["area"],
            "tech_code":    r["tech_code"],
            "matched_name": r["matched_name"],
            "requested_by": r["requested_by"],
            "qty":          float(r["qty"]),
            "dollars":      float(r["issued_value"]),
        } for _, r in grp.iterrows()]
        total += goals_store.replace_materials_week(wk.isoformat(), rows)
        weeks += 1
    return weeks, total
