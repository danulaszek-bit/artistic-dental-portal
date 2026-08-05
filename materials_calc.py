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
    "Cad/Cam Implant and Attachment": ("Fixed", "CAD/CAM"),   # historical dept — Danny: Fixed/CAD-CAM
    "Ceramics":                 ("Fixed", "Ceramics"),
    "Crown and Bridge":         ("Fixed", "Crown & Bridge"),
    "Ingots and Discs":         ("Fixed", "Ingots & Discs"),
    "Removable":                ("Removable", "Removables"),
    "Removable Supplies":       ("Removable", "Removables"),  # historical dept
    "Chairside":                ("Removable", "Chairside"),
    "Implants and Attachments": ("Removable", "Implants & Attachments"),
    "Model and Die":            ("GM", "Model/Die"),
    # Excluded (overhead / catch-all): Distribution, General Supplies,
    # Unassigned, Repair and Maintenance.
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


DEPT_GENERAL = "Department (general)"


def map_and_match(df: pd.DataFrame, dashboard: str) -> pd.DataFrame:
    """
    Map a raw parsed Issued frame (from load_materials_issued or
    parse_issued_file) to one dashboard: apply CLIXON_MAP dept→area, filter to
    the dashboard, and attribute each row to a technician.

    Attribution rule (per Danny): a material is credited to a person ONLY when
    the requester name matches a CURRENT ACTIVE-ROSTER technician (the roster
    is rebuilt each run from EmployeeProductivity, so departed / long-inactive
    staff automatically fall off). Everything else — unknown names, purchasing
    staff, or past employees in the historical backfill — rolls into the
    department-general bucket. No aliases: department-level history is the
    goal, not comparisons to past employees.

    Columns: issue_date, area, requested_by, matched_name, tech_code, qty,
    issued_value. Unattributed rows get matched_name = DEPT_GENERAL, tech_code "".
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["bucket"] = df["department"].map(CLIXON_MAP)
    df = df.dropna(subset=["bucket"]).copy()
    df["dashboard"] = df["bucket"].apply(lambda b: b[0])
    df["area"] = df["bucket"].apply(lambda b: b[1])
    df = df[df["dashboard"] == dashboard].copy()
    if df.empty:
        return pd.DataFrame()

    by_key = {}
    for t in goals_store.list_technicians(active_only=True):
        k = _name_key(t["name"])
        if k:
            by_key[k] = t

    def _match(requested_by: str):
        k = _name_key(str(requested_by).strip())
        t = by_key.get(k) if k else None
        if t:
            return pd.Series([t["name"], t["tech_code"]])
        return pd.Series([DEPT_GENERAL, ""])   # unmatched / departed → dept general

    df[["matched_name", "tech_code"]] = df["requested_by"].apply(_match)
    return df[["issue_date", "area", "requested_by", "matched_name",
               "tech_code", "qty", "issued_value"]].reset_index(drop=True)


def load_materials(mt_folder: Path, dashboard: str) -> pd.DataFrame:
    """Freshest rolling Clixon export, mapped/matched for one dashboard."""
    return map_and_match(load_materials_issued(mt_folder), dashboard)


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


def _persist_mapped(frames: list) -> tuple[int, int]:
    """
    Replace-by-DAY ingest from already-mapped per-dashboard frames: each source
    overwrites only the exact days it contains (see replace_materials_dates).
    Returns (distinct_days, rows_written).
    """
    import pandas as _pd
    frames = [f for f in frames if not f.empty]
    if not frames:
        return 0, 0
    m = _pd.concat(frames, ignore_index=True)
    m["issue_date"] = _pd.to_datetime(m["issue_date"], errors="coerce")
    m = m.dropna(subset=["issue_date"])
    if m.empty:
        return 0, 0

    m["week_start"] = (m["issue_date"] - _pd.to_timedelta(m["issue_date"].dt.weekday, unit="D")).dt.date

    rows = [{
        "issue_date":   r["issue_date"].date().isoformat(),
        "week_start":   r["week_start"].isoformat(),
        "dashboard":    r["dashboard"],
        "area":         r["area"],
        "tech_code":    r["tech_code"],
        "matched_name": r["matched_name"],
        "requested_by": r["requested_by"],
        "qty":          float(r["qty"]),
        "dollars":      float(r["issued_value"]),
    } for _, r in m.iterrows()]
    n_days = m["issue_date"].dt.date.nunique()
    total = goals_store.replace_materials_dates(rows)
    return n_days, total


def _mapped_frames(raw_df: pd.DataFrame) -> list:
    out = []
    for d in ("Fixed", "Removable"):
        f = map_and_match(raw_df, d)
        if not f.empty:
            f = f.copy()
            f["dashboard"] = d
            out.append(f)
    return out


def persist_materials(mt_folder: Path) -> tuple[int, int]:
    """
    Ingest the current (freshest) Clixon export into materials_history using
    replace-by-week (the newest week-to-date file is the most complete for its
    week). Returns (weeks_written, rows_written). Local-only.
    """
    return _persist_mapped(_mapped_frames(load_materials_issued(mt_folder)))


def persist_materials_from_file(path: Path) -> tuple[int, int]:
    """
    One-time history backfill: ingest a SPECIFIC Clixon Issued file (any
    covered span) via the same replace-by-week logic. Used by
    ingest_materials_history.py, independent of the rolling freshest-file flow.
    """
    from mt_reports_parser import parse_issued_file
    return _persist_mapped(_mapped_frames(parse_issued_file(path)))
