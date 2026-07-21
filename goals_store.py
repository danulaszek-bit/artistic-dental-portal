"""
goals_store.py
===============
SQLite-backed persistence for technician goals and PTO — the first
durable, user-editable data in this app (everything else is regenerated
fresh from Magic Touch exports on every pipeline run).

DB lives at data/manager_portal.db (sibling to cache/, NOT inside
cache/latest/) so it is never swept into git_push.bat's auto-commit of
cache/latest/, and is listed in .gitignore.

Goals are append-only and effective-dated: setting a new goal inserts a
new row rather than overwriting the old one, so historical percent-of-goal
figures always read against whatever goal was actually in effect on that
day, even if the goal has since changed.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "data" / "manager_portal.db"


@contextmanager
def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with _conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS technicians (
            tech_code TEXT PRIMARY KEY,
            name      TEXT NOT NULL,
            dashboard TEXT NOT NULL,   -- 'Fixed' | 'Removable'
            area      TEXT NOT NULL,   -- e.g. 'CAD/CAM', 'Ceramics', 'Removables'
            active    INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS goals (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            tech_code      TEXT NOT NULL,
            units_per_day  REAL NOT NULL,
            effective_date TEXT NOT NULL,   -- ISO date
            created_at     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_goals_tech_date
            ON goals (tech_code, effective_date);

        CREATE TABLE IF NOT EXISTS pto (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            tech_code  TEXT NOT NULL,
            pto_date   TEXT NOT NULL,       -- ISO date
            portion    TEXT NOT NULL,       -- 'half' | 'full'
            note       TEXT DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pto_tech_date
            ON pto (tech_code, pto_date);
        """)


init_db()


# ── Technician roster ─────────────────────────────────────────────────────────

def refresh_roster(rows: list[dict]) -> None:
    """
    Upsert the active technician roster. Called once per pipeline run with
    rows like {'tech_code', 'name', 'dashboard', 'area'}. Any existing
    technician NOT in this run's roster is marked inactive (not deleted —
    their goal/PTO history stays intact). Never touches goals or pto tables.
    """
    seen = {r["tech_code"] for r in rows}
    with _conn() as conn:
        conn.execute("UPDATE technicians SET active = 0")
        for r in rows:
            conn.execute("""
                INSERT INTO technicians (tech_code, name, dashboard, area, active)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(tech_code) DO UPDATE SET
                    name=excluded.name, dashboard=excluded.dashboard,
                    area=excluded.area, active=1
            """, (r["tech_code"], r["name"], r["dashboard"], r["area"]))
        if seen:
            placeholders = ",".join("?" * len(seen))
            conn.execute(
                f"UPDATE technicians SET active = 1 WHERE tech_code IN ({placeholders})",
                tuple(seen),
            )


def list_technicians(dashboard: str | None = None, active_only: bool = True) -> list[dict]:
    q = "SELECT * FROM technicians"
    conds, params = [], []
    if dashboard:
        conds.append("dashboard = ?"); params.append(dashboard)
    if active_only:
        conds.append("active = 1")
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY area, name"
    with _conn() as conn:
        return [dict(r) for r in conn.execute(q, params).fetchall()]


# ── Goals (effective-dated) ───────────────────────────────────────────────────

def set_goal(tech_code: str, units_per_day: float, effective_date: date | None = None) -> None:
    """Insert a new effective-dated goal row. Does not modify prior rows."""
    eff = (effective_date or date.today()).isoformat()
    with _conn() as conn:
        conn.execute(
            "INSERT INTO goals (tech_code, units_per_day, effective_date, created_at) "
            "VALUES (?, ?, ?, ?)",
            (tech_code, units_per_day, eff, datetime.now().isoformat(timespec="seconds")),
        )


def get_goal_on(tech_code: str, on_date: date | None = None) -> float | None:
    """The goal in effect on `on_date` (defaults to today) — the latest row
    with effective_date <= on_date."""
    d = (on_date or date.today()).isoformat()
    with _conn() as conn:
        row = conn.execute(
            "SELECT units_per_day FROM goals WHERE tech_code = ? AND effective_date <= ? "
            "ORDER BY effective_date DESC, id DESC LIMIT 1",
            (tech_code, d),
        ).fetchone()
    return row["units_per_day"] if row else None


def get_current_goal(tech_code: str) -> float | None:
    return get_goal_on(tech_code, date.today())


def get_goal_history(tech_code: str) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT units_per_day, effective_date, created_at FROM goals "
            "WHERE tech_code = ? ORDER BY effective_date DESC, id DESC",
            (tech_code,),
        ).fetchall()
    return [dict(r) for r in rows]


# ── PTO (half-day minimum increment) ──────────────────────────────────────────

def add_pto(tech_code: str, pto_date: date, portion: str = "full", note: str = "") -> None:
    if portion not in ("half", "full"):
        raise ValueError("portion must be 'half' or 'full'")
    with _conn() as conn:
        conn.execute(
            "INSERT INTO pto (tech_code, pto_date, portion, note, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (tech_code, pto_date.isoformat(), portion, note,
             datetime.now().isoformat(timespec="seconds")),
        )


def get_pto_on(tech_code: str, on_date: date) -> str | None:
    """Returns 'half', 'full', or None for a specific technician/date."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT portion FROM pto WHERE tech_code = ? AND pto_date = ?",
            (tech_code, on_date.isoformat()),
        ).fetchone()
    return row["portion"] if row else None


def list_upcoming_pto(dashboard: str | None = None, days: int = 7) -> list[dict]:
    """PTO entries from today through `days` ahead, joined with technician
    name/area, newest-scheduled first."""
    today = date.today().isoformat()
    end   = (date.today() + timedelta(days=days)).isoformat()
    q = """
        SELECT p.tech_code, t.name, t.area, t.dashboard, p.pto_date, p.portion, p.note
        FROM pto p JOIN technicians t ON t.tech_code = p.tech_code
        WHERE p.pto_date BETWEEN ? AND ?
    """
    params = [today, end]
    if dashboard:
        q += " AND t.dashboard = ?"
        params.append(dashboard)
    q += " ORDER BY p.pto_date"
    with _conn() as conn:
        return [dict(r) for r in conn.execute(q, params).fetchall()]


def projected_capacity_pct(dashboard: str, on_date: date | None = None) -> float:
    """
    Sum of PTO-adjusted goals ÷ sum of full goals, as a percentage, for all
    active technicians in `dashboard` ('Fixed' or 'Removable') on `on_date`
    (defaults to today). A half-day PTO counts as 50% available, full-day 0%.
    Returns 100.0 if there are no active technicians with a goal set (nothing
    to project against, rather than a misleading 0%).
    """
    d = on_date or date.today()
    techs = list_technicians(dashboard=dashboard, active_only=True)

    total_goal, available_goal = 0.0, 0.0
    for t in techs:
        goal = get_goal_on(t["tech_code"], d)
        if not goal:
            continue
        total_goal += goal
        portion = get_pto_on(t["tech_code"], d)
        factor = 0.0 if portion == "full" else (0.5 if portion == "half" else 1.0)
        available_goal += goal * factor

    if total_goal <= 0:
        return 100.0
    return round(available_goal / total_goal * 100, 1)
