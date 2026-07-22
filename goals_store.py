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
            portion    TEXT NOT NULL,       -- 'half' | 'full'  (half = 4-hour increment)
            paid       INTEGER NOT NULL DEFAULT 1,  -- 0 = unpaid time off
            note       TEXT DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pto_tech_date
            ON pto (tech_code, pto_date);

        -- Pay type + base rate, effective-dated like goals.
        -- base_rate meaning depends on pay_type:
        --   hourly -> $/hour · salary -> annual $ · unit -> unused (0; task_rates apply)
        CREATE TABLE IF NOT EXISTS pay_settings (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            tech_code      TEXT NOT NULL,
            pay_type       TEXT NOT NULL,   -- 'hourly' | 'unit' | 'salary'
            base_rate      REAL NOT NULL DEFAULT 0,
            effective_date TEXT NOT NULL,
            created_at     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pay_tech_date
            ON pay_settings (tech_code, effective_date);

        -- Piece-pay: per-employee, per-MagicTouch-task-code rate, effective-dated.
        CREATE TABLE IF NOT EXISTS task_rates (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            tech_code      TEXT NOT NULL,
            task_code      TEXT NOT NULL,
            task_desc      TEXT DEFAULT '',
            rate           REAL NOT NULL,
            effective_date TEXT NOT NULL,
            created_at     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_taskrates_lookup
            ON task_rates (tech_code, task_code, effective_date);

        -- Scheduled out-of-lab work (usually Chairside): excluded from capacity
        -- on both sides of the ratio, labor $ charged to target_area instead of
        -- the technician's home area.
        CREATE TABLE IF NOT EXISTS out_of_lab (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            tech_code   TEXT NOT NULL,
            work_date   TEXT NOT NULL,      -- ISO date
            target_area TEXT NOT NULL,      -- area the labor $ charges to
            note        TEXT DEFAULT '',
            created_at  TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_ool_tech_date
            ON out_of_lab (tech_code, work_date);

        -- Daily labor dollars per technician. source='estimated' rows are
        -- recomputed/upserted by the pipeline; a future payroll-reconciliation
        -- import will overwrite periods with source='actual' rows, which
        -- estimates never replace.
        CREATE TABLE IF NOT EXISTS labor_history (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            work_date  TEXT NOT NULL,
            tech_code  TEXT NOT NULL,
            area       TEXT NOT NULL,       -- area charged (home or out-of-lab target)
            dashboard  TEXT NOT NULL,
            pay_type   TEXT NOT NULL,
            dollars    REAL NOT NULL,
            source     TEXT NOT NULL DEFAULT 'estimated',  -- 'estimated' | 'actual'
            created_at TEXT NOT NULL,
            UNIQUE (work_date, tech_code, area, source)
        );
        CREATE INDEX IF NOT EXISTS idx_labor_date
            ON labor_history (work_date, dashboard);
        """)


def _migrate() -> None:
    """Additive migrations for DBs created before a column existed."""
    with _conn() as conn:
        try:
            conn.execute("ALTER TABLE pto ADD COLUMN paid INTEGER NOT NULL DEFAULT 1")
        except sqlite3.OperationalError:
            pass  # column already exists


init_db()
_migrate()


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

def add_pto(tech_code: str, pto_date: date, portion: str = "full", note: str = "",
            paid: bool = True) -> None:
    """portion 'half' = a 4-hour increment. paid=False records unpaid time off
    (same capacity impact; salaried labor is NOT charged for the unpaid part)."""
    if portion not in ("half", "full"):
        raise ValueError("portion must be 'half' or 'full'")
    with _conn() as conn:
        conn.execute(
            "INSERT INTO pto (tech_code, pto_date, portion, paid, note, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (tech_code, pto_date.isoformat(), portion, 1 if paid else 0, note,
             datetime.now().isoformat(timespec="seconds")),
        )


def get_pto_on(tech_code: str, on_date: date) -> str | None:
    """Returns 'half', 'full', or None for a specific technician/date
    (paid or unpaid alike — capacity treats both as absence)."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT portion FROM pto WHERE tech_code = ? AND pto_date = ?",
            (tech_code, on_date.isoformat()),
        ).fetchone()
    return row["portion"] if row else None


def get_pto_detail_on(tech_code: str, on_date: date) -> dict | None:
    """{'portion': 'half'|'full', 'paid': bool} or None."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT portion, paid FROM pto WHERE tech_code = ? AND pto_date = ?",
            (tech_code, on_date.isoformat()),
        ).fetchone()
    return {"portion": row["portion"], "paid": bool(row["paid"])} if row else None


def list_upcoming_pto(dashboard: str | None = None, days: int = 7) -> list[dict]:
    """PTO entries from today through `days` ahead, joined with technician
    name/area, newest-scheduled first."""
    today = date.today().isoformat()
    end   = (date.today() + timedelta(days=days)).isoformat()
    q = """
        SELECT p.tech_code, t.name, t.area, t.dashboard, p.pto_date, p.portion, p.paid, p.note
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
    An out-of-lab day removes the technician from BOTH sides of the ratio —
    they aren't expected to produce in-lab, so the department % isn't dinged.
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
        if get_out_of_lab_on(t["tech_code"], d):
            continue  # excluded from numerator AND denominator
        total_goal += goal
        portion = get_pto_on(t["tech_code"], d)
        factor = 0.0 if portion == "full" else (0.5 if portion == "half" else 1.0)
        available_goal += goal * factor

    if total_goal <= 0:
        return 100.0
    return round(available_goal / total_goal * 100, 1)


# ── Pay settings (effective-dated, like goals) ────────────────────────────────

def set_pay_setting(tech_code: str, pay_type: str, base_rate: float = 0.0,
                    effective_date: date | None = None) -> None:
    if pay_type not in ("hourly", "unit", "salary"):
        raise ValueError("pay_type must be 'hourly', 'unit', or 'salary'")
    eff = (effective_date or date.today()).isoformat()
    with _conn() as conn:
        conn.execute(
            "INSERT INTO pay_settings (tech_code, pay_type, base_rate, effective_date, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (tech_code, pay_type, base_rate, eff,
             datetime.now().isoformat(timespec="seconds")),
        )


def get_pay_setting_on(tech_code: str, on_date: date | None = None) -> dict | None:
    """The pay setting in effect on `on_date` — {'pay_type', 'base_rate'} or None."""
    d = (on_date or date.today()).isoformat()
    with _conn() as conn:
        row = conn.execute(
            "SELECT pay_type, base_rate FROM pay_settings "
            "WHERE tech_code = ? AND effective_date <= ? "
            "ORDER BY effective_date DESC, id DESC LIMIT 1",
            (tech_code, d),
        ).fetchone()
    return dict(row) if row else None


# ── Piece-pay task rates (per employee × task code, effective-dated) ─────────

def set_task_rate(tech_code: str, task_code: str, rate: float,
                  task_desc: str = "", effective_date: date | None = None) -> None:
    eff = (effective_date or date.today()).isoformat()
    with _conn() as conn:
        conn.execute(
            "INSERT INTO task_rates (tech_code, task_code, task_desc, rate, effective_date, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (tech_code, str(task_code), task_desc, rate, eff,
             datetime.now().isoformat(timespec="seconds")),
        )


def get_task_rate_on(tech_code: str, task_code: str, on_date: date | None = None) -> float | None:
    d = (on_date or date.today()).isoformat()
    with _conn() as conn:
        row = conn.execute(
            "SELECT rate FROM task_rates "
            "WHERE tech_code = ? AND task_code = ? AND effective_date <= ? "
            "ORDER BY effective_date DESC, id DESC LIMIT 1",
            (tech_code, str(task_code), d),
        ).fetchone()
    return row["rate"] if row else None


def get_current_task_rates(tech_code: str) -> dict[str, float]:
    """{task_code: rate} — latest effective rate per task for one technician."""
    d = date.today().isoformat()
    with _conn() as conn:
        rows = conn.execute(
            "SELECT task_code, rate FROM task_rates "
            "WHERE tech_code = ? AND effective_date <= ? "
            "ORDER BY effective_date ASC, id ASC",
            (tech_code, d),
        ).fetchall()
    out: dict[str, float] = {}
    for r in rows:               # later (more recent) rows overwrite earlier ones
        out[r["task_code"]] = r["rate"]
    return out


# ── Out-of-lab scheduling ─────────────────────────────────────────────────────

def add_out_of_lab(tech_code: str, work_date: date, target_area: str, note: str = "") -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO out_of_lab (tech_code, work_date, target_area, note, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (tech_code, work_date.isoformat(), target_area, note,
             datetime.now().isoformat(timespec="seconds")),
        )


def get_out_of_lab_on(tech_code: str, on_date: date) -> str | None:
    """Target area if the technician is scheduled out of lab that day, else None."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT target_area FROM out_of_lab WHERE tech_code = ? AND work_date = ?",
            (tech_code, on_date.isoformat()),
        ).fetchone()
    return row["target_area"] if row else None


def list_upcoming_out_of_lab(dashboard: str | None = None, days: int = 14) -> list[dict]:
    today = date.today().isoformat()
    end   = (date.today() + timedelta(days=days)).isoformat()
    q = """
        SELECT o.tech_code, t.name, t.area, t.dashboard, o.work_date, o.target_area, o.note
        FROM out_of_lab o JOIN technicians t ON t.tech_code = o.tech_code
        WHERE o.work_date BETWEEN ? AND ?
    """
    params = [today, end]
    if dashboard:
        q += " AND t.dashboard = ?"
        params.append(dashboard)
    q += " ORDER BY o.work_date"
    with _conn() as conn:
        return [dict(r) for r in conn.execute(q, params).fetchall()]


# ── Labor history (estimated now; actuals overwrite later via reconciliation) ─

def upsert_labor_estimates(rows: list[dict]) -> int:
    """
    Upsert source='estimated' daily labor rows: {'work_date' (ISO str),
    'tech_code', 'area', 'dashboard', 'pay_type', 'dollars'}. Never touches
    source='actual' rows — once a payroll reconciliation lands for a period,
    those actuals stand.
    """
    n = 0
    now = datetime.now().isoformat(timespec="seconds")
    with _conn() as conn:
        for r in rows:
            conn.execute(
                "INSERT INTO labor_history (work_date, tech_code, area, dashboard, pay_type, dollars, source, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 'estimated', ?) "
                "ON CONFLICT(work_date, tech_code, area, source) DO UPDATE SET "
                "  dollars=excluded.dollars, pay_type=excluded.pay_type, "
                "  dashboard=excluded.dashboard, created_at=excluded.created_at",
                (r["work_date"], r["tech_code"], r["area"], r["dashboard"],
                 r["pay_type"], r["dollars"], now),
            )
            n += 1
    return n


def get_labor_history(dashboard: str | None = None, start: date | None = None,
                      end: date | None = None) -> list[dict]:
    """
    Daily labor rows, preferring 'actual' over 'estimated' when both exist for
    the same (date, tech): actuals win, estimates for covered dates are dropped.
    """
    q = "SELECT work_date, tech_code, area, dashboard, pay_type, dollars, source FROM labor_history WHERE 1=1"
    params: list = []
    if dashboard:
        q += " AND dashboard = ?"; params.append(dashboard)
    if start:
        q += " AND work_date >= ?"; params.append(start.isoformat())
    if end:
        q += " AND work_date <= ?"; params.append(end.isoformat())
    q += " ORDER BY work_date"
    with _conn() as conn:
        rows = [dict(r) for r in conn.execute(q, params).fetchall()]

    actual_keys = {(r["work_date"], r["tech_code"]) for r in rows if r["source"] == "actual"}
    return [r for r in rows
            if r["source"] == "actual" or (r["work_date"], r["tech_code"]) not in actual_keys]
