"""
ingest_payroll.py
=================
Imports a Proliant "Payroll Register" export (single-period or YTD) into the
local labor_history table as source='actual' rows.

    py ingest_payroll.py                     # newest *Payroll Register* in historical/
    py ingest_payroll.py <path.xlsx>
    py ingest_payroll.py --dry-run           # report only, write nothing

PRIVACY: writes ONLY to the local SQLite DB. Payroll dollars must never reach
cache/latest/ (committed + served to the cloud app).
"""
from __future__ import annotations

import sys
from pathlib import Path

import goals_store
from proliant_parser import (daily_actual_rows, match_to_roster,
                             parse_payroll_register)

BASE_DIR = Path(__file__).parent
HIST_DIR = BASE_DIR / "historical"


def find_latest() -> Path | None:
    hits = [p for p in HIST_DIR.glob("*.xlsx")
            if "payroll register" in p.name.lower() and not p.name.startswith("~$")]
    return max(hits, key=lambda p: p.stat().st_mtime) if hits else None


def main(argv: list[str]) -> int:
    dry = "--dry-run" in argv
    args = [a for a in argv if not a.startswith("--")]
    path = Path(args[0]) if args else find_latest()
    if not path or not path.exists():
        print(f"No payroll register found in {HIST_DIR}")
        return 1

    print(f"Reading {path.name} ...")
    df = parse_payroll_register(path)
    if df.empty:
        print("Nothing parsed — unexpected format.")
        return 1

    roster = goals_store.list_technicians(active_only=True)
    df = match_to_roster(df, roster)

    periods = sorted(set(zip(df["period_start"], df["period_end"])))
    prod = df[df["dashboard"].notna()]
    matched = prod[prod["tech_code"].notna()]
    unmatched = prod[prod["tech_code"].isna()]

    print(f"  {len(df)} checks · {df['empid'].nunique()} employees · "
          f"{len(periods)} pay periods ({periods[0][0]} .. {periods[-1][1]})")
    print(f"  total gross ${df['gross'].sum():,.2f} — production depts "
          f"${prod['gross'].sum():,.2f}, excluded ${df['gross'].sum() - prod['gross'].sum():,.2f}")
    print(f"  roster-matched ${matched['gross'].sum():,.2f} · "
          f"payroll-only ${unmatched['gross'].sum():,.2f}")
    if len(unmatched):
        print("  payroll-only (in production depts, not on the MagicTouch roster):")
        for (n, d), g in unmatched.groupby(["name", "dept_name"])["gross"].sum().items():
            print(f"    {n:32} {d:24} ${g:>11,.2f}")

    rows = daily_actual_rows(df)
    total = sum(r["dollars"] for r in rows)
    print(f"  -> {len(rows)} daily 'actual' rows, ${total:,.2f} allocated")

    if dry:
        print("DRY RUN — nothing written.")
        return 0

    removed = goals_store.clear_labor_actuals()
    n = goals_store.upsert_labor_actuals(rows)
    print(f"Replaced {removed} existing actual rows with {n}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
