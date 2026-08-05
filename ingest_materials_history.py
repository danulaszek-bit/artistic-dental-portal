"""
ingest_materials_history.py
===========================
ONE-TIME backfill of historical Clixon "Issued" materials into the local
materials_history store, using the same replace-by-week logic as the ongoing
pipeline. Kept SEPARATE from the rolling weekly export so the two never
collide (the pipeline reads only the freshest *issued* file; a history file
sitting in the same folder would fight it).

Usage
-----
1. Put the history export in the portal's historical/ folder (where the other
   one-time backfills live — Sales History 2020.csv, Sales_2025.csv, etc.):
       C:\\ArtisticDentalPortal\\historical\\Materials_History.xlsx   (.xls / .csv ok)
   Keeping it here (not C:\\MT_Reports_Local) means the hourly rolling pipeline
   never touches it — only this script reads it.
2. Run once:
       py ingest_materials_history.py
3. It ingests every week the file covers (replace-by-week) and prints a
   summary. Safe to re-run — idempotent. The data lives in the local DB after.

Pass a different path as an argument if the file is elsewhere:
       py ingest_materials_history.py "D:\\somewhere\\old_materials.xlsx"
"""
from __future__ import annotations

import sys
from pathlib import Path

DEFAULT = Path(__file__).parent / "historical" / "Materials_History.xlsx"


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT
    if not path.exists():
        # tolerate the other extensions at the default stem
        alt = [path.with_suffix(e) for e in (".xlsx", ".xls", ".csv")]
        found = next((p for p in alt if p.exists()), None)
        if not found:
            print(f"History file not found: {path}")
            print("Place it at C:\\ArtisticDentalPortal\\historical\\Materials_History.xlsx "
                  "(or pass a path).")
            sys.exit(1)
        path = found

    import materials_calc
    print(f"Ingesting materials history from: {path.name}")
    days, rows = materials_calc.persist_materials_from_file(path)
    print(f"Done — {rows} rows across {days} day(s) written to materials_history (local DB).")
    print("This span now shows on the manager dashboards. You can delete/archive the file.")


if __name__ == "__main__":
    main()
