"""
Artistic Dental Studio — One-shot Case History Importer
========================================================
Run this ONCE to seed cache/case_history.csv from Lexie's three big case files.
After this, pipeline.py merges the nightly Gmail-pulled CSV in incrementally.

Usage:
    py import_case_history.py [folder_with_case_files]

Defaults to scanning the current folder for "Case ListBy Date*" files.
Output:
    cache/case_history.csv      — long-lived deduped case history
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path

import retention as ret


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
)
log = logging.getLogger("import_case_history")

DEFAULT_PATTERN = 'Case ListBy Date*'


def main():
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('.')
    if not folder.exists():
        log.error("Folder not found: %s", folder)
        sys.exit(1)

    out_path = Path('cache/case_history.csv')
    log.info("Scanning %s for files matching '%s'", folder, DEFAULT_PATTERN)

    all_files = []
    for ext in ('.xls', '.xlsx', '.csv'):
        all_files.extend(sorted(folder.glob(f'{DEFAULT_PATTERN}{ext}')))

    if not all_files:
        log.error("No case-list files found in %s", folder)
        sys.exit(1)

    log.info("Found %d file(s) to import:", len(all_files))
    for f in all_files:
        log.info("  - %s (%.1f MB)", f.name, f.stat().st_size / 1024 / 1024)

    existing = ret.load_case_history(out_path)
    if not existing.empty:
        log.warning("Existing case_history.csv has %d rows. New imports will be merged.",
                    len(existing))

    total_parsed = 0
    for f in all_files:
        log.info("Parsing %s ...", f.name)
        cases = ret.parse_case_file(f)
        log.info("  → %d unique case rows", len(cases))
        total_parsed += len(cases)
        fresh_df = ret.cases_to_dataframe(cases)
        existing = ret.merge_case_dataframes(existing, fresh_df)

    ret.save_case_history(existing, out_path)
    log.info("=" * 60)
    log.info("Imported %d total case rows → %d unique after dedup",
             total_parsed, len(existing))
    log.info("Saved to: %s", out_path.resolve())
    log.info("Date range: %s → %s",
             existing['date_in'].min(), existing['date_in'].max())
    log.info("Unique doctors: %d", existing['account_id'].nunique())
    log.info("=" * 60)


if __name__ == '__main__':
    main()
