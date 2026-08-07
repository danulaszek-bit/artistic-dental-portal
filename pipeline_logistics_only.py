"""
Logistics-only mini pipeline
============================
Reads DailyShipping&LogisticsReport.xls, runs compute_logistics(),
writes cache/latest/cases_logistics.csv + logistics_summary.csv,
then pushes to GitHub only if the data actually changed.

Run manually : py pipeline_logistics_only.py
Scheduled    : every 1-2 minutes via Task Scheduler (run_logistics.bat)
"""

import logging
import os
import subprocess
from datetime import date
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [logistics] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
with open(BASE_DIR / "config.yaml") as f:
    CFG = yaml.safe_load(f)

CACHE_DIR  = BASE_DIR / CFG["output"]["local_cache_dir"]
LATEST_DIR = CACHE_DIR / "latest"
LATEST_DIR.mkdir(parents=True, exist_ok=True)

WATCH_FOLDER = Path(CFG["data_source"]["csv"]["watch_folder"])


def main():
    from mt_reports_parser import load_shipping_logistics_report
    from pipeline_logistics import compute_logistics

    log.info("Loading shipping & logistics report from %s", WATCH_FOLDER)
    shipping_df = load_shipping_logistics_report(WATCH_FOLDER)

    if shipping_df.empty:
        log.warning("No data returned from load_shipping_logistics_report — aborting")
        return

    log.info("Loaded %d cases — running compute_logistics()", len(shipping_df))
    compute_logistics(
        cases_df=shipping_df,
        base_dir=BASE_DIR,
        cache_dir=CACHE_DIR,
        latest_dir=LATEST_DIR,
    )
    log.info("cases_logistics.csv + logistics_summary.csv written")
    # ↑ Local data is now fresh EVERY run (task fires ~1/min), so the wall
    #   display / LAN dashboard stays minute-fresh regardless of push cadence.

    # ── Push to GitHub (throttled to every PUSH_INTERVAL_SEC) ────────────────
    # The git push is what syncs the CLOUD copy. Pushing every minute writes
    # to .git 1,440x/day — each write is a chance for an interrupted push to
    # corrupt a ref. Throttling to 3 min cuts that risk ~3x; the cloud lags a
    # few minutes at most, the local display does not.
    repo = str(BASE_DIR)
    PUSH_INTERVAL_SEC = 180
    import time
    state_file = BASE_DIR / ".logistics_push_state"
    try:
        last_push = float(state_file.read_text().strip())
    except (OSError, ValueError):
        last_push = 0.0
    if time.time() - last_push < PUSH_INTERVAL_SEC:
        log.info("Push throttled (<%ds since last) — data written locally, "
                 "cloud sync next cycle", PUSH_INTERVAL_SEC)
        return

    # Self-heal any ref corruption left by a previously interrupted push,
    # BEFORE attempting git operations, so a broken ref auto-recovers instead
    # of freezing the cloud for hours.
    from git_health import repair_if_broken
    repair_if_broken(BASE_DIR)

    # Skip git entirely if another process holds a lock (will retry next cycle)
    lock_files = [BASE_DIR / ".git" / lk for lk in ("index.lock", "HEAD.lock")]
    if any(lk.exists() for lk in lock_files):
        log.warning("Git lock detected — skipping push this cycle (will retry next run)")
        return

    # Git must NEVER wait on a human here. This runs unattended every few
    # minutes; when the credential store failed, git sat at "Username for
    # 'https://github.com':" forever, wedging the scheduled task in Running and
    # leaving elevated git processes behind that only a reboot/admin could kill.
    # These make an auth failure return an error immediately instead.
    genv = {
        **os.environ,
        "GIT_TERMINAL_PROMPT": "0",   # no stdin prompt
        "GCM_INTERACTIVE": "never",   # no Git Credential Manager UI
        "GIT_ASKPASS": "",            # no askpass helper
        "SSH_ASKPASS": "",
    }

    try:
        subprocess.run(["git", "add", "cache/latest/cases_logistics.csv",
                        "cache/latest/logistics_summary.csv"],
                       cwd=repo, check=True, env=genv, timeout=60)

        diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo, env=genv)
        if diff.returncode != 0:
            # Stamp _data_version.py so Streamlit Cloud redeploys
            from datetime import datetime
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ver_file = BASE_DIR / "_data_version.py"
            ver_file.write_text(
                "# Auto-generated — do not edit manually.\n"
                "# Changing this file forces Streamlit Cloud to redeploy.\n"
                f'DATA_VERSION = "{ts}"\n'
            )
            subprocess.run(["git", "add", "_data_version.py"],
                           cwd=repo, check=True, env=genv, timeout=60)
            subprocess.run(["git", "commit", "-m", f"Logistics update {ts}"],
                           cwd=repo, check=True, env=genv, timeout=60)
            subprocess.run(["git", "push", "origin", "main"],
                           cwd=repo, check=True, env=genv, timeout=180)
            state_file.write_text(str(time.time()))
            log.info("Pushed to GitHub — Streamlit will redeploy")
        else:
            state_file.write_text(str(time.time()))  # nothing to push; still reset the clock
            log.info("No logistics changes — skipping push")
    except subprocess.CalledProcessError as e:
        # Commits still accumulate locally and go out on a later cycle, so a
        # failed push delays the cloud copy but never loses data.
        log.warning("Git operation failed (%s) — will retry next cycle", e)
    except subprocess.TimeoutExpired as e:
        log.error("Git timed out (%s) — check credentials; commits are queued "
                  "locally and will push once auth works", e)


if __name__ == "__main__":
    main()
