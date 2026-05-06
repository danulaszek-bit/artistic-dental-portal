@echo off
REM ============================================================
REM Artistic Dental Portal - nightly auto-push
REM Runs after pipeline.py at ~6:15 AM via Task Scheduler.
REM Order matters: retention KPIs must be computed BEFORE git add.
REM ============================================================

cd /d C:\ArtisticDentalPortal

REM --- Step 1: Recompute retention KPIs from latest case_history + period files ---
echo [%date% %time%] Running compute_retention_kpis.py
py compute_retention_kpis.py
if errorlevel 1 (
    echo [%date% %time%] WARNING: compute_retention_kpis.py failed - pushing executive data only
)

REM --- Step 2: Stage cache/latest/ (which now includes retention CSVs) ---
git add cache/latest/

REM --- Step 3: Commit and push ---
git commit -m "Auto-update data %date%"
git push origin main

echo [%date% %time%] Nightly push complete.
