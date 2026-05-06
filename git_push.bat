@echo off
REM ============================================================
REM Artistic Dental Portal - nightly auto-push
REM Runs after pipeline.py at ~6:15 AM via Task Scheduler.
REM Order matters: KPI computers must finish BEFORE git add.
REM ============================================================

cd /d C:\ArtisticDentalPortal

REM --- Step 1: Recompute retention KPIs ---
echo [%date% %time%] Running compute_retention_kpis.py
py compute_retention_kpis.py
if errorlevel 1 (
    echo [%date% %time%] WARNING: compute_retention_kpis.py failed - continuing
)

REM --- Step 2: Recompute active accounts (Crystal Report fix) ---
echo [%date% %time%] Running compute_active_accounts.py
py compute_active_accounts.py
if errorlevel 1 (
    echo [%date% %time%] WARNING: compute_active_accounts.py failed - continuing
)

REM --- Step 3: Stage cache/latest/ (now includes retention + active accounts CSVs) ---
git add cache/latest/

REM --- Step 4: Commit and push ---
git commit -m "Auto-update data %date%"
git push origin main

echo [%date% %time%] Nightly push complete.
