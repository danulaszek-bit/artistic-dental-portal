@echo off
REM ============================================================
REM Artistic Dental Portal - one-shot GitHub deploy
REM Stages, commits, and pushes the Streamlit app + retention work.
REM Re-runnable: safe to run as often as you want.
REM ============================================================

cd /d C:\ArtisticDentalPortal

echo.
echo === git status before ===
git status --short

echo.
echo === Adding files ===
git add .gitignore
git add config.yaml
git add requirements.txt
git add pipeline.py
git add dashboard.py
git add retention.py
git add import_case_history.py
git add compute_retention_kpis.py
git add compute_active_accounts.py
git add git_push.bat
git add deploy.bat
git add .streamlit/
git add pages/
git add cache/latest/

echo.
echo === git status after staging ===
git status --short

echo.
set /p MSG=Enter commit message (or press Enter for default): 
if "%MSG%"=="" set MSG=Update Partner Portal

echo.
echo === Committing ===
git commit -m "%MSG%"

echo.
echo === Pushing to origin/main ===
git push origin main

echo.
echo === Done ===
pause
