@echo off
REM ============================================================
REM start_dashboard_if_down.bat
REM Starts the Streamlit portal ONLY if it isn't already serving
REM on port 8501. Safe to run repeatedly — acts as both the
REM boot auto-start and a crash watchdog (via the scheduled task
REM registered by setup_dashboard_autostart.ps1).
REM ============================================================
cd /d C:\ArtisticDentalPortal

REM Already listening on 8501? Nothing to do.
netstat -ano | findstr ":8501 " | findstr "LISTENING" >nul 2>&1
if %errorlevel%==0 (
    echo [%date% %time%] Dashboard already running on 8501 - no action.
    exit /b 0
)

echo [%date% %time%] Dashboard not running - starting it...
start "Artistic Dental Dashboard" /min py -m streamlit run dashboard.py --server.headless true --server.port 8501
exit /b 0
