@echo off
:: restart_dashboard.bat
:: Kills any running Streamlit process and restarts the dashboard.

cd /d "C:\ArtisticDentalPortal"

echo Stopping Streamlit...
taskkill /F /IM streamlit.exe /T 2>nul
taskkill /F /FI "WINDOWTITLE eq streamlit*" /T 2>nul
:: Kill any python process running streamlit (PowerShell - wmic is unreliable/deprecated)
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name like 'py%%'\" | Where-Object { $_.CommandLine -like '*streamlit*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"

timeout /t 2 /nobreak >nul

echo Starting dashboard...
start "Streamlit Dashboard" python -m streamlit run dashboard.py --server.runOnSave true

echo.
echo Dashboard restarting - open http://localhost:8501 in your browser.
