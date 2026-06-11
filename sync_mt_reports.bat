@echo off
:: ─────────────────────────────────────────────────────────────────────────────
:: sync_mt_reports.bat
:: Mirrors \\2019servermts01\reports2 → C:\MT_Reports_Local
:: Run manually or via Task Scheduler (setup_mt_sync_task.ps1)
:: ─────────────────────────────────────────────────────────────────────────────

set SOURCE=\\2019servermts01\reports2
set DEST=C:\MT_Reports_Local
set LOG=C:\MT_Reports_Local\sync.log

:: Create destination if it doesn't exist
if not exist "%DEST%" mkdir "%DEST%"

:: Mirror the share — copy new/changed, remove deleted, retry on network hiccup
robocopy "%SOURCE%" "%DEST%" *.csv *.xls *.xlsx /MIR /Z /R:2 /W:5 /NP /LOG+:"%LOG%" /TEE

exit /b 0
