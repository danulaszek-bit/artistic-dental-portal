# ─────────────────────────────────────────────────────────────────────────────
# setup_mt_sync_task.ps1
# Registers sync_mt_reports.bat as a Windows scheduled task.
# Run ONCE by right-clicking → "Run with PowerShell"
# No admin rights required — task runs as current user.
# ─────────────────────────────────────────────────────────────────────────────

$TaskName   = "MT_Reports_Sync"
$BatFile    = "C:\ArtisticDentalPortal\sync_mt_reports.bat"
$Description = "Mirrors \\2019servermts01\reports2 to C:\MT_Reports_Local every 15 minutes"

# Remove existing task if present
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# Action: run the bat file
$Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$BatFile`""

# Trigger: every 15 minutes, indefinitely
$Trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 3) -Once -At (Get-Date)

# Settings: run whether or not user is logged on, start if missed
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

# Register
Register-ScheduledTask `
    -TaskName    $TaskName `
    -Action      $Action `
    -Trigger     $Trigger `
    -Settings    $Settings `
    -Description $Description `
    -RunLevel    Limited | Out-Null

Write-Host ""
Write-Host "✓ Task '$TaskName' registered successfully." -ForegroundColor Green
Write-Host "  Syncs every 3 minutes → C:\MT_Reports_Local"
Write-Host "  Log file: C:\MT_Reports_Local\sync.log"
Write-Host ""
Write-Host "To run it immediately: Start-ScheduledTask -TaskName '$TaskName'"
Write-Host "To remove it:          Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
