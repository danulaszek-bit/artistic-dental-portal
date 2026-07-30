# =============================================================================
# setup_dashboard_autostart.ps1
# =============================================================================
# Registers a scheduled task that keeps the Streamlit portal running:
#   - starts it automatically when you log on after a reboot
#   - re-checks every 5 minutes and restarts it if it has crashed (watchdog)
# The launcher (start_dashboard_if_down.bat) only starts a new instance when
# nothing is already serving on 8501, so this never stacks duplicates.
#
# Run as administrator (this self-elevates with a UAC prompt):
#   powershell -ExecutionPolicy Bypass -File setup_dashboard_autostart.ps1
# =============================================================================

# --- Self-elevate if not running as admin ---
$currentUser = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentUser.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Not elevated. Relaunching with admin privileges (UAC prompt incoming)..."
    Start-Process powershell.exe `
        -ArgumentList "-NoProfile","-ExecutionPolicy","Bypass","-File","`"$($MyInvocation.MyCommand.Path)`"" `
        -Verb RunAs
    exit
}

$ErrorActionPreference = "Stop"
$workDir  = "C:\ArtisticDentalPortal"
$launcher = "$workDir\start_dashboard_if_down.bat"
$taskName = "Artistic Dental Dashboard"

if (-not (Test-Path $launcher)) {
    Write-Error "Launcher not found: $launcher"
    exit 1
}

# Remove any previous copy of this task
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Host "  [del] existing '$taskName'"
}

$action = New-ScheduledTaskAction -Execute $launcher -WorkingDirectory $workDir

# Trigger: at logon, then re-check every 5 min (watchdog) for a long duration.
# Each logon re-arms the repetition window, so it effectively runs forever on
# an always-on, logged-in lab PC.
$trigger = New-ScheduledTaskTrigger -AtLogOn
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Minutes 5) `
    -RepetitionDuration (New-TimeSpan -Days 365)).Repetition

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5)

$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Highest

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "Auto-starts the Streamlit portal at logon and restarts it if it crashes (checks every 5 min)." `
    -Force | Out-Null

Write-Host ""
Write-Host "  [add] '$taskName' - starts at logon + 5-min crash watchdog"
Write-Host ""
Write-Host "Running it once now so the dashboard comes up immediately..."
Start-ScheduledTask -TaskName $taskName
Start-Sleep -Seconds 8
$up = (netstat -ano | Select-String ":8501.*LISTENING")
if ($up) { Write-Host "OK - dashboard is serving on http://localhost:8501" }
else     { Write-Host "Note: not yet listening - give it a few more seconds, then browse to http://localhost:8501" }
