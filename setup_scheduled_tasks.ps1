# =============================================================================
# Artistic Dental Portal - Task Scheduler Setup (CLEAN REBUILD)
# =============================================================================
# Wipes the 4 existing tasks (if any) and recreates them with simple wrapper
# .bat actions — no argument escaping, no PowerShell-Task Scheduler quote
# weirdness. Each task runs ONE thing: a .bat file from C:\ArtisticDentalPortal\
#
# Run as administrator:
#   powershell -ExecutionPolicy Bypass -File setup_scheduled_tasks.ps1
# =============================================================================

# --- Self-elevate if not running as admin ---
$currentUser = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentUser.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Not elevated. Relaunching with admin privileges (UAC prompt incoming)..."
    $myPath = $MyInvocation.MyCommand.Path
    Start-Process powershell.exe `
        -ArgumentList "-NoProfile","-ExecutionPolicy","Bypass","-File","`"$myPath`"" `
        -Verb RunAs
    exit
}

$ErrorActionPreference = "Stop"
$workDir = "C:\ArtisticDentalPortal"

# --- Wipe any existing instances of these tasks (clean slate) ---
$taskNames = @(
    "Artistic Dental - Cases Export",
    "Artistic Dental - Sales Export",
    "Artistic Dental Pipeline",
    "Artistic Dental Git Push",
    "Artistic Dental Logistics"
)
foreach ($t in $taskNames) {
    if (Get-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $t -Confirm:$false
        Write-Host "  [del] $t"
    }
}

# --- Confirm wrapper .bat files exist ---
$batFiles = @{
    "Artistic Dental - Cases Export" = "$workDir\run_cases.bat"
    "Artistic Dental - Sales Export" = "$workDir\run_sales.bat"
    "Artistic Dental Pipeline"        = "$workDir\run_pipeline.bat"
    "Artistic Dental Git Push"        = "$workDir\git_push.bat"
    "Artistic Dental Logistics"       = "$workDir\run_logistics.bat"
}
foreach ($f in $batFiles.Values) {
    if (-not (Test-Path $f)) {
        Write-Error "Missing wrapper file: $f"
        exit 1
    }
}

# --- Common settings (ALL the flags we want, explicitly) ---
$repeatInterval      = New-TimeSpan -Hours 2      # exports + git push: every 2h
$repeatIntervalPipe  = New-TimeSpan -Hours 1      # full pipeline: hourly at :20 (staggered behind the :00/:10 exports)
$repeatIntervalLogis = New-TimeSpan -Minutes 1    # logistics only: every 1 min
$repeatDuration     = New-TimeSpan -Hours 14

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -DontStopOnIdleEnd `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 20)

# Run as the current user, with their highest available privileges, only
# when they're logged on (so AHK can drive visible UI).
$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Highest

# --- Helper: register a task running a .bat file ---
function Register-BatTask {
    param([string]$Name, [string]$BatFile, [string]$StartTime, [string]$Description,
          [System.TimeSpan]$Interval = $repeatInterval)
    $action = New-ScheduledTaskAction -Execute $BatFile -WorkingDirectory $workDir
    $trigger = New-ScheduledTaskTrigger -Daily -At $StartTime
    $trigger.Repetition = (New-ScheduledTaskTrigger `
        -Once -At $StartTime `
        -RepetitionInterval $Interval `
        -RepetitionDuration $repeatDuration).Repetition

    Register-ScheduledTask `
        -TaskName $Name `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description $Description `
        -Force | Out-Null

    Write-Host "  [add] $Name (start $StartTime, every $($Interval.TotalMinutes)min x 14h)"
}

Write-Host ""
Write-Host "Registering 4 scheduled tasks..."
Register-BatTask `
    -Name "Artistic Dental - Cases Export" `
    -BatFile "$workDir\run_cases.bat" `
    -StartTime "5:00 AM" `
    -Description "Drives DLCPM to export AllCasesByDateIn via AHK."

Register-BatTask `
    -Name "Artistic Dental - Sales Export" `
    -BatFile "$workDir\run_sales.bat" `
    -StartTime "5:10 AM" `
    -Description "Drives DLCPM to export ADL_Pipeline (Sales_Data) via AHK."

Register-BatTask `
    -Name "Artistic Dental Pipeline" `
    -BatFile "$workDir\run_pipeline.bat" `
    -StartTime "5:20 AM" `
    -Description "Processes live_exports CSVs into cache/latest/ KPI files." `
    -Interval $repeatIntervalPipe

Register-BatTask `
    -Name "Artistic Dental Git Push" `
    -BatFile "$workDir\git_push.bat" `
    -StartTime "5:30 AM" `
    -Description "Computes retention/active KPIs, commits, pushes to GitHub."

Register-BatTask `
    -Name "Artistic Dental Logistics" `
    -BatFile "$workDir\run_logistics.bat" `
    -StartTime "5:21 AM" `
    -Description "Logistics-only mini pipeline: XLS -> cases_logistics.csv -> GitHub push." `
    -Interval $repeatIntervalLogis

Write-Host ""
Write-Host "Done. All 5 tasks are now ready. Right-click any in Task Scheduler -> Run."
