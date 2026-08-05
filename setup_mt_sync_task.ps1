# ─────────────────────────────────────────────────────────────────────────────
# setup_mt_sync_task.ps1
# Registers sync_mt_reports.bat (mirrors \\2019servermts01\reports2 ->
# C:\MT_Reports_Local) as a Windows scheduled task:
#   * starts at logon of ANY user
#   * runs whether a user is logged on or not
#   * highest privileges
#   * repeats every 3 minutes, indefinitely
#
# "Run whether logged on or not" + a NETWORK share means the task needs stored
# credentials (Password logon type) — S4U/SYSTEM can't reliably reach the share.
# So this prompts for YOUR Windows password once; re-run it if your password
# changes. Self-elevates (UAC) because Password logon + Highest need admin.
#
# Run:  right-click -> Run with PowerShell   (or)
#       powershell -ExecutionPolicy Bypass -File setup_mt_sync_task.ps1
# ─────────────────────────────────────────────────────────────────────────────

# --- Self-elevate if not admin ---
$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Relaunching elevated (UAC prompt incoming)..."
    Start-Process powershell.exe `
        -ArgumentList "-NoProfile","-ExecutionPolicy","Bypass","-File","`"$($MyInvocation.MyCommand.Path)`"" `
        -Verb RunAs
    exit
}

$ErrorActionPreference = "Stop"
$TaskName    = "MT_Reports_Sync"
$BatFile     = "C:\ArtisticDentalPortal\sync_mt_reports.bat"
$Description  = "Mirrors \\2019servermts01\reports2 to C:\MT_Reports_Local every 3 minutes (any-user logon, runs logged on or not, indefinitely)."

if (-not (Test-Path $BatFile)) { Write-Error "Missing: $BatFile"; exit 1 }

# --- Credentials so the task can run while logged out and reach the share ---
$defaultUser = "$env:USERDOMAIN\$env:USERNAME"
$cred = Get-Credential -UserName $defaultUser `
    -Message "Enter the Windows password for $defaultUser so the sync can run when you're logged out."

# --- Remove any prior copy ---
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue

# --- Action: run the sync bat ---
$Action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument ("/c `"" + $BatFile + "`"")

# --- Trigger: at logon of ANY user, then repeat every 3 min indefinitely ---
$Trigger = New-ScheduledTaskTrigger -AtLogOn
try {
    $rep = New-ScheduledTaskTrigger -Once -At (Get-Date) `
        -RepetitionInterval (New-TimeSpan -Minutes 3) `
        -RepetitionDuration ([TimeSpan]::MaxValue)         # "Indefinitely"
} catch {
    # Some Windows builds reject MaxValue — fall back to an effectively-forever span.
    $rep = New-ScheduledTaskTrigger -Once -At (Get-Date) `
        -RepetitionInterval (New-TimeSpan -Minutes 3) `
        -RepetitionDuration (New-TimeSpan -Days 3650)
}
$Trigger.Repetition = $rep.Repetition

# --- Settings ---
$Settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew

# --- Register: run whether logged on or not (Password logon), highest privileges ---
Register-ScheduledTask `
    -TaskName    $TaskName `
    -Action      $Action `
    -Trigger     $Trigger `
    -Settings    $Settings `
    -Description $Description `
    -User        $cred.UserName `
    -Password    $cred.GetNetworkCredential().Password `
    -RunLevel    Highest `
    -Force | Out-Null

Write-Host ""
Write-Host "Task registered: $TaskName" -ForegroundColor Green
Write-Host "  Trigger : at logon of any user"
Write-Host "  Runs    : whether logged on or not, highest privileges, as $($cred.UserName)"
Write-Host "  Repeat  : every 3 minutes, indefinitely"
Write-Host "  Log     : C:\MT_Reports_Local\sync.log"
Write-Host ""
Write-Host "Kick it off now with:  Start-ScheduledTask -TaskName $TaskName"
