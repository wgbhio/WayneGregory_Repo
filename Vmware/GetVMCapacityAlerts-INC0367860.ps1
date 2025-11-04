<#
.SYNOPSIS
  Connects to vCenters, collects CPU/Memory sizes for specific VMs, pulls CPU/Memory alarm events for the last 30/60/90 days, summarizes per VM with recommendations, and (Windows only) checks the OS power plan — if it's **Balanced**, switches to **High performance**.

.NOTES
  Requires: VMware.PowerCLI
  Author: Wayne’s helper
  Date: $(Get-Date -Format 'yyyy-MM-dd')

.OUTPUTS
  - Console summary tables with recommendations
  - CSV exports to .\Reports (created if missing)

#>

#region Parameters & Setup
param(
  [string[]]$VCenterServers = @('ukprim098.bfl.local','usprim004.bfl.local','ieprim018.bfl.local'),

  [string[]]$VmNames = @(
    'SPRAP015','USPRAP016','USPRWS009','USPRWS010','USPRAP015','USPRAP016','IEPRAP020','UKPRAP220','UKPRAP196'
  ),

  [string]$OutputFolder = (Join-Path -Path (Get-Location) -ChildPath 'Reports')
)

if (-not (Get-Module -ListAvailable -Name VMware.PowerCLI)) {
  Write-Error 'VMware.PowerCLI is not installed. Install-Module VMware.PowerCLI -Scope CurrentUser'
  return
}

$null = New-Item -ItemType Directory -Force -Path $OutputFolder
$alertsFolder = Join-Path $OutputFolder 'Alerts'
$hardwareFolder = Join-Path $OutputFolder 'Hardware'
$osFolder = Join-Path $OutputFolder 'OS'
$null = New-Item -ItemType Directory -Force -Path $alertsFolder
$null = New-Item -ItemType Directory -Force -Path $hardwareFolder
$null = New-Item -ItemType Directory -Force -Path $osFolder

$now   = Get-Date
$cut30 = $now.AddDays(-30)
$cut60 = $now.AddDays(-60)
$cut90 = $now.AddDays(-90)

#endregion

#region Connect to vCenters
Write-Host "Connecting to vCenters: $($VCenterServers -join ', ')" -ForegroundColor Cyan
$cred = Get-Credential -Message 'Enter vCenter credentials (SSO or AD account with read perms)'

$connectedServers = @()
foreach ($vc in $VCenterServers) {
  try {
    $srv = Connect-VIServer -Server $vc -Credential $cred -WarningAction SilentlyContinue -ErrorAction Stop
    $connectedServers += $srv
  }
  catch {
    Write-Warning "Failed to connect to ${vc}: $($_.Exception.Message)"
  }
}

if (-not $connectedServers) { Write-Error 'Could not connect to any vCenter.'; return }
#endregion

#region Resolve VMs and capture CPU/Memory sizes

$VmNames = $VmNames | Select-Object -Unique

$vmInventory = @()
$vmResolved = @()
foreach ($name in $VmNames) {
  try {
    $vms = Get-VM -Name $name -Server $connectedServers -ErrorAction Stop
  }
  catch {
    Write-Warning "VM not found: $name"
    continue
  }

  foreach ($v in $vms) {
    $numCpu          = $v.NumCpu
    $coresPerSocket  = $v.ExtensionData.Config.Hardware.NumCoresPerSocket
    $sockets         = if ($coresPerSocket -gt 0) { [math]::Ceiling($numCpu / $coresPerSocket) } else { $null }
    $memoryGB        = [math]::Round($v.MemoryGB,2)

    $vmInventory += [pscustomobject]@{
      VCenter          = $v.VIServer.Name
      VMName           = $v.Name
      PowerState       = $v.PowerState
      NumCPU_Total     = $numCpu
      CoresPerSocket   = $coresPerSocket
      CPU_Sockets      = $sockets
      MemoryGB         = $memoryGB
    }
    $vmResolved += $v
  }
}

$hardwareCsv = Join-Path $hardwareFolder ("VM-Hardware-$(Get-Date -Format 'yyyyMMdd_HHmmss').csv")
$vmInventory | Sort-Object VMName | Tee-Object -Variable vmHardwareOut | Export-Csv -NoTypeInformation -Path $hardwareCsv

Write-Host "\n== VM CPU/Memory Sizes ==" -ForegroundColor Green
$vmHardwareOut | Format-Table -AutoSize

#endregion

#region Pull CPU/Memory alarm events for last 90 days (and bucket into 30/60/90)

function Get-CpuMemAlarmEventsForVM {
  param(
    [Parameter(Mandatory)] $VM
  )

  try {
    $events = Get-VIEvent -Entity $VM -Start $cut90 -MaxSamples ([int]::MaxValue)
  }
  catch {
    Write-Warning "Failed to fetch events for $($VM.Name): $($_.Exception.Message)"
    $events = @()
  }

  $alarmEvents = $events | Where-Object {
    $_.GetType().Name -match 'Alarm' -and $_.FullFormattedMessage -match '(?i)\b(cpu|memory)\b'
  } | Select-Object @{n='When';e={$_.CreatedTime}},
                       @{n='EventType';e={$_.GetType().Name}},
                       @{n='Message';e={$_.FullFormattedMessage}},
                       @{n='UserName';e={$_.UserName}}

  $e30 = $alarmEvents | Where-Object { $_.When -ge $cut30 }
  $e60 = $alarmEvents | Where-Object { $_.When -ge $cut60 }
  $e90 = $alarmEvents

  [pscustomobject]@{
    VMName     = $VM.Name
    Cnt_30d    = ($e30 | Measure-Object).Count
    Cnt_60d    = ($e60 | Measure-Object).Count
    Cnt_90d    = ($e90 | Measure-Object).Count
    Messages   = $alarmEvents.Message
  }
}

$alarmSummary = @()
foreach ($v in $vmResolved) {
  $bucketed = Get-CpuMemAlarmEventsForVM -VM $v
  $alarmSummary += $bucketed
}

#endregion

#region Combine hardware + alerts + recommendation

$report = foreach ($vm in $vmInventory) {
  $alerts = $alarmSummary | Where-Object { $_.VMName -eq $vm.VMName }

  $recommendation = 'No CPU/Memory alerts in last 90d – sizing looks fine.'
  if ($alerts -and $alerts.Cnt_90d -gt 0) {
    $msgText = ($alerts.Messages -join ' ').ToLower()
    if ($msgText -match 'cpu' -and $msgText -match 'memory') {
      $recommendation = 'Frequent CPU and Memory alerts – consider adding both CPU and RAM.'
    }
    elseif ($msgText -match 'cpu') {
      $recommendation = 'Frequent CPU alerts – consider adding more vCPUs.'
    }
    elseif ($msgText -match 'memory') {
      $recommendation = 'Frequent Memory alerts – consider adding more RAM.'
    }
    else {
      $recommendation = 'Some CPU/Memory alerts – monitor usage, may need adjustment.'
    }
  }

  [pscustomobject]@{
    VCenter        = $vm.VCenter
    VMName         = $vm.VMName
    CPU            = $vm.NumCPU_Total
    MemoryGB       = $vm.MemoryGB
    Alerts_30d     = ($alerts.Cnt_30d | Out-String).Trim()
    Alerts_60d     = ($alerts.Cnt_60d | Out-String).Trim()
    Alerts_90d     = ($alerts.Cnt_90d | Out-String).Trim()
    Recommendation = $recommendation
  }
}

Write-Host "\n== VM Resource Summary with Recommendations ==" -ForegroundColor Green
$report | Format-Table -AutoSize

$summaryCsv = Join-Path $OutputFolder ("VM-Summary-$(Get-Date -Format 'yyyyMMdd_HHmmss').csv")
$report | Export-Csv -NoTypeInformation -Path $summaryCsv

Write-Host "\nExported summary to: $summaryCsv" -ForegroundColor Yellow

#endregion

#region OS Power Plan: If Balanced, set to High performance (Windows only)

$guestCred = Get-Credential -Message 'Enter DOMAIN admin creds for guest OS access (Invoke-VMScript)'

$GuidBalanced = '381b4222-f694-41f0-9685-ff5bb260df2e'
$GuidHighPerf = '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'

$osPowerPlanResults = @()

foreach ($v in $vmResolved) {
  $isWinGuest   = (($v.Guest.GuestFamily -as [string]) -match 'windows') -or (($v.Guest.OSFullName -as [string]) -match 'Windows')
  $toolsOkGuest = ($v.ExtensionData.Guest.ToolsStatus -match 'toolsOk')
  if (-not $isWinGuest)   { $osPowerPlanResults += [pscustomobject]@{VMName=$v.Name;VCenter=$v.VIServer.Name;Status='Skipped (non-Windows)'; Current=''; Changed=$false; Note=''}; continue }
  if (-not $toolsOkGuest) { $osPowerPlanResults += [pscustomobject]@{VMName=$v.Name;VCenter=$v.VIServer.Name;Status='Skipped (VMware Tools not OK)'; Current=''; Changed=$false; Note=''}; continue }

  $script = @"
  \$current = (powercfg -getactivescheme) 2>&1
  \$currentGuid = (\$current -replace '.*GUID\s+([a-f0-9-]{36}).*','\$1').ToLower()
  if ([string]::IsNullOrWhiteSpace(\$currentGuid)) { \$currentGuid = 'unknown' }

  \$guidBalanced = '$GuidBalanced'
  \$guidHighPerf = '$GuidHighPerf'

  \$isBalanced = (\$currentGuid -eq \$guidBalanced) -or (\$current -match '(?i)balanced')

  if (\$isBalanced) {
    powercfg -setactive \$guidHighPerf 2>&1 | Out-Null
    \$target = \$guidHighPerf
    \$changed = \$true
  } else {
    \$target = \$currentGuid
    \$changed = \$false
  }

  Write-Output ("VM=$env:COMPUTERNAME; CurrentGuid=\$currentGuid; IsBalanced=\$isBalanced; Target=\$target; Changed=\$changed")
"@

  try {
    $res = Invoke-VMScript -VM $v -ScriptText $script -ScriptType powershell -GuestCredential $guestCred -ErrorAction Stop
    $txt = $res.ScriptOutput.Trim()

    $kv = @{}
    foreach ($pair in $txt -split ';') { $k,$val = $pair -split '=',2; if ($k) { $kv[$k.Trim()] = $val.Trim() } }

    $changedVal = $false
    if ($kv.ContainsKey('Changed') -and $kv['Changed']) { [void][bool]::TryParse($kv['Changed'], [ref]$changedVal) }

    $osPowerPlanResults += [pscustomobject]@{
      VMName  = $v.Name
      VCenter = $v.VIServer.Name
      Status  = 'OK'
      Current = $kv['CurrentGuid']
      Changed = $changedVal
      Note    = "IsBalanced=$($kv['IsBalanced']); Target=$($kv['Target']); GuestReportedVM=$($kv['VM'])"
    }
  }
  catch {
    $osPowerPlanResults += [pscustomobject]@{
      VMName  = $v.Name
      VCenter = $v.VIServer.Name
      Status  = 'Error'
      Current = ''
      Changed = $false
      Note    = $_.Exception.Message
    }
  }
}

Write-Host "\n== Windows OS Power Plan (Balanced -> High performance if needed) ==" -ForegroundColor Green
$osPowerPlanResults | Format-Table -AutoSize

$osCsv = Join-Path $osFolder ("VM-OSPowerPlan-$(Get-Date -Format 'yyyyMMdd_HHmmss').csv")
$osPowerPlanResults | Export-Csv -NoTypeInformation -Path $osCsv
Write-Host "Exported OS power plan results to: $osCsv" -ForegroundColor Yellow

#endregion

#region Disconnect
if ($connectedServers) { Disconnect-VIServer -Server $connectedServers -Confirm:$false }
#endregion