<#
.SYNOPSIS
  Connect to vCenters, log into Windows VMs via VMware Tools, set the OS power plan to High (or Ultimate), and report before/after.

.NOTES
  Tested with VCF PowerCLI (VMware.VimAutomation.Core)
  Requires: VMware.VimAutomation.Core
  Author: Wayne's helper
  Date: (Get-Date -Format 'yyyy-MM-dd')

.OUTPUTS
  - On-screen table: VM, vCenter, OS, Tools, Before/After GUID+Name, Changed, Status, Note
  - CSV export under .\Reports\OS\
#>

# --- PS7 -> auto relaunch under Windows PowerShell 5.1 for Invoke-VMScript support ---
if ($PSVersionTable.PSEdition -eq 'Core') {
  Write-Host "Re-launching under Windows PowerShell 5.1 for VMware Tools operations..." -ForegroundColor Yellow
  $wpps = (Get-Command powershell.exe -ErrorAction SilentlyContinue).Source
  if (-not $wpps) { throw "Could not find Windows PowerShell (powershell.exe) on this system." }

  $argsList = @('-NoProfile','-ExecutionPolicy','Bypass','-File', $PSCommandPath)
  foreach ($kv in $PSBoundParameters.GetEnumerator()) {
    $argsList += "-$($kv.Key)"
    $argsList += "$($kv.Value)"
  }

  $p = Start-Process -FilePath $wpps -ArgumentList $argsList -PassThru -Wait
  if ($p.ExitCode -ne 0) { throw "Child Windows PowerShell process exited with code $($p.ExitCode)." }
  return
}
# --- end shim ---


#region Parameters
param(
  [string[]]$VCenterServers = @('ukprim098.bfl.local','usprim004.bfl.local','ieprim018.bfl.local'),

  [string[]]$VmNames = @('UKPRAP359'),

  [ValidateSet('High','Ultimate')]
  [string]$TargetScheme = 'High',

  [int]$InvokeTimeoutSeconds = 120,          # hard timeout per guest execution (via job wrapper)

  [switch]$IgnoreVCenterCerts,               # ignore invalid vCenter certs (labs)

  [string]$OutputFolder = (Join-Path -Path (Get-Location) -ChildPath 'Reports')
)
#endregion

#region Preflight: Modules, folders, config
try {
  Import-Module VMware.VimAutomation.Core -ErrorAction Stop
} catch {
  Write-Error "Could not import VMware.VimAutomation.Core. Ensure VCF PowerCLI is installed and modules available."
  return
}

if ($IgnoreVCenterCerts) {
  try { Set-PowerCLIConfiguration -InvalidCertificateAction Ignore -Confirm:$false | Out-Null } catch {}
}

$null = New-Item -ItemType Directory -Force -Path $OutputFolder | Out-Null
$osFolder = Join-Path $OutputFolder 'OS'
$null = New-Item -ItemType Directory -Force -Path $osFolder | Out-Null
#endregion

#region Connect vCenters
Write-Host "Connecting to vCenters: $($VCenterServers -join ', ')" -ForegroundColor Cyan
$cred = Get-Credential -Message 'Enter vCenter credentials (SSO/AD with read perms)'
$connectedServers = @()
foreach ($vc in $VCenterServers) {
  try {
    $connectedServers += Connect-VIServer -Server $vc -Credential $cred -WarningAction SilentlyContinue -ErrorAction Stop
  } catch {
    Write-Warning "Failed to connect to ${vc}: $($_.Exception.Message)"
  }
}
if (-not $connectedServers) { Write-Error 'Could not connect to any vCenter.'; return }
#endregion

#region Resolve VMs
$VmNames = $VmNames | Where-Object { $_ -and $_.Trim() } | Select-Object -Unique
$targets = @()
foreach ($name in $VmNames) {
  try { $targets += Get-VM -Name $name -Server $connectedServers -ErrorAction Stop }
  catch { Write-Warning "VM not found: $name" }
}
if (-not $targets) { Write-Warning 'No target VMs resolved.'; return }
#endregion

#region Guest creds & target GUIDs
$guestCred = Get-Credential -Message 'Enter DOMAIN admin creds for guest OS access (Invoke-VMScript)'

$GuidHighPerf = '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'
$GuidUltimate = 'e9a42b02-d5df-448d-aa00-03f14749eb61' # Not present on all hosts
$targetGuid   = if ($TargetScheme -eq 'Ultimate') { $GuidUltimate } else { $GuidHighPerf }
#endregion

#region Guest script (JSON output; Ultimate ensure)
function New-GuestScript {
  param([string]$TargetGuid, [string]$TargetSchemeName)
@"
`$ErrorActionPreference = 'Stop'

function Get-ActiveScheme {
  `$raw  = (powercfg -getactivescheme) 2>&1
  `$guid = (`$raw -replace '.*GUID\\s+([a-f0-9-]{36}).*','$1').ToLower()
  if ([string]::IsNullOrWhiteSpace(`$guid)) { `$guid = 'unknown' }
  return @{ Raw=`$raw; Guid=`$guid }
}
function Get-SchemeName([string]`$Guid){
  `$schemes = (powercfg -list) 2>&1
  `$name = 'Unknown'
  foreach (`$line in (`$schemes -split "`n")){
    if (`$line -match [regex]::Escape(`$Guid)){
      `$m = [regex]::Match(`$line,'GUID:[^\\(]*\\(([^\\)]*)\\)')
      if (`$m.Success){ `$name = `$m.Groups[1].Value.Trim() }
      break
    }
  }
  return `$name
}
function Scheme-Exists([string]`$Guid){
  (powercfg -list) 2>&1 | Select-String -SimpleMatch `$Guid | ForEach-Object { return `$true }
  return `$false
}

`$before     = Get-ActiveScheme
`$beforeName = Get-SchemeName `$before.Guid
`$errorText  = $null
`$ensured    = $false

# If targeting Ultimate and it's missing, try to duplicate/add it
if (`$TargetGuid -eq '$GuidUltimate' -and -not (Scheme-Exists `$TargetGuid)) {
  try {
    powercfg -duplicatescheme `$TargetGuid | Out-Null
    `$ensured = $true
  } catch {
    `$errorText = "Ultimate plan not present and could not be added: $($_.Exception.Message)"
  }
}

if (-not `$errorText) {
  try { powercfg -setactive `$TargetGuid 2>&1 | Out-Null } catch { `$errorText = $_.Exception.Message }
}

`$after     = Get-ActiveScheme
`$afterName = Get-SchemeName `$after.Guid
`$changed   = [string]::Compare(`$before.Guid, `$after.Guid, `$true) -ne 0

[pscustomobject]@{
  VM                = `$env:COMPUTERNAME
  TargetGuid        = `$TargetGuid
  TargetSchemeName  = '$TargetSchemeName'
  Ensured           = `$ensured
  BeforeGuid        = `$before.Guid
  BeforeName        = `$beforeName
  AfterGuid         = `$after.Guid
  AfterName         = `$afterName
  Changed           = `$changed
  Error             = `$errorText
} | ConvertTo-Json -Compress
"@
}
#endregion

#region Helper: Invoke-VMScript hard-timeout wrapper (job-based)
function Invoke-VMScriptWithTimeout {
  param(
    [Parameter(Mandatory)]$VM,
    [Parameter(Mandatory)][string]$ScriptText,
    [Parameter(Mandatory)][System.Management.Automation.PSCredential]$GuestCredential,
    [int]$TimeoutSeconds = 120
  )
  $job = Start-Job -ScriptBlock {
    param($vm,$st,$cred)
    Invoke-VMScript -VM $vm `
      -ScriptText $st `
      -ScriptType powershell `
      -GuestCredential $cred `
      -ErrorAction Stop `
      -ToolsWaitSeconds 90
  } -ArgumentList $VM,$ScriptText,$GuestCredential

  if (-not (Wait-Job $job -Timeout $TimeoutSeconds)) {
    Stop-Job $job -Force | Out-Null
    Remove-Job $job -Force | Out-Null
    throw "Invoke-VMScript timed out after $TimeoutSeconds seconds."
  }
  try {
    $out = Receive-Job $job -ErrorAction Stop
    return $out
  } finally {
    Remove-Job $job -Force | Out-Null
  }
}
#endregion

#region Execute
$rows = @()
foreach ($vm in $targets) {
  $isWinGuest   = (($vm.Guest.GuestFamily -as [string]) -match 'windows') -or (($vm.Guest.OSFullName -as [string]) -match 'Windows')
  $toolsStatus  = $vm.ExtensionData.Guest.ToolsStatus
  $toolsOkGuest = ($toolsStatus -match 'toolsOk')
  $osName       = $vm.Guest.OSFullName

  # vCenter name fallback for blank VIServer.Name
  $vcName = $vm.VIServer.Name
  if (-not $vcName) {
    try {
      $uri = [uri]$vm.ExtensionData.Client.ServiceUrl
      $vcName = $uri.Host
    } catch { $vcName = '' }
  }

  if (-not $isWinGuest) {
    $rows += [pscustomobject]@{
      VMName=$vm.Name; VCenter=$vcName; OS=$osName; Tools=$toolsStatus
      Status='Skipped (non-Windows)'; BeforeGuid=''; BeforeName=''; AfterGuid=''; AfterName=''; Changed=$false; Note=''
    }
    continue
  }
  if (-not $toolsOkGuest) {
    $rows += [pscustomobject]@{
      VMName=$vm.Name; VCenter=$vcName; OS=$osName; Tools=$toolsStatus
      Status='Skipped (VMware Tools not OK)'; BeforeGuid=''; BeforeName=''; AfterGuid=''; AfterName=''; Changed=$false; Note=''
    }
    continue
  }

  $guestScript = New-GuestScript -TargetGuid $targetGuid -TargetSchemeName $TargetScheme

  try {
    # Run with job-based timeout wrapper (no -TimeoutSeconds on Invoke-VMScript)
    $res = Invoke-VMScriptWithTimeout -VM $vm -ScriptText $guestScript -GuestCredential $guestCred -TimeoutSeconds $InvokeTimeoutSeconds
    $txt = $res.ScriptOutput.Trim()

    $obj = $null
    try { $obj = $txt | ConvertFrom-Json -ErrorAction Stop } catch {}

    if (-not $obj) {
      $rows += [pscustomobject]@{
        VMName=$vm.Name; VCenter=$vcName; OS=$osName; Tools=$toolsStatus
        Status='Completed with error'; BeforeGuid=''; BeforeName=''; AfterGuid=''; AfterName=''
        Changed=$false; Note="Unexpected output (non-JSON): $($txt -replace '\s+',' ')"
      }
      continue
    }

    $rows += [pscustomobject]@{
      VMName     = $vm.Name
      VCenter    = $vcName
      OS         = $osName
      Tools      = $toolsStatus
      Status     = if ($obj.Error) { 'Completed with error' } else { 'OK' }
      BeforeGuid = $obj.BeforeGuid
      BeforeName = $obj.BeforeName
      AfterGuid  = $obj.AfterGuid
      AfterName  = $obj.AfterName
      Changed    = [bool]$obj.Changed
      Note       = if ($obj.Error) { [string]$obj.Error } else { "Target=$($obj.TargetSchemeName) ($($obj.TargetGuid)); Ensured=$($obj.Ensured); GuestReportedVM=$($obj.VM)" }
    }
  }
  catch {
    $rows += [pscustomobject]@{
      VMName=$vm.Name; VCenter=$vcName; OS=$osName; Tools=$toolsStatus
      Status='Invoke-VMScript failed'; BeforeGuid=''; BeforeName=''; AfterGuid=''; AfterName=''
      Changed=$false; Note=$_.Exception.Message
    }
  }
}
#endregion

#region Output
Write-Host "`n== Windows OS Power Plan (Force to $TargetScheme) via VMware Tools ==" -ForegroundColor Green
$rows | Sort-Object VCenter, VMName | Format-Table -AutoSize VMName, VCenter, OS, Tools, BeforeGuid, BeforeName, AfterGuid, AfterName, Changed, Status, Note

$csv = Join-Path $osFolder ("VM-OSPowerPlan-SET-$TargetScheme-$(Get-Date -Format 'yyyyMMdd_HHmmss').csv")
$rows | Export-Csv -NoTypeInformation -Path $csv
Write-Host "Exported to: $csv" -ForegroundColor Yellow
#endregion

#region Disconnect
if ($connectedServers) { Disconnect-VIServer -Server $connectedServers -Confirm:$false }
#endregion
