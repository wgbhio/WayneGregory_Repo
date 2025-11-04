<# ==========================================================================
VM Builder & Deployer (Integrated, Content-Optional, Clean Finalize)
- Fully qualified VMware cmdlets to avoid Hyper-V conflicts
- Content Library optional (uses local template if Content module unavailable)
- All helper functions at top
- End-of-run finalize: set Tools policy → reboot → power off → UpgradeVM_Task($null) → power on → wait tools → final status
=========================================================================== #>

# --- Modules & capability checks --------------------------------------------
Import-Module VMware.VimAutomation.Core -ErrorAction Stop

function TryImport-Module { param([Parameter(Mandatory)][string]$Name)
  try { Import-Module $Name -ErrorAction Stop; return $true } catch { return $false }
}
$global:__hasContentModule = TryImport-Module -Name 'VMware.VimAutomation.Content'
TryImport-Module -Name 'VMware.VimAutomation.Vds' | Out-Null

# --- Timestamp & step helpers ------------------------------------------------
function Write-Stamp([string]$msg) { Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg) }

function Invoke-Step {
  param([Parameter(Mandatory)][string]$Name,[Parameter(Mandatory)][scriptblock]$Script)
  Write-Stamp ("▶ {0}" -f $Name)
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  try { $r = & $Script; $sw.Stop(); Write-Stamp ("✓ {0} ({1:g})" -f $Name,$sw.Elapsed); return $r }
  catch { $sw.Stop(); Write-Host ("✗ {0} FAILED after {1:g}: {2}" -f $Name,$sw.Elapsed,$_.Exception.Message) -ForegroundColor Red; if ($_.Exception){ Write-Host (" • EXCEPTION: {0}" -f $_.Exception.ToString()) -ForegroundColor DarkYellow } ; if ($_.ScriptStackTrace){ Write-Host (" • STACK: {0}" -f $_.ScriptStackTrace) -ForegroundColor DarkRed } ; throw }
}

# --- Prompt helpers ---------------------------------------------------------
function Ask { param([string]$Prompt,[string]$Default=$null,[switch]$AllowEmpty)
  while ($true) {
    $full = if ($null -ne $Default -and $Default -ne '') { "${Prompt} [${Default}]: " } else { "${Prompt}: " }
    $ans = Read-Host $full
    if ([string]::IsNullOrWhiteSpace($ans)) { $ans = $Default }
    if ($AllowEmpty -or -not [string]::IsNullOrWhiteSpace($ans)) { return $ans }
  }
}
function AskInt { param([string]$Prompt,[int]$Default)
  while ($true){
    $v = Ask -Prompt $Prompt -Default $Default
    if ($v -as [int] -ne $null){ return [int]$v }
    Write-Host "Please enter a whole number." -ForegroundColor Yellow
  }
}
function AskChoice { param([string]$Prompt,[string[]]$Choices,[int]$DefaultIndex=0)
  Write-Host $Prompt -ForegroundColor Cyan
  for ($i=0; $i -lt $Choices.Count; $i++) { $m = if ($i -eq $DefaultIndex) { '*' } else { ' ' } ; Write-Host (" [{0}] {1} {2}" -f $i,$Choices[$i],$m) }
  while ($true) {
    $ans = Read-Host "Enter number or value (default $DefaultIndex)"
    if ([string]::IsNullOrWhiteSpace($ans)) { return $Choices[$DefaultIndex] }
    if ($ans -as [int] -ne $null) {
      $idx = [int]$ans
      if ($idx -ge 0 -and $idx -lt $Choices.Count) { return $Choices[$idx] }
    } else {
      $exact = $Choices | Where-Object { $_ -eq $ans }
      if ($exact) { return $exact }
    }
  }
}

# --- JSON helpers -----------------------------------------------------------
function Remove-JsonComments {
  param([Parameter(Mandatory)][string]$Text)
  $sb = New-Object System.Text.StringBuilder
  $inString=$false; $inLine=$false; $inBlock=$false; $escape=$false
  for ($i=0; $i -lt $Text.Length; $i++) {
    $ch = $Text[$i]
    $nx = if ($i+1 -lt $Text.Length) { $Text[$i+1] } else { [char]0 }
    if ($inLine) { if ($ch -eq "`r" -or $ch -eq "`n") { $inLine=$false; $sb.Append($ch) | Out-Null } ; continue }
    if ($inBlock){ if ($ch -eq '*' -and $nx -eq '/') { $inBlock=$false; $i++ } ; continue }
    if ($inString){
      $sb.Append($ch) | Out-Null
      if (-not $escape -and $ch -eq '"'){ $inString=$false }
      $escape = ($ch -eq '\\') -and -not $escape
      continue
    }
    if ($ch -eq '"'){ $inString=$true; $escape=$false; $sb.Append($ch) | Out-Null; continue }
    if ($ch -eq '/' -and $nx -eq '/') { $inLine=$true; $i++; continue }
    if ($ch -eq '/' -and $nx -eq '*') { $inBlock=$true; $i++; continue }
    $sb.Append($ch) | Out-Null
  }
  $sb.ToString()
}
function Read-Config {
  param([string]$Path)
  if (-not (Test-Path $Path)) { throw "Config file not found: $Path" }
  $raw = Get-Content -Raw -Path $Path
  $clean = Remove-JsonComments -Text $raw
  $clean = [regex]::Replace($clean, ',(?=\s*[\]}])', '')
  try { $clean | ConvertFrom-Json } catch { throw "Config parse error: $_" }
}

# --- Folder resolvers -------------------------------------------------------
function Resolve-RootFolder { param([Parameter(Mandatory)]$Datacenter,[Parameter(Mandatory)][string]$Name)
  $vmRoot = VMware.VimAutomation.Core\Get-Folder -Name 'vm' -Location $Datacenter -Type VM -ErrorAction Stop | Select-Object -First 1
  $folder = VMware.VimAutomation.Core\Get-Folder -Location $vmRoot -Type VM -ErrorAction SilentlyContinue | Where-Object { $_.Name -eq $Name } | Select-Object -First 1
  if (-not $folder) { $folder = VMware.VimAutomation.Core\New-Folder -Name $Name -Location $vmRoot -ErrorAction Stop }
  return $folder
}
function Ensure-TemplatesFolderStrict { param([Parameter(Mandatory)]$Datacenter,[string]$FolderName='Templates') (Resolve-RootFolder -Datacenter $Datacenter -Name $FolderName) }

# --- AD helpers -------------------------------------------------------------
function Wait-ForADComputer {
  param(
    [string]$Name,[string]$BaseDN,[int]$TimeoutSec=180,
    [System.Management.Automation.PSCredential]$Credential,[string]$Server
  )
  $elapsed=0
  while($elapsed -lt $TimeoutSec){
    try{
      $p=@{ LDAPFilter="(sAMAccountName=$Name$)"; SearchBase=$BaseDN; SearchScope='Subtree'; ErrorAction='SilentlyContinue' }
      if($Credential){$p.Credential=$Credential}; if($Server){$p.Server=$Server}
      $obj=Get-ADComputer @p
      if($obj){return $obj}
    } catch{}
    Start-Sleep -Seconds 5; $elapsed+=5
  }
  return $null
}
function Get-BaseDNFromTargetOU {
  param([Parameter(Mandatory)][string]$TargetOU)
  $dcParts = ($TargetOU -split ',') | Where-Object { $_ -match '^\s*DC=' }
  if (-not $dcParts) { throw "TargetOU does not appear to be a distinguished name (missing DC=... parts)." }
  ($dcParts -join ',').Trim()
}
function Get-AdCredential {
  param(
    [bool]$UseVCenterCreds,[System.Management.Automation.PSCredential]$VcCred,
    [string]$CredFile,[string]$DefaultCredFilePath,[switch]$PromptIfMissing
  )
  if ($UseVCenterCreds -and $VcCred) { return $VcCred }
  if ($CredFile -and (Test-Path $CredFile)) { try { return Import-Clixml -Path $CredFile } catch {} }
  if ($DefaultCredFilePath -and (Test-Path $DefaultCredFilePath)) { try { return Import-Clixml -Path $DefaultCredFilePath } catch {} }
  if ($PromptIfMissing) {
    Write-Host "Enter credentials for Active Directory domain (e.g. bfl\administrator)" -ForegroundColor Cyan
    $c = Get-Credential
    try { $dir = Split-Path -Parent $DefaultCredFilePath; if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null } } catch {}
    try { if ($DefaultCredFilePath) { $c | Export-Clixml -Path $DefaultCredFilePath -Force | Out-Null } } catch {}
    return $c
  }
  return $null
}
function Get-AdDcCandidates {
  param([string]$Region,[string]$UsSite,[string]$Preferred)
  $cands = @()
  if ($Preferred) { $cands += $Preferred }
  try {
    Import-Module ActiveDirectory -ErrorAction Stop | Out-Null
    $all = Get-ADDomainController -Filter * -ErrorAction Stop
    if ($Region -eq 'US' -and $UsSite) {
      $siteName = if ($UsSite -eq 'HAW') { 'Hawthorne' } elseif ($UsSite -eq 'MAR') { 'Marlborough' } else { $null }
      if ($siteName) { $cands += ($all | Where-Object { $_.Site -eq $siteName -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName) }
      $cands += ($all | Where-Object { $_.HostName -like 'USPRDC036*' -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
      $cands += ($all | Where-Object { $_.HostName -like 'USPRDC*' -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
    } elseif ($Region -eq 'UK') {
      $cands += ($all | Where-Object { $_.HostName -like 'UKPRDC*' -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
    } elseif ($Region -eq 'IE') {
      $cands += ($all | Where-Object { $_.HostName -like 'IEPRDC*' -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
    }
    $cands += ($all | Where-Object { -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
  } catch { Write-Warning ("Get-AdDcCandidates fallback only. Error: {0}" -f $_.Exception.Message) }
  $seen=@{}; $out=@(); foreach($x in $cands){ if(-not $seen.ContainsKey($x)){ $seen[$x]=$true; $out+=$x } } return $out
}

# --- VMware helpers ---------------------------------------------------------
function Select-BestDatastoreFromCluster {
  param([Parameter(Mandatory)]$ClusterOrStoragePod)
  $dss = @( VMware.VimAutomation.Core\Get-Datastore -RelatedObject $ClusterOrStoragePod -ErrorAction SilentlyContinue | Where-Object { $_ } )
  function Test-InMaintenance([object]$ds) {
    $mmProp = $ds.PSObject.Properties['MaintenanceMode']
    if ($mmProp) {
      $v = $mmProp.Value
      if ($v -is [bool]) { return [bool]$v }
      if ($v -is [string]) { return ($v -match 'inMaintenance|maintenance') }
    }
    if ($ds.ExtensionData -and $ds.ExtensionData.Summary -and $ds.ExtensionData.Summary.MaintenanceMode) {
      return ($ds.ExtensionData.Summary.MaintenanceMode -match 'inMaintenance|maintenance')
    }
    return $false
  }
  $candidates = $dss | Where-Object { $_.State -eq 'Available' -and -not (Test-InMaintenance $_) } | Sort-Object -Property FreeSpaceGB -Descending
  if (-not $candidates) { throw "No suitable datastores found in datastore cluster '$($ClusterOrStoragePod.Name)'." }
  return $candidates[0]
}
function Get-DatacenterForCluster {
  param([Parameter(Mandatory)]$Cluster)
  foreach ($dc in (VMware.VimAutomation.Core\Get-Datacenter)) {
    $clustersInDc = VMware.VimAutomation.Core\Get-Cluster -Location $dc -ErrorAction SilentlyContinue
    if ($clustersInDc -and ($clustersInDc | Where-Object { $_.Id -eq $Cluster.Id })) { return $dc }
  }
  throw "Could not resolve Datacenter for cluster '$($Cluster.Name)'."
}
function Set-VMNicStartConnected {
  param([Parameter(Mandatory)]$VM)
  $vmView=VMware.VimAutomation.Core\Get-View -Id $VM.Id; $spec=New-Object VMware.Vim.VirtualMachineConfigSpec; $changes=@()
  foreach ($dev in $vmView.Config.Hardware.Device) {
    if ($dev -is [VMware.Vim.VirtualEthernetCard]) {
      if (-not $dev.Connectable){ $dev.Connectable=New-Object VMware.Vim.VirtualDeviceConnectInfo }
      $dev.Connectable.Connected=$true; $dev.Connectable.StartConnected=$true; $dev.Connectable.AllowGuestControl=$true
      $chg=New-Object VMware.Vim.VirtualDeviceConfigSpec; $chg.operation='edit'; $chg.device=$dev; $changes+=$chg
    }
  }
  if ($changes.Count -gt 0){ $spec.deviceChange=$changes; $vmView.ReconfigVM_Task($spec) | Out-Null }
}
function Ensure-SingleCDHard {
  param([Parameter(Mandatory)]$VM)
  $vmView=VMware.VimAutomation.Core\Get-View -Id $VM.Id; $cds=@($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.VirtualCdrom] })
  if ($cds.Count -gt 1){
    $spec=New-Object VMware.Vim.VirtualMachineConfigSpec; $changes=@()
    for($i=1;$i -lt $cds.Count;$i++){ $devSpec=New-Object VMware.Vim.VirtualDeviceConfigSpec; $devSpec.operation='remove'; $devSpec.device=$cds[$i]; $changes+=$devSpec }
    $spec.deviceChange=$changes; $vmView.ReconfigVM_Task($spec)|Out-Null
  }
  $vmView=VMware.VimAutomation.Core\Get-View -Id $VM.Id; $firstCd=(@($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.VirtualCdrom] }))[0]
  if ($firstCd){
    if (-not $firstCd.Connectable){ $firstCd.Connectable=New-Object VMware.Vim.VirtualDeviceConnectInfo }
    $firstCd.Connectable.Connected=$false; $firstCd.Connectable.StartConnected=$false; $firstCd.Connectable.AllowGuestControl=$true
    $spec2=New-Object VMware.Vim.VirtualMachineConfigSpec; $edit=New-Object VMware.Vim.VirtualDeviceConfigSpec; $edit.operation='edit'; $edit.device=$firstCd
    $spec2.deviceChange=@($edit); $vmView.ReconfigVM_Task($spec2)| Out-Null
  }
}
function Wait-ToolsHealthy {
  param([Parameter(Mandatory)]$VM,[int]$TimeoutSec=180,[int]$PollSec=5)
  $deadline=(Get-Date).AddSeconds($TimeoutSec)
  do{
    try{
      $g=VMware.VimAutomation.Core\Get-VMGuest -VM $VM -ErrorAction Stop
      if($g.State -eq 'Running' -and $g.ToolsStatus -in 'toolsOk','toolsOld'){ return $g.ToolsStatus }
    } catch{}
    Start-Sleep -Seconds $PollSec
  } while((Get-Date) -lt $deadline)
  return $null
}
function New-VM_FilteringOvfWarning {
  param([hashtable]$Params)
  $warn = @()
  $vm = VMware.VimAutomation.Core\New-VM @Params -WarningAction SilentlyContinue -WarningVariable +warn
  if ($warn) {
    $ignore = [regex]'com\.vmware\.vcenter\.ovf\.ovf_warning.*target_datastore.*sdrs'
    foreach($w in $warn){ $wStr = [string]$w; if (-not $ignore.IsMatch($wStr)) { Write-Warning $wStr } }
  }
  return $vm
}

# --- PVSCSI helpers ---------------------------------------------------------
function Get-ScsiSnapshot {
  param([Parameter(Mandatory)]$VM)
  $v = VMware.VimAutomation.Core\Get-View -Id $VM.Id
  $list = @()
  foreach($d in $v.Config.Hardware.Device){
    if ($d -is [VMware.Vim.VirtualSCSIController]) { $list += [pscustomobject]@{ Bus=$d.BusNumber; Key=$d.Key; Type=$d.GetType().Name } }
  }
  $list | Sort-Object Bus
}
function Add-PvScsiControllerApi {
  param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)][int]$BusNumber)
  $vmView = VMware.VimAutomation.Core\Get-View -Id $VM.Id
  $exists = $vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.ParaVirtualSCSIController] -and $_.BusNumber -eq $BusNumber } | Select-Object -First 1
  if ($exists) { return $true }
  $spec = New-Object VMware.Vim.VirtualMachineConfigSpec
  $ctrl = New-Object VMware.Vim.ParaVirtualSCSIController
  $ctrl.Key = -1000 - $BusNumber
  $ctrl.BusNumber = $BusNumber
  $ctrl.SharedBus = [VMware.Vim.VirtualSCSISharing]::noSharing
  $ctrl.ControllerKey = 100
  $devSpec = New-Object VMware.Vim.VirtualDeviceConfigSpec
  $devSpec.Operation = [VMware.Vim.VirtualDeviceConfigSpecOperation]::add
  $devSpec.Device = $ctrl
  $spec.DeviceChange = @($devSpec)
  (VMware.VimAutomation.Core\Get-View -Id $VM.Id).ReconfigVM_Task($spec) | Out-Null
  return $true
}
function Get-NextScsiUnitNumber {
  param([Parameter(Mandatory)]$VmView,[Parameter(Mandatory)][int]$ControllerKey)
  $used = @()
  foreach($d in $VmView.Config.Hardware.Device){
    if ($d -is [VMware.Vim.VirtualDisk] -and $d.ControllerKey -eq $ControllerKey -and $d.UnitNumber -ne $null){
      $used += [int]$d.UnitNumber
    }
  }
  foreach($u in 0..15){
    if ($u -ne 7 -and $used -notcontains $u){ return $u }
  }
  throw "No free SCSI unit number left on controller key $ControllerKey."
}
function Move-HardDiskToBusApi {
  param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)]$HardDisk,[Parameter(Mandatory)][int]$TargetBus)
  $vmView = VMware.VimAutomation.Core\Get-View -Id $VM.Id

  if ($TargetBus -ge 1) {
    $have = $vmView.Config.Hardware.Device |
      Where-Object { $_ -is [VMware.Vim.ParaVirtualSCSIController] -and $_.BusNumber -eq $TargetBus } |
      Select-Object -First 1
    if (-not $have) {
      Add-PvScsiControllerApi -VM $VM -BusNumber $TargetBus | Out-Null
      Start-Sleep -Milliseconds 700
      $vmView = VMware.VimAutomation.Core\Get-View -Id $VM.Id
    }
  }

  $controllers = @{}
  foreach($c in $vmView.Config.Hardware.Device){
    if ($c -is [VMware.Vim.VirtualSCSIController]) { $controllers[$c.BusNumber] = $c }
  }
  if (-not $controllers.ContainsKey($TargetBus)) { throw "No SCSI controller on bus $TargetBus." }

  $ctrl   = $controllers[$TargetBus]
  $diskDev= $vmView.Config.Hardware.Device |
    Where-Object { $_ -is [VMware.Vim.VirtualDisk] -and $_.Key -eq $HardDisk.ExtensionData.Key } |
    Select-Object -First 1
  if (-not $diskDev) { throw "Cannot find disk device for key $($HardDisk.ExtensionData.Key)." }

  $unit = Get-NextScsiUnitNumber -VmView $vmView -ControllerKey $ctrl.Key
  $diskDev.ControllerKey = $ctrl.Key
  $diskDev.UnitNumber    = $unit

  $spec = New-Object VMware.Vim.VirtualMachineConfigSpec
  $chg  = New-Object VMware.Vim.VirtualDeviceConfigSpec
  $chg.operation = [VMware.Vim.VirtualDeviceConfigSpecOperation]::edit
  $chg.device    = $diskDev
  $spec.deviceChange = @($chg)
  $vmView.ReconfigVM_Task($spec) | Out-Null

  Write-Stamp ("Moved disk '{0}' to SCSI{1}:{2}" -f $HardDisk.Name, $TargetBus, $unit)
}
function Add-AdditionalDisksRoundRobin {
  param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)]$Datastore,[Parameter(Mandatory)][int[]]$SizesGB)
  if (-not $SizesGB -or $SizesGB.Count -eq 0) { return }
  1..3 | ForEach-Object { Add-PvScsiControllerApi -VM $VM -BusNumber $_ | Out-Null }
  for ($i=0; $i -lt $SizesGB.Count; $i++) {
    $bus = (1,2,3)[ $i % 3 ]
    $size = [int]$SizesGB[$i]
    $hd = VMware.VimAutomation.Core\New-HardDisk -VM $VM -CapacityGB $size -Datastore $Datastore -Confirm:$false -ErrorAction Stop
    try { Move-HardDiskToBusApi -VM $VM -HardDisk $hd -TargetBus $bus } catch { Write-Host ("WARN: Move disk to SCSI{0} failed: {1}" -f $bus,$_.Exception.Message) -ForegroundColor Yellow }
    Write-Stamp ("Added data disk #{0} ({1} GB) on SCSI{2}" -f ($i+1), $size, $bus)
    Start-Sleep -Milliseconds 300
  }
}

# --- Config normalizer ------------------------------------------------------
function Convert-StructuredConfig {
  param([Parameter(Mandatory)]$In)
  function Pick { param([object[]]$v) foreach($x in $v){ if($null -ne $x -and "$x" -ne ''){ return $x } } $null }
  function Get-Prop($obj, $name) { if ($null -eq $obj) { return $null }; ($obj.PSObject.Properties[$name])?.Value }

  if (-not $In.VMware -and -not $In.AD -and -not $In.Options) { return $In }
  $vmw=$In.VMware; $ad=$In.AD; $opt=$In.Options; $cl=$vmw.ContentLibrary; $hw=$vmw.Hardware

  [pscustomobject]@{
    VCSA               = Pick @((Get-Prop $vmw 'VCSA'), (Get-Prop $vmw 'vCenter'))
    Cluster            = Get-Prop $vmw 'Cluster'
    VMHost             = Pick @((Get-Prop $vmw 'VMHost'), (Get-Prop $vmw 'Host'))
    Folder             = Pick @((Get-Prop $vmw 'VMFolder'), (Get-Prop $vmw 'Folder'))
    TemplatesFolderName= Pick @((Get-Prop $vmw 'TemplatesFolderName'), 'Templates')
    Network            = Get-Prop $vmw 'Network'
    Datastore          = Get-Prop $vmw 'Datastore'
    DatastoreCluster   = Get-Prop $vmw 'DatastoreCluster'
    ContentLibraryName = Pick @((Get-Prop $cl 'Library'), (Get-Prop $In 'ContentLibraryName'))
    TemplateName       = Pick @((Get-Prop $cl 'Item'), (Get-Prop $In 'TemplateName'))
    LocalTemplateName  = Get-Prop $cl 'LocalTemplateName'
    ForceReplace       = Pick @((Get-Prop $cl 'ForceReplace'), (Get-Prop $In 'ForceReplace'), $true)
    VMName             = Get-Prop $vmw 'VMName'
    CustomizationSpec  = Get-Prop $vmw 'CustomizationSpec'
    CPU                = Get-Prop $hw 'CPU'
    MemoryGB           = Get-Prop $hw 'MemoryGB'
    DiskGB             = Get-Prop $hw 'DiskGB'
    AdditionalDiskGB   = Get-Prop $hw 'AdditionalDiskGB'
    AdditionalDisks    = Get-Prop $hw 'AdditionalDisks'
    Sockets            = Get-Prop $hw 'Sockets'
    CoresPerSocket     = Get-Prop $hw 'CoresPerSocket'
    ADTargetOU         = Get-Prop $ad 'TargetOU'
    ADServer           = Get-Prop $ad 'Server'
    ADUseVCenterCreds  = Pick @((Get-Prop $ad 'UseVCenterCreds'), $true)
    ADWaitForSeconds   = Pick @((Get-Prop $ad 'WaitForSeconds'), 180)
    ADCredFile         = Get-Prop $ad 'CredFile'
    ADForceSync        = Pick @((Get-Prop $ad 'ForceSync'), $false)
    ADPostSyncPeers    = Get-Prop $ad 'PostSyncPeers'
    PowerOn            = Pick @((Get-Prop $opt 'PowerOn'), $true)
    EnsureNICConnected = Pick @((Get-Prop $opt 'EnsureNICConnected'), $true)
    RemoveExtraCDROMs  = Pick @((Get-Prop $opt 'RemoveExtraCDROMs'), $true)
    ToolsWaitSeconds   = Pick @((Get-Prop $opt 'ToolsWaitSeconds'), 180)
    PostPowerOnDelay   = Pick @((Get-Prop $opt 'PostPowerOnDelay'), 15)
    ChangeRequestNumber= Pick @((Get-Prop $opt 'ChangeRequestNumber'), (Get-Prop $In 'ChangeRequestNumber'), $null)
    PostDeploy         = Pick @((Get-Prop $In 'PostDeploy'), @{})
  }
}

# --- vSphere Tags (optional) -----------------------------------------------
function Assign-Tag-Safely {
  param([Parameter(Mandatory)][string]$Category,[Parameter(Mandatory)][string]$Tag,[Parameter(Mandatory)]$Entity)
  try {
    $cat = VMware.VimAutomation.Core\Get-TagCategory -Name $Category -ErrorAction SilentlyContinue
    if (-not $cat) { $cat = VMware.VimAutomation.Core\New-TagCategory -Name $Category -Cardinality Single -EntityType 'VirtualMachine' -Confirm:$false }
    $tg = VMware.VimAutomation.Core\Get-Tag -Category $cat -Name $Tag -ErrorAction SilentlyContinue
    if (-not $tg) { $tg = VMware.VimAutomation.Core\New-Tag -Category $cat -Name $Tag -Confirm:$false }
    VMware.VimAutomation.Core\New-TagAssignment -Entity $Entity -Tag $tg -Confirm:$false | Out-Null
  } catch { Write-Warning ("Tag assignment failed for {0}:{1} → {2}" -f $Category,$Tag,$_.Exception.Message) }
}
function Add-VMTags { param([Parameter(Mandatory)]$VM,[hashtable]$Tags) if (-not $Tags) { return } ; foreach($k in $Tags.Keys){ $v = "$($Tags[$k])"; if (-not [string]::IsNullOrWhiteSpace($v)) { Assign-Tag-Safely -Category $k -Tag $v -Entity $VM } } }
function Invoke-ApplyTags { param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)][hashtable]$PostDeploy) if ($PostDeploy.EnableTags -ne $true) { return } ; Add-VMTags -VM $VM -Tags $PostDeploy.Tags }

# --- Post-deploy finishers (optional) --------------------------------------
function Invoke-DeploySCOMCrowdStrike { param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)][hashtable]$PostDeploy)
  if ($PostDeploy.EnableSCOMCrowdStrike -ne $true) { return }
  try { $guest = VMware.VimAutomation.Core\Get-VMGuest -VM $VM -ErrorAction Stop; if ($guest.State -ne 'Running') { Write-Warning "SCOM/CS skipped: guest not running."; return } } catch { Write-Warning "SCOM/CS skipped: cannot query VMGuest."; return }
  $scriptPath = Join-Path $PSScriptRoot '6.Deploy-SCOM-CrowdStrike.ps1'
  if (Test-Path $scriptPath) { Write-Stamp "Running external SCOM/CrowdStrike deploy script…"; & $scriptPath -VMName $VM.Name -ErrorAction Continue } else { Write-Warning "SCOM/CrowdStrike script not found. Skipping." }
}
function Invoke-LocalAdminAccess { param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)][hashtable]$PostDeploy)
  if ($PostDeploy.LocalAdminGroups -isnot [System.Array] -or $PostDeploy.LocalAdminGroups.Count -eq 0) { return }
  $scriptPath = Join-Path $PSScriptRoot '4.LocalAdminAccess.ps1'
  if (Test-Path $scriptPath) { Write-Stamp "Granting local admin access via external script…"; & $scriptPath -VMName $VM.Name -Groups $PostDeploy.LocalAdminGroups -ErrorAction Continue } else { Write-Warning "LocalAdminAccess script not found. Skipping." }
}
function Invoke-SetIvantiPatchingGroups { param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)][hashtable]$PostDeploy)
  if (-not $PostDeploy.IvantiGroup) { return }
  $scriptPath = Join-Path $PSScriptRoot '5.Set-Ivanti-Patching-Groups.ps1'
  if (Test-Path $scriptPath) { Write-Stamp "Setting Ivanti patching group via external script…"; & $scriptPath -VMName $VM.Name -Group $PostDeploy.IvantiGroup -ErrorAction Continue } else { Write-Warning "Ivanti patching script not found. Skipping." }
}
function Invoke-SetWindowsPowerPlanHigh { param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)][hashtable]$PostDeploy)
  if ($PostDeploy.PowerPlan -ne 'High') { return }
  $scriptPath = Join-Path $PSScriptRoot '7.Set-windows-power-plan.ps1'
  if (Test-Path $scriptPath) { Write-Stamp "Setting High performance power plan via external script…"; & $scriptPath -VMName $VM.Name -ErrorAction Continue } else { Write-Warning "Power plan script not found. Skipping." }
}

# --- Tools policy + HW upgrade helpers -------------------------------------
function Set-ToolsUpgradePolicySafe {
  param([Parameter(Mandatory)]$VM, [ValidateSet('manual','upgradeAtPowerCycle')] [string]$Policy='upgradeAtPowerCycle')
  try {
    $cmd = Get-Command VMware.VimAutomation.Core\Set-VM -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Parameters.ContainsKey('ToolsUpgradePolicy')) {
      VMware.VimAutomation.Core\Set-VM -VM $VM -ToolsUpgradePolicy $Policy -Confirm:$false -ErrorAction Stop | Out-Null
      return $true
    }
  } catch {}
  try {
    $vmView = VMware.VimAutomation.Core\Get-View -Id $VM.Id -ErrorAction Stop
    if (-not $vmView.Config.Tools) {
      $spec = New-Object VMware.Vim.VirtualMachineConfigSpec
      $spec.Tools = New-Object VMware.Vim.ToolsConfigInfo
      $vmView.ReconfigVM_Task($spec) | Out-Null
      Start-Sleep -Milliseconds 300
      $vmView = VMware.VimAutomation.Core\Get-View -Id $VM.Id
    }
    $vmView.Config.Tools.ToolsUpgradePolicy = $Policy
    $spec = New-Object VMware.Vim.VirtualMachineConfigSpec
    $spec.Tools = $vmView.Config.Tools
    $vmView.ReconfigVM_Task($spec) | Out-Null
    return $true
  } catch {
    Write-Warning ("Set-ToolsUpgradePolicySafe failed: {0}" -f $_.Exception.Message)
    return $false
  }
}

function Invoke-FinalizeVm {
  param(
    [Parameter(Mandatory)]$VM,
    [int]$ToolsWaitSeconds = 180
  )
  if ($ToolsWaitSeconds -lt 30) { $ToolsWaitSeconds = 30 }
  Write-Stamp "Finalize: enforcing Tools policy, rebooting, upgrading HW, and revalidating Tools…"

  # Ensure Tools policy and healthy status
  try { Set-ToolsUpgradePolicySafe -VM $VM | Out-Null } catch { Write-Warning ("Tools policy finalize failed: {0}" -f $_.Exception.Message) }
  try { $null = Wait-ToolsHealthy -VM $VM -TimeoutSec $ToolsWaitSeconds } catch {}

  # Restart the guest to settle policy
  try {
    $guest = VMware.VimAutomation.Core\Get-VMGuest -VM $VM -ErrorAction SilentlyContinue
    if ($guest -and $guest.State -eq 'Running') {
      Write-Stamp "Finalize: Restarting guest to apply Tools policy…"
      VMware.VimAutomation.Core\Restart-VMGuest -VM $VM -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
      try { $null = Wait-ToolsHealthy -VM $VM -TimeoutSec $ToolsWaitSeconds } catch {}
    }
  } catch { Write-Warning ("Restart-VMGuest failed: {0}" -f $_.Exception.Message) }

  # Power off for HW upgrade
  try {
    $vmObj = VMware.VimAutomation.Core\Get-VM -Id $VM.Id -ErrorAction Stop
    if ($vmObj.PowerState -ne 'PoweredOff') {
      Write-Stamp "Finalize: Powering off for HW upgrade…"
      VMware.VimAutomation.Core\Stop-VM -VM $vmObj -Confirm:$false -ErrorAction Stop | Out-Null
    }
  } catch { Write-Warning ("Stop-VM before HW upgrade failed: {0}" -f $_.Exception.Message) }

  # Upgrade to latest supported via API
  $hwOk = $false
  try {
    $vmView = VMware.VimAutomation.Core\Get-View -Id $VM.Id -ErrorAction Stop
    $taskRef = $vmView.UpgradeVM_Task($null)   # $null => latest supported
    if ($taskRef) { $hwOk = $true }
  } catch { Write-Warning ("UpgradeVM_Task failed: {0}" -f $_.Exception.Message) }
  if (-not $hwOk) { Write-Warning "Hardware upgrade did not complete; VM will remain at current hardware version." }

  # Power on and revalidate Tools
  try {
    Write-Stamp "Finalize: Powering on after HW upgrade…"
    VMware.VimAutomation.Core\Start-VM -VM $VM -Confirm:$false -ErrorAction Stop | Out-Null
  } catch { Write-Warning ("Start-VM after HW upgrade failed: {0}" -f $_.Exception.Message) }
  try { $null = Wait-ToolsHealthy -VM $VM -TimeoutSec $ToolsWaitSeconds } catch {}
}

# --- Builder ---------------------------------------------------------------
function Build-DeployConfig {

  # Fixed maps
  $RegionToVCSA = @{ 'UK'='ukprim098.bfl.local'; 'US'='usprim004.bfl.local'; 'IE'='ieprim018.bfl.local' }
  $RegionToContentLib = @{ 'UK'='Template Library'; 'IE'='DB4 Packer Templates'; 'US'='Packer templates' }

  # Region+Env -> Network
  $NetworkMap = @{
    'US|DEV' = 'VLAN_2032_Stretch'; 'US|PROD' = 'VLAN_2002_Stretch'
    'UK|PROD' = 'BZY|ProdVMs_AP|ProdVMs_EPG'; 'UK|DEV' = 'BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG'
    'IE|DEV' = 'BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG'; 'IE|PROD' = 'BZY|ProdVMs_AP|ProdVMs_EPG'
  }

  $USOptions = @(
    @{ Name='US Stretched'; Cluster='Compute'; DSCluster='PROD_POD_CLUSTER'; Site=$null },
    @{ Name='US HAW';       Cluster='HAW-Local'; DSCluster='HAW_LOCAL_CLUSTER'; Site='HAW'  },
    @{ Name='US MAR';       Cluster='MAR-Local'; DSCluster='MAR_LOCAL_CLUSTER'; Site='MAR'  }
  )

  # OU map (DNs)
  $OU = @{
    'UK|DEV'      = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=London,OU=Accounts Computer,DC=bfl,DC=local'
    'UK|PROD'     = 'OU=Application Servers,OU=Servers & Exceptions,OU=London,OU=Accounts Computer,DC=bfl,DC=local'
    'IE|DEV'      = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Ireland,OU=Accounts Computer,DC=bfl,DC=local'
    'IE|PROD'     = 'OU=Application Servers,OU=Servers & Exceptions,OU=Ireland,OU=Accounts Computer,DC=bfl,DC=local'
    'US|DEV|HAW'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local'
    'US|PROD|HAW' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local'
    'US|DEV|MAR'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local'
    'US|PROD|MAR' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local'
  }

  # AD servers
  $ADServerMap = @{
    'UK'='UKPRDC011.bfl.local'; 'IE'='IEPRDC010.bfl.local'
    'US|HAW'='USPRDC036.bfl.local'; 'US|MAR'='USPRDC036.bfl.local'
  }

  # CL templates
  $TemplateItemMap = [ordered]@{
    'Windows 2022'   = [ordered]@{ UK='PKR_windows_server_2022_std_Current'; IE='PKR_windows_server_2022_std_Current'; US='PKR_windows_server_2022_std_Current' };
    'Windows 2019'   = [ordered]@{ UK='PKR_windows_server_2019_std_Current'; IE='PKR_windows_server_2019_std_Current'; US='PKR_windows_server_2019_std_Current' };
    'Linux CentOS 7' = [ordered]@{ UK='PKR_centos-7_Current'; IE='PKR_centos-7_Current'; US='PKR_centos-7_Current' };
    'Linux RHEL 7'   = [ordered]@{ UK='PKR_redhat-7_Current'; IE='PKR_redhat-7_Current'; US='PKR_redhat-7_Current' };
    'Linux RHEL 9'   = [ordered]@{ UK='PKR_redhat-9_Current'; IE='PKR_redhat-9_Current'; US='PKR_redhat-9_Current' };
    'Linux SLES 15'  = [ordered]@{ UK='sles-15-sp4_Current'; IE='sles-15-sp4_Current'; US='sles-15-sp4_Current' };
  }

  # Customization Specs
  $SpecMap = @{
    'Windows 2022'=@{ UK='Win2022_Template_LD9'; IE='Win2022_Template_DB4'; US='Win2022_Template' }
    'Windows 2019'=@{ UK='Win2019_Template_LD9'; IE='Win2019_Template_DB4'; US='Win2019_Template' }
    'Linux CentOS 7'=@{ UK=$null; IE=$null; US=$null }
    'Linux RHEL 7'=@{   UK=$null; IE=$null; US=$null }
    'Linux RHEL 9'=@{   UK=$null; IE=$null; US=$null }
    'Linux SLES 15'=@{  UK=$null; IE=$null; US=$null }
  }

  # Step 1: Region
  $region = AskChoice -Prompt 'Select VCSA Region' -Choices @('UK','US','IE') -DefaultIndex 0
  $vcsa = $RegionToVCSA[$region]
  $clib = $RegionToContentLib[$region]

  # Step 2: Env
  $env = AskChoice -Prompt 'Environment' -Choices @('PROD','DEV') -DefaultIndex 1
  $netKey = "$region|$env"
  $network = $NetworkMap[$netKey]

  # Root folders
  $vmFolder = if ($env -eq 'PROD') { 'Production' } else { 'Development' }
  $templatesFolder = 'Templates'

  # Step 3: Cluster + OU selection
  $cluster=$null; $dsCluster=$null; $usSite=$null
  if ($region -eq 'UK') { $cluster='LD9Compute'; $dsCluster='LD9_DatastoreCluster' }
  elseif ($region -eq 'IE') { $cluster='DB4Compute'; $dsCluster='DB4_DatastoreCluster' }
  else {
    $picked = AskChoice -Prompt 'US Cluster' -Choices (@('US Stretched','US HAW','US MAR')) -DefaultIndex 0
    $row = switch ($picked) {
      'US Stretched' { @{ Cluster='Compute';   DSCluster='PROD_POD_CLUSTER';   Site=$null } }
      'US HAW'       { @{ Cluster='HAW-Local'; DSCluster='HAW_LOCAL_CLUSTER';  Site='HAW' } }
      'US MAR'       { @{ Cluster='MAR-Local'; DSCluster='MAR_LOCAL_CLUSTER';  Site='MAR' } }
    }
    $cluster=$row.Cluster; $dsCluster=$row.DSCluster; $usSite=$row.Site
    if ($row.Site -eq $null -and $picked -eq 'US Stretched') {
      $usSite = AskChoice -Prompt 'Select US site for OU' -Choices @('HAW','MAR') -DefaultIndex 0
    }
  }
  $ouKey = if ($region -eq 'US') { "$region|$env|$usSite" } else { "$region|$env" }
  $adOU = if ($OU.ContainsKey($ouKey)) { $OU[$ouKey] } else { $null }
  $adServer = if ($region -eq 'US') { $ADServerMap["US|$usSite"] } else { $ADServerMap[$region] }

  # Step 4: Template & Spec
  $osChoices = @('Windows 2022','Windows 2019','Linux CentOS 7','Linux RHEL 7','Linux RHEL 9','Linux SLES 15')
  $tmplChoice = AskChoice -Prompt 'Choose OS / Template' -Choices $osChoices -DefaultIndex 0
  $templateItem = $TemplateItemMap[$tmplChoice][$region]
  $customSpec = $SpecMap[$tmplChoice][$region]

  # Step 5/6/7: CPU/RAM/Disk
  $cpu = AskInt -Prompt 'Total vCPU' -Default 4
  if ($cpu -gt 48) { if ($cpu % 2 -ne 0) { Write-Host 'CPU > 48 → 2 sockets; rounding vCPU up to even.' -ForegroundColor Yellow; $cpu++ } ; $sockets=2; $coresPerSocket=[int]($cpu/2) }
  else { $sockets=1; $coresPerSocket=$cpu }
  $memGB = AskInt -Prompt 'Memory (GB)' -Default 12
  $diskGB = AskInt -Prompt 'System disk (GB)' -Default 100

  # NEW: interactive additional disks
  $addCount = AskInt -Prompt 'How many additional data disks?' -Default 0
  $additionalDisks = @()
  for ($i=1; $i -le $addCount; $i++){
    $sz = AskInt -Prompt (" Size of additional disk #{0} (GB)" -f $i) -Default 50
    $additionalDisks += $sz
  }

  # Step 8: VM name
  do {
    $vmName1 = Ask -Prompt 'VM Name'
    $vmName2 = Ask -Prompt 'Confirm VM Name'
    if ($vmName1 -ne $vmName2){ Write-Host 'Names did not match. Try again.' -ForegroundColor Yellow }
  } until ($vmName1 -eq $vmName2)
  $vmName = $vmName1

  # Step 9: Change number
  $changeNum = Ask -Prompt 'Change Number (goes into VM Notes)' -AllowEmpty

  # Post-deploy block (defaults disabled)
  $post = [ordered]@{
    EnableTags=$false; Tags=@{};
    EnableSCOMCrowdStrike=$false;
    LocalAdminGroups=@();
    IvantiGroup=$null;
    PowerPlan=$null # 'High' to enforce
  }

  $vmw = [ordered]@{
    VCSA=$vcsa; Cluster=$cluster; VMHost=$null; VMFolder=$vmFolder; TemplatesFolderName=$templatesFolder;
    Network=$network; Datastore=''; DatastoreCluster=$dsCluster;
    ContentLibrary=[ordered]@{ Library=$clib; Item=$templateItem; LocalTemplateName=$null; ForceReplace=$true };
    VMName=$vmName; CustomizationSpec=$customSpec;
    Hardware=[ordered]@{ CPU=$cpu; MemoryGB=$memGB; DiskGB=$diskGB; AdditionalDiskGB=0; AdditionalDisks=$additionalDisks; Sockets=$sockets; CoresPerSocket=$coresPerSocket }
  }
  $out = [ordered]@{
    VMware=$vmw
    Options=[ordered]@{ PowerOn=$true; EnsureNICConnected=$true; RemoveExtraCDROMs=$true; ToolsWaitSeconds=90; PostPowerOnDelay=15 }
    AD=[ordered]@{ TargetOU=$adOU; Server=$adServer; UseVCenterCreds=$true; WaitForSeconds=180; ForceSync=$true }
    PostDeploy=$post
  }
  if (-not $adOU) { $out.Remove('AD') }
  if ($changeNum) { $out.ChangeRequestNumber = $changeNum }

  $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } elseif ($MyInvocation.MyCommand.Path) { Split-Path -Parent $MyInvocation.MyCommand.Path } else { (Get-Location).Path }
  $outPath = Join-Path $scriptDir ("{0}.json" -f $vmName)
  ($out | ConvertTo-Json -Depth 8) | Set-Content -Path $outPath -Encoding UTF8 -NoNewline
  Write-Host "Saved configuration → $outPath" -ForegroundColor Green
  return $outPath
}

# --- Main deploy -----------------------------------------------------------
function Deploy-FromCLv2 {
  param([Parameter(Mandatory=$true)][string]$ConfigPath)

  # Guard against stray default -Credential injection
  $savedDefaults = $null
  if ($PSDefaultParameterValues) {
    $savedDefaults = @{} + $PSDefaultParameterValues
    foreach ($k in @($PSDefaultParameterValues.Keys)) { if ($k -match 'Credential$') { $PSDefaultParameterValues.Remove($k) | Out-Null } }
  }

  $oldEap = $ErrorActionPreference
  $ErrorActionPreference = 'Stop'
  try {
    # --- Load & normalize config
    $rawConfig = Read-Config -Path $ConfigPath
    $config = Convert-StructuredConfig -In $rawConfig
    foreach ($key in @('VCSA','Cluster','Network','ContentLibraryName','TemplateName','Folder')) {
      if (-not $config.$key) { throw "Missing required config field: '$key'." }
    }

    # --- Transcript
    $deployLog = Join-Path $env:TEMP ("VMDeploy-{0:yyyyMMdd-HHmmss}-{1}.log" -f (Get-Date), $config.VMName)
    try { Start-Transcript -Path $deployLog -Append -ErrorAction SilentlyContinue | Out-Null } catch {}
    Write-Stamp ("Logging to: {0}" -f $deployLog)

    # --- vCenter credentials (per-VCSA cache)
    $CredPathRoot = Join-Path $env:USERPROFILE 'OneDrive\OneDrive - Beazley Group\Documents\Scripts\DeploymentScript'
    if (-not (Test-Path $CredPathRoot)) { New-Item -Path $CredPathRoot -ItemType Directory -Force | Out-Null }
    $SafeVcsaName = ($config.VCSA -replace '[^a-zA-Z0-9\.-]', '_')
    $CredFile = Join-Path $CredPathRoot ("{0}-cred.xml" -f $SafeVcsaName)
    $cred=$null
    if (Test-Path $CredFile) {
      try { $cred=Import-Clixml -Path $CredFile; if (-not ($cred -and $cred.UserName)) { $cred=$null } else { [void]$cred.GetNetworkCredential().Password } } catch { $cred=$null }
    }
    if (-not $cred) {
      Write-Host "Enter credentials for $($config.VCSA) (e.g. administrator@vsphere.local)"
      $cred=Get-Credential
      try { $cred | Export-Clixml -Path $CredFile -Force | Out-Null } catch {}
    }
    try { VMware.VimAutomation.Core\Set-PowerCLIConfiguration -InvalidCertificateAction Ignore -Confirm:$false | Out-Null } catch {}
    $null = Invoke-Step -Name ("VMware.VimAutomation.Core\Connect-VIServer {0}" -f $config.VCSA) -Script { VMware.VimAutomation.Core\Connect-VIServer -Server $config.VCSA -Credential $cred -ErrorAction Stop }

    # --- Placement
    $clusterObj = VMware.VimAutomation.Core\Get-Cluster -Name $config.Cluster -ErrorAction Stop
    $dc = Get-DatacenterForCluster -Cluster $clusterObj
    $tplFolderName = if ($config.TemplatesFolderName) { $config.TemplatesFolderName } else { 'Templates' }
    $templatesFolder= Ensure-TemplatesFolderStrict -Datacenter $dc -FolderName $tplFolderName

    try { if (-not (Get-Command VMware.VimAutomation.Vds\Get-VDPortgroup -ErrorAction SilentlyContinue)) { TryImport-Module -Name 'VMware.VimAutomation.Vds' | Out-Null } } catch {}

    $hosts = $clusterObj | VMware.VimAutomation.Core\Get-VMHost | Sort-Object Name
    $vmHost = if ($config.VMHost) {
      $h=$hosts | Where-Object { $_.Name -eq $config.VMHost }
      if (-not $h) { throw "VMHost '$($config.VMHost)' not found in cluster '$($clusterObj.Name)'." }
      $h
    } else { $hosts | Get-Random }

    $datastoreObj=$null
    if ($config.DatastoreCluster) {
      $dsCluster = VMware.VimAutomation.Core\Get-DatastoreCluster -Name $config.DatastoreCluster -ErrorAction SilentlyContinue
      if ($dsCluster) {
        Write-Host "Using datastore cluster '$($dsCluster.Name)'; selecting datastore with most free space..."
        $datastoreObj = Select-BestDatastoreFromCluster -ClusterOrStoragePod $dsCluster
        Write-Host "Selected datastore: $($datastoreObj.Name)"
      }
    }
    if (-not $datastoreObj -and $config.Datastore) {
      try { $datastoreObj = VMware.VimAutomation.Core\Get-Datastore -Name $config.Datastore -ErrorAction Stop } catch {}
    }
    if (-not $datastoreObj) { throw "Provide 'Datastore' or 'DatastoreCluster' in config; none resolved." }

    # --- Network lookup
    $pgStd = VMware.VimAutomation.Core\Get-VirtualPortGroup -Standard -ErrorAction SilentlyContinue
    $pgDvs = $null; try { $pgDvs = VMware.VimAutomation.Vds\Get-VDPortgroup -ErrorAction SilentlyContinue } catch {}
    $allPG=@(); if($pgStd){$allPG+=$pgStd}; if($pgDvs){$allPG+=$pgDvs}
    $networkObj = $allPG | Where-Object { $_.Name -eq $config.Network }
    if (-not $networkObj) { throw "Portgroup '$($config.Network)' not found in vCenter '$($config.VCSA)'." }
    $usePortGroup=$false
    try { $usePortGroup = ($networkObj -is [VMware.VimAutomation.Vds.Types.V1.VDPortgroup]) -or ($networkObj.GetType().Name -eq 'VDPortgroup') } catch { $usePortGroup=$false }
    $vmFolder = Resolve-RootFolder -Datacenter $dc -Name $config.Folder

    # --- Content Library → ensure local template OR fallback to existing template
    $localTemplateName = if ($config.LocalTemplateName) { $config.LocalTemplateName } else { $config.TemplateName }
    $localTemplate = $null
    if ($global:__hasContentModule -and (Get-Command -Name Get-ContentLibraryItem -ErrorAction SilentlyContinue)) {
      $clItem = Get-ContentLibraryItem -ContentLibrary $config.ContentLibraryName -Name $config.TemplateName -ErrorAction SilentlyContinue
      if ($clItem) {
        if ($config.ForceReplace) {
          if ($t=VMware.VimAutomation.Core\Get-Template -Name $localTemplateName -ErrorAction SilentlyContinue){ Write-Host "Template '$localTemplateName' exists. Removing (force)..."; VMware.VimAutomation.Core\Remove-Template -Template $t -DeletePermanently -Confirm:$false -ErrorAction Stop }
          if ($v=VMware.VimAutomation.Core\Get-VM -Name $localTemplateName -ErrorAction SilentlyContinue){ Write-Host "VM '$localTemplateName' exists. Removing (force)..."; VMware.VimAutomation.Core\Remove-VM -VM $v -DeletePermanently -Confirm:$false -ErrorAction Stop }
        }
        Write-Host ("`nRefreshing local template from CL item '{0}' → '{1}'..." -f $clItem.Name,$localTemplateName)
        $seedParams=@{ Name=$localTemplateName; ContentLibraryItem=$clItem; VMHost=$vmHost; Datastore=$datastoreObj; Location=$templatesFolder; ErrorAction='Stop' }
        $ovfCfg=$null
        try { $ovfCfg = VMware.VimAutomation.Core\Get-OvfConfiguration -ContentLibraryItem $clItem -Target $clusterObj -ErrorAction Stop; Write-Host 'Detected OVF/OVA; mapping networks via OVF configuration...' }
        catch { Write-Host 'Detected VM template CL item; setting network parameter...' }
        if ($ovfCfg -and $ovfCfg.NetworkMapping) {
          $nm = $ovfCfg.NetworkMapping
          $done = $false
          if ($nm -is [System.Collections.IDictionary]) { foreach ($k in $nm.Keys){ $nm[$k].Value = $networkObj }; $done = $true }
          if (-not $done) { $propNames = @($nm.PSObject.Properties | Select-Object -ExpandProperty Name); if ($propNames -and $propNames.Length -gt 0) { foreach ($p in $propNames) { ($nm.$p).Value = $networkObj }; $done = $true } }
          if (-not $done) { $enumMember = $nm | Get-Member -Name GetEnumerator -ErrorAction SilentlyContinue; if ($enumMember) { foreach ($pair in @($nm.GetEnumerator())) { $pair.Value.Value = $networkObj }; $done = $true } }
          if ($done) { $seedParams['OvfConfiguration']=$ovfCfg } else { if ($networkObj -is [VMware.VimAutomation.Vds.Types.V1.VDPortgroup]) { $seedParams['PortGroup']=$networkObj } else { $seedParams['NetworkName']=$networkObj.Name } }
        } else {
          if ($networkObj -is [VMware.VimAutomation.Vds.Types.V1.VDPortgroup]) { $seedParams['PortGroup']=$networkObj } else { $seedParams['NetworkName']=$networkObj.Name }
        }
        $seedVM = New-VM_FilteringOvfWarning -Params $seedParams
        if ($config.RemoveExtraCDROMs){ Ensure-SingleCDHard -VM $seedVM }
        if ($config.EnsureNICConnected){ Set-VMNicStartConnected -VM $seedVM }
        if ($seedVM.PowerState -ne 'PoweredOff'){ VMware.VimAutomation.Core\Stop-VM -VM $seedVM -Confirm:$false | Out-Null }
        Write-Host ("Converting '{0}' to a local vSphere Template in '{1}'..." -f $localTemplateName,$tplFolderName)
        $localTemplate = VMware.VimAutomation.Core\Set-VM -VM $seedVM -ToTemplate -Name $localTemplateName -Confirm:$false -ErrorAction Stop
        Write-Host ("✅ Local template ready: '{0}'." -f $localTemplate.Name)
      }
    }

    if (-not $localTemplate) {
      $localTemplate = VMware.VimAutomation.Core\Get-Template -Name $localTemplateName -ErrorAction SilentlyContinue
      if (-not $localTemplate) { throw "Content Library unavailable and local template '$localTemplateName' not found. Create local template or install Content module." }
      Write-Host ("Using existing local template '{0}' (skipping Content Library refresh)..." -f $localTemplate.Name)
    }

    # --- Deploy VM (powered off)
    if (VMware.VimAutomation.Core\Get-VM -Name $config.VMName -ErrorAction SilentlyContinue){ throw "A VM named '$($config.VMName)' already exists." }
    $oscSpec=$null
    if ($config.CustomizationSpec){
      $oscSpec=VMware.VimAutomation.Core\Get-OSCustomizationSpec -Name $config.CustomizationSpec -ErrorAction SilentlyContinue
      if (-not $oscSpec){ Write-Warning "OS Customization Spec '$($config.CustomizationSpec)' not found; continuing without it." }
    }
    $fromLabel = if ($global:__hasContentModule) { 'template' } else { 'local template' }
	Write-Host ("`nDeploying VM '{0}' from {1} '{2}'..." -f $config.VMName, $fromLabel, $localTemplate.Name)

    $createParams=@{ Name=$config.VMName; Template=$localTemplate; VMHost=$vmHost; Datastore=$datastoreObj; Location=$vmFolder; ErrorAction='Stop' }
    if ($oscSpec){ $createParams['OSCustomizationSpec']=$oscSpec }
    $newVM = VMware.VimAutomation.Core\New-VM @createParams

    # NIC mapping
    try{
      $firstNic=VMware.VimAutomation.Core\Get-NetworkAdapter -VM $newVM | Select-Object -First 1
      if($firstNic){
        if($usePortGroup){ VMware.VimAutomation.Core\Set-NetworkAdapter -NetworkAdapter $firstNic -PortGroup $networkObj -Confirm:$false | Out-Null }
        else { VMware.VimAutomation.Core\Set-NetworkAdapter -NetworkAdapter $firstNic -NetworkName $config.Network -Confirm:$false | Out-Null }
        Write-Stamp ("Ensured NIC 0 on '{0}'" -f $networkObj.Name)
      }
    } catch { Write-Warning "Post-clone network set failed: $($_.Exception.Message)" }

    if ($config.RemoveExtraCDROMs){ Ensure-SingleCDHard -VM $newVM }
    if ($config.EnsureNICConnected){ Set-VMNicStartConnected -VM $newVM }

    # CPU/Mem + system disk grow while powered off
    if ($config.CPU -or $config.MemoryGB -or $config.CoresPerSocket){
      $setParams=@{ VM=$newVM; Confirm=$false; ErrorAction='SilentlyContinue' }
      if($config.CPU){$setParams['NumCPU']=[int]$config.CPU}
      if($config.MemoryGB){$setParams['MemoryGB']=[int]$config.MemoryGB}
      if($config.CoresPerSocket){$setParams['CoresPerSocket']=[int]$config.CoresPerSocket}
      VMware.VimAutomation.Core\Set-VM @setParams | Out-Null
    }
    if ($config.DiskGB){
      $sysDisk=VMware.VimAutomation.Core\Get-HardDisk -VM $newVM | Select-Object -First 1
      if($sysDisk -and $config.DiskGB -gt $sysDisk.CapacityGB){
        VMware.VimAutomation.Core\Set-HardDisk -HardDisk $sysDisk -CapacityGB $config.DiskGB -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
      }
    }

    # Additional data disks
    $additionalSizes = @()
    if ($rawConfig.VMware -and $rawConfig.VMware.Hardware -and $rawConfig.VMware.Hardware.AdditionalDisks) {
      $additionalSizes = @($rawConfig.VMware.Hardware.AdditionalDisks | ForEach-Object { [int]$_ })
    } elseif ($config.AdditionalDisks) {
      $additionalSizes = @($config.AdditionalDisks | ForEach-Object { [int]$_ })
    } elseif ($config.AdditionalDiskGB -and [int]$config.AdditionalDiskGB -gt 0) {
      $additionalSizes = @([int]$config.AdditionalDiskGB)
    }
    if ($additionalSizes.Count -gt 0) { Add-AdditionalDisksRoundRobin -VM $newVM -Datastore $datastoreObj -SizesGB $additionalSizes } else { Write-Stamp "No additional disk sizes provided." }

    # Notes
    if ($config.ChangeRequestNumber){
      $note = "CR: $($config.ChangeRequestNumber) — deployed $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
      try { VMware.VimAutomation.Core\Set-VM -VM $newVM -Notes $note -Confirm:$false | Out-Null; Write-Stamp "Notes set: $note" } catch { Write-Warning "Failed to set Notes: $($_.Exception.Message)" }
    }

    # --- Power on & settle ---------------------------------------------------
    if ($config.PowerOn){
      Invoke-Step -Name "Power on VM" -Script { VMware.VimAutomation.Core\Start-VM -VM $newVM -Confirm:$false | Out-Null }
      if ($config.PostPowerOnDelay -gt 0) { Write-Stamp ("Sleeping {0}s post power-on…" -f $config.PostPowerOnDelay); Start-Sleep -Seconds ([int]$config.PostPowerOnDelay) }
    }
    if ($config.PowerOn){
      $tw = if ($config.ToolsWaitSeconds) { [int]$config.ToolsWaitSeconds } else { 180 }
      try { $null = Invoke-Step -Name ("Wait for VMware Tools healthy ({0}s)" -f $tw) -Script { Wait-ToolsHealthy -VM $newVM -TimeoutSec $tw } } catch { Write-Warning ("Wait-ToolsHealthy threw: {0}" -f $_.Exception.Message) }
    }

    # ===================== AD MOVE (LAST STEP BEFORE FINALIZE) ===============
    if ($config.ADTargetOU) {
      try {
        Import-Module ActiveDirectory -ErrorAction Stop | Out-Null
        $usSite = if ($config.ADTargetOU -match 'OU=Hawthorne') { 'HAW' } elseif ($config.ADTargetOU -match 'OU=Marlborough') { 'MAR' } else { $null }
        $region = if ($config.ADTargetOU -match 'OU=Ireland') { 'IE' } elseif ($config.ADTargetOU -match 'OU=London') { 'UK' } elseif ($usSite) { 'US' } else { 'OTHER' }
        $dcList = Get-AdDcCandidates -Region $region -UsSite $usSite -Preferred $config.ADServer
        if (-not $dcList -or $dcList.Count -eq 0) { if ($config.ADServer) { $dcList = @($config.ADServer) } else { $dcList = @() } }
        $dcList = @($dcList | Select-Object -Unique)

        $CredPathRoot = Join-Path $env:USERPROFILE 'OneDrive\OneDrive - Beazley Group\Documents\Scripts\DeploymentScript'
        $SafeVcsaName = ($config.VCSA -replace '[^a-zA-Z0-9\.-]', '_')
        $DefaultAdCredFile = Join-Path $CredPathRoot ("{0}-cred.xml" -f $SafeVcsaName)
        $adCred = Get-AdCredential -UseVCenterCreds:$config.ADUseVCenterCreds -VcCred:$cred -CredFile:$config.ADCredFile -DefaultCredFilePath:$DefaultAdCredFile -PromptIfMissing

        $searchBase = $null
        try { if ($null -ne $rawConfig -and ($rawConfig.PSObject.Properties['AD'])) { $adObj = $rawConfig.AD; $hasProp = $adObj | Get-Member -Name UnmanagedOU -ErrorAction SilentlyContinue; if ($hasProp) { $searchBase = $adObj.UnmanagedOU } } } catch {}
        if (-not $searchBase -and $config.ADUnmanagedOU) { $searchBase = $config.ADUnmanagedOU }
        if (-not $searchBase) { $searchBase = Get-BaseDNFromTargetOU -TargetOU $config.ADTargetOU }

        $timeoutTotal = [int]($(if ($config.ADWaitForSeconds) { $config.ADWaitForSeconds } else { 180 }))
        $timeoutPerDc = [int]([Math]::Max(30, [Math]::Floor($timeoutTotal / [Math]::Max(1,$dcList.Count))))

        $adComp = $null
        foreach($dc in $dcList){
          $adComp = Wait-ForADComputer -Name $config.VMName -BaseDN $searchBase -TimeoutSec $timeoutPerDc -Credential $adCred -Server $dc
          if ($adComp) { break }
        }

        if ($adComp) {
          $moveServer = if ($config.ADServer) { $config.ADServer } else { $dcList | Select-Object -First 1 }
          $label = if ($moveServer) { $moveServer } else { '<auto>' }
          Invoke-Step -Name ("Move-ADObject to target OU on {0}" -f $label) -Script {
            $p=@{ Identity=$adComp.DistinguishedName; TargetPath=$config.ADTargetOU; ErrorAction='Stop' }
            if($adCred){$p.Credential=$adCred}; if($moveServer){$p.Server=$moveServer}
            Move-ADObject @p
          }
          Write-Stamp ("AD MOVE SUCCESS → '{0}' to '{1}'" -f $adComp.Name, $config.ADTargetOU)
        } else {
          Write-Warning "AD: Computer not found for move within timeout."
        }
      } catch { Write-Warning ("AD move skipped/failed: {0}" -f $_.Exception.Message) }
    } else { Write-Stamp "AD move: not configured." }

    # ===================== Post-Deploy Finishers (optional) ===================
    $pd = if ($config.PostDeploy) { $config.PostDeploy } else { @{} }
    try { Invoke-ApplyTags -VM $newVM -PostDeploy $pd } catch {}
    try { Invoke-DeploySCOMCrowdStrike -VM $newVM -PostDeploy $pd } catch {}
    try { Invoke-LocalAdminAccess -VM $newVM -PostDeploy $pd } catch {}
    try { Invoke-SetIvantiPatchingGroups -VM $newVM -PostDeploy $pd } catch {}
    try { Invoke-SetWindowsPowerPlanHigh -VM $newVM -PostDeploy $pd } catch {}

    # ===================== Finalize sequence ==================================
    Invoke-FinalizeVm -VM $newVM -ToolsWaitSeconds ([int]$config.ToolsWaitSeconds)

    # --- Final summary --------------------------------------------------------
    try{
      $vmObj = VMware.VimAutomation.Core\Get-VM -Id $newVM.Id -ErrorAction SilentlyContinue
      $toolsState = 'unknown'
      try { $guest = VMware.VimAutomation.Core\Get-VMGuest -VM $newVM -ErrorAction SilentlyContinue; if ($guest) { $toolsState = $guest.ToolsStatus } } catch {}
      Write-Stamp ("Final status → Tools: {0}; HW: {1}; Power: {2}" -f $toolsState, ($vmObj.HardwareVersion ?? 'Unknown'), $vmObj.PowerState)
    } catch {}
    Write-Host ("✅ VM '{0}' deployed." -f $config.VMName)

    try { Stop-Transcript | Out-Null } catch {}

  } finally {
    $ErrorActionPreference = $oldEap
    if ($savedDefaults) { $PSDefaultParameterValues = $savedDefaults }
  }
}

# --- Menu ------------------------------------------------------------------
$global:LastConfigPath = $null
function Show-Menu {
  Write-Host ""
  Write-Host "==== VM Builder & Deployer (Integrated, Content-Optional) ====" -ForegroundColor Cyan
  Write-Host " [1] Build new JSON (guided)"
  Write-Host " [10] Deploy VM from JSON"
  Write-Host " [99] Build then Deploy"
  Write-Host " [0] Exit"
}
while ($true) {
  Show-Menu
  $sel = Read-Host "Select option"
  switch ($sel) {
    '1'  { try { $global:LastConfigPath = Build-DeployConfig } catch { Write-Warning $_ } }
    '10' {
      $path = if ($global:LastConfigPath) { Ask -Prompt 'ConfigPath' -Default $global:LastConfigPath } else { Ask -Prompt 'ConfigPath' }
      if (-not [string]::IsNullOrWhiteSpace($path)) { try { Deploy-FromCLv2 -ConfigPath $path } catch { Write-Warning $_ } }
    }
    '99' { try { $global:LastConfigPath = Build-DeployConfig; if ($global:LastConfigPath) { Deploy-FromCLv2 -ConfigPath $global:LastConfigPath } } catch { Write-Warning $_ } }
    '0'  { break }
    default { Write-Host 'Unknown option.' -ForegroundColor Yellow }
  }
}
