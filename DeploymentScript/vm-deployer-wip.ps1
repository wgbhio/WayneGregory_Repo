<# ======================================================================
VM Builder & Deployer (Integrated, Content-Optional, Clean Finalize)
- Uses Content Library if available (deploy → convert to site-suffixed template)
- Force-overwrites existing template of same name (never deletes a VM)
- Fallback to local unsuffixed template if CL item is not deployable
- US site templating & placement (MAR / HAW / COMPUTE) baked in
- Finalize: enforce Tools policy → restart → HW upgrade → power cycle → tools wait
- Post-finalize: Cohesity & Zerto tags assigned to the new VM (Cohesity skipped for 'DV*' servers)
- Post-tags: Optional CrowdStrike (all) + SCOM (PR only) installer function
- AD: Optional domain join + OU move using either vCenter creds or a supplied AD cred file
PS 5.1 compatible (no ternary ?, no ??, no null-conditional operators)

Baseline V4 (US) — v5
Key traits preserved (Baseline V2):
- AD move runs last, no replication call
- Content Library refresh then deploy
- Round-robin additional data disks across PVSCSI 1–3
- Defaults: ToolsWaitSeconds=60, PostPowerOnDelay=15, AD.WaitForSeconds=60
- Root folders: Templates / Production / Development
- Notes minimal: "CR: {CR} — deployed {TS}"
====================================================================== #>
<#
# --- Modules ---------------------------------------------------------------
Import-Module VMware.VimAutomation.Core -ErrorAction Stop
try { Import-Module VMware.VimAutomation.Vds -ErrorAction SilentlyContinue | Out-Null } catch {}
#>
function TryImport-Module {
  param([Parameter(Mandatory)][string]$Name)
  try { Import-Module $Name -ErrorAction Stop; return $true } catch { return $false }
}
$global:__hasContentModule = TryImport-Module -Name 'VMware.VimAutomation.Content'

# --- Utilities -------------------------------------------------------------
function Write-Stamp { param([string]$msg) Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg) }

function Test-WSManWithRetry {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][string]$ComputerName,
    [int]$TimeoutSeconds = 240,
    [int]$IntervalSeconds = 20,

    # NEW: optionally flush local DNS cache between retries
    [switch]$FlushDnsOnRetry
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  do {
    try {
      if (Test-WSMan -ComputerName $ComputerName -ErrorAction SilentlyContinue) {
        Write-Stamp ("[WinRM] {0}: online." -f $ComputerName)
        return $true
      }
    } catch {}

    # NEW: flush DNS locally if requested
    if ($FlushDnsOnRetry) {
      try {
        Write-Stamp ("[WinRM] {0}: flushing local DNS cache (ipconfig /flushdns)..." -f $ComputerName)
        & ipconfig /flushdns | Out-Null
      } catch {
        Write-Stamp ("[WinRM] {0}: DNS flush failed: {1}" -f $ComputerName,$_.Exception.Message)
      }
    }

    Write-Stamp ("[WinRM] {0}: not reachable yet, retrying in {1}s…" -f $ComputerName,$IntervalSeconds)
    Start-Sleep -Seconds $IntervalSeconds
  } while ((Get-Date) -lt $deadline)

  Write-Stamp ("[WinRM] {0}: unreachable after {1}s." -f $ComputerName,$TimeoutSeconds)
  return $false
}


function Invoke-Step {
  param([Parameter(Mandatory)][string]$Name, [Parameter(Mandatory)][scriptblock]$Script)
  Write-Stamp ("▶ {0}" -f $Name)
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  try { $r = & $Script; $sw.Stop(); Write-Stamp ("✓ {0} ({1:g})" -f $Name,$sw.Elapsed); return $r }
  catch { $sw.Stop(); Write-Host ("✗ {0} FAILED after {1:g}: {2}" -f $Name,$sw.Elapsed,$_.Exception.Message) -ForegroundColor Red; throw }
}

# --- Prompts ---------------------------------------------------------------
function Ask { param([string]$Prompt,[string]$Default=$null,[switch]$AllowEmpty)
  while ($true) {
    $full = if ($null -ne $Default -and $Default -ne '') { "${Prompt} [${Default}]: " } else { "${Prompt}: " }
    $ans = Read-Host $full
    if ([string]::IsNullOrWhiteSpace($ans)) { $ans = $Default }
    if ($AllowEmpty -or -not [string]::IsNullOrWhiteSpace($ans)) { return $ans }
  }
}
function AskInt { param([string]$Prompt,[int]$Default)
  while ($true) {
    $v = Ask -Prompt $Prompt -Default $Default
    if ($v -as [int] -ne $null) { return [int]$v }
    Write-Host "Please enter a whole number." -ForegroundColor Yellow
  }
}
function AskChoice { param([string]$Prompt,[string[]]$Choices,[int]$DefaultIndex=0)
  Write-Host $Prompt -ForegroundColor Cyan
  for ($i=0; $i -lt $Choices.Count; $i++) { $m = if ($i -eq $DefaultIndex) { '*' } else { ' ' } ; Write-Host (" [{0}] {1} {2}" -f $i,$Choices[$i],$m) }
  while ($true) {
    $ans = Read-Host "Enter number or value (default $DefaultIndex)"
    if ([string]::IsNullOrWhiteSpace($ans)) { return [string]$Choices[$DefaultIndex] }
    if ($ans -as [int] -ne $null) {
      $idx = [int]$ans
      if ($idx -ge 0 -and $idx -lt $Choices.Count) { return [string]$Choices[$idx] }
    } else {
      $exact = $Choices | Where-Object { $_ -eq $ans }
      if ($exact) { return [string]$exact }
    }
  }
}

# --- JSON helpers ----------------------------------------------------------
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

# --- Folder & vSphere helpers ---------------------------------------------
function Resolve-RootFolder {
  param([Parameter(Mandatory)]$Datacenter,[Parameter(Mandatory)][string]$Name)
  $vmRoot = VMware.VimAutomation.Core\Get-Folder -Name 'vm' -Location $Datacenter -Type VM -ErrorAction Stop | Select-Object -First 1
  $folder = VMware.VimAutomation.Core\Get-Folder -Location $vmRoot -Type VM -ErrorAction SilentlyContinue | Where-Object { $_.Name -eq $Name } | Select-Object -First 1
  if (-not $folder) { $folder = VMware.VimAutomation.Core\New-Folder -Name $Name -Location $vmRoot -ErrorAction Stop }
  return $folder
}
function Ensure-TemplatesFolderStrict { param([Parameter(Mandatory)]$Datacenter,[string]$FolderName='Templates') (Resolve-RootFolder -Datacenter $Datacenter -Name $FolderName) }

function Select-BestDatastoreFromCluster {
  param([Parameter(Mandatory)]$ClusterOrStoragePod)
  $dss = @( VMware.VimAutomation.Core\Get-Datastore -RelatedObject $ClusterOrStoragePod -ErrorAction SilentlyContinue | Where-Object { $_ } )
  $candidates = $dss | Where-Object { $_.State -eq 'Available' } | Sort-Object -Property FreeSpaceGB -Descending
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
  $vmView=VMware.VimAutomation.Core\Get-View -Id $VM.Id
  $spec=New-Object VMware.Vim.VirtualMachineConfigSpec
  $changes=@()
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
  $vmView=VMware.VimAutomation.Core\Get-View -Id $VM.Id
  $cds=@($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.VirtualCdrom] })
  if ($cds.Count -gt 1){
    $spec=New-Object VMware.Vim.VirtualMachineConfigSpec; $changes=@()
    for($i=1;$i -lt $cds.Count;$i++){ $devSpec=New-Object VMware.Vim.VirtualDeviceConfigSpec; $devSpec.operation='remove'; $devSpec.device=$cds[$i]; $changes+=$devSpec }
    $spec.deviceChange=$changes; $vmView.ReconfigVM_Task($spec)|Out-Null
  }
  $vmView=VMware.VimAutomation.Core\Get-View -Id $VM.Id
  $firstCd=(@($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.VirtualCdrom] }))[0]
  if ($firstCd){
    if (-not $firstCd.Connectable){ $firstCd.Connectable=New-Object VMware.Vim.VirtualDeviceConnectInfo }
    $firstCd.Connectable.Connected=$false; $firstCd.Connectable.StartConnected=$false; $firstCd.Connectable.AllowGuestControl=$true
    $spec2=New-Object VMware.Vim.VirtualMachineConfigSpec
    $edit=New-Object VMware.Vim.VirtualDeviceConfigSpec; $edit.operation='edit'; $edit.device=$firstCd
    $spec2.deviceChange=@($edit); $vmView.ReconfigVM_Task($spec2)| Out-Null
  }
}

# --- Tools policy & finalize ------------------------------------------------
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
function Wait-ToolsHealthy {
  param([Parameter(Mandatory)]$VM,[int]$TimeoutSec=180,[int]$PollSec=5)
  $deadline=(Get-Date).AddSeconds($TimeoutSec)
  do{
    try{
      $g=VMware.VimAutomation.Core\Get-VMGuest -VM $VM -ErrorAction Stop
      if($g.State -eq 'Running' -and ($g.ToolsStatus -eq 'toolsOk' -or $g.ToolsStatus -eq 'toolsOld')){ return $g.ToolsStatus }
    } catch{}
    Start-Sleep -Seconds $PollSec
  } while((Get-Date) -lt $deadline)
  return $null
}
function Invoke-FinalizeVm {
  param([Parameter(Mandatory)]$VM,[int]$ToolsWaitSeconds = 60)
  if ($ToolsWaitSeconds -lt 30) { $ToolsWaitSeconds = 30 }
  Write-Stamp "Finalize: enforcing Tools policy, rebooting, upgrading HW, and revalidating Tools…"

  try { Set-ToolsUpgradePolicySafe -VM $VM | Out-Null } catch {}
  try { $null = Wait-ToolsHealthy -VM $VM -TimeoutSec $ToolsWaitSeconds } catch {}

  try {
    $guest = VMware.VimAutomation.Core\Get-VMGuest -VM $VM -ErrorAction SilentlyContinue
    if ($guest -and $guest.State -eq 'Running') {
      Write-Stamp "Finalize: Restarting guest to apply Tools policy…"
      VMware.VimAutomation.Core\Restart-VMGuest -VM $VM -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
      try { $null = Wait-ToolsHealthy -VM $VM -TimeoutSec $ToolsWaitSeconds } catch {}
    }
  } catch {}

  try {
    $vmObj = VMware.VimAutomation.Core\Get-VM -Id $VM.Id -ErrorAction Stop
    if ($vmObj.PowerState -ne 'PoweredOff') {
      Write-Stamp "Finalize: Powering off for HW upgrade…"
      VMware.VimAutomation.Core\Stop-VM -VM $vmObj -Confirm:$false -ErrorAction Stop | Out-Null
    }
  } catch {}

  $hwOk = $false
  try {
    $vmView = VMware.VimAutomation.Core\Get-View -Id $VM.Id -ErrorAction Stop
    $taskRef = $vmView.UpgradeVM_Task($null)   # latest supported
    if ($taskRef) { $hwOk = $true }
  } catch { Write-Warning ("UpgradeVM_Task failed: {0}" -f $_.Exception.Message) }
  if (-not $hwOk) { Write-Warning "Hardware upgrade did not complete; VM will remain at current hardware version." }

  try {
    Write-Stamp "Finalize: Powering on after HW upgrade…"
    VMware.VimAutomation.Core\Start-VM -VM $VM -Confirm:$false -ErrorAction Stop | Out-Null
  } catch {}
  try { $null = Wait-ToolsHealthy -VM $VM -TimeoutSec $ToolsWaitSeconds } catch {}
}

# --- Site suffix & config normalize ----------------------------------------
function Convert-StructuredConfig {
  param([Parameter(Mandatory)]$In)

  function Pick {
    param([object[]]$v)
    foreach ($x in $v) {
      if ($null -ne $x -and "" -ne "$x") { return $x }
    }
    $null
  }

  function Get-Prop($obj, $name) {
    if ($null -eq $obj) { return $null }
    $p = $obj.PSObject.Properties[$name]
    if ($null -ne $p) { return $p.Value }
    return $null
  }

  if ($In -and $In.PSObject.Properties['VMware']) { $vmw=$In.VMware } else { $vmw=$In }
  $ad=$In.AD; $opt=$In.Options; $cl=$vmw.ContentLibrary; $hw=$vmw.Hardware

  $out = [pscustomobject]@{
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
    AdditionalDisks    = Get-Prop $hw 'AdditionalDisks'
    Sockets            = Get-Prop $hw 'Sockets'
    CoresPerSocket     = Get-Prop $hw 'CoresPerSocket'
    ADTargetOU         = Get-Prop $ad 'TargetOU'
    ADServer           = Get-Prop $ad 'Server'
    ADUseVCenterCreds  = Pick @((Get-Prop $ad 'UseVCenterCreds'), $false)
    ADWaitForSeconds   = Pick @((Get-Prop $ad 'WaitForSeconds'), 60)
    ADCredFile         = Get-Prop $ad 'CredFile'
    PowerOn            = Pick @((Get-Prop $opt 'PowerOn'), $true)
    EnsureNICConnected = Pick @((Get-Prop $opt 'EnsureNICConnected'), $true)
    RemoveExtraCDROMs  = Pick @((Get-Prop $opt 'RemoveExtraCDROMs'), $true)
    ToolsWaitSeconds   = Pick @((Get-Prop $opt 'ToolsWaitSeconds'), 60)
    PostPowerOnDelay   = Pick @((Get-Prop $opt 'PostPowerOnDelay'), 15)
    ChangeRequestNumber= Pick @((Get-Prop $In 'ChangeRequestNumber'), (Get-Prop $opt 'ChangeRequestNumber'), $null)
    PostDeploy         = Pick @((Get-Prop $In 'PostDeploy'), @{})
  }

  if ($out.AdditionalDisks -and ($out.AdditionalDisks -isnot [System.Array])) {
    $out.AdditionalDisks = @($out.AdditionalDisks)
  }

  if (-not $out.LocalTemplateName -and $out.TemplateName) {
    $usSite = $null
    if ($out.ADTargetOU -match 'OU=Hawthorne') { $usSite = 'HAW' }
    elseif ($out.ADTargetOU -match 'OU=Marlborough') { $usSite = 'MAR' }

    $region = $null
    if     ($out.Cluster -match '^HAW-Local$')     { $region='US'; $usSite='HAW' }
    elseif ($out.Cluster -match '^MAR-Local$')     { $region='US'; $usSite='MAR' }
    elseif ($out.Cluster -match '^COMPUTE-Local$') { $region='US'; $usSite='COMPUTE' }
    elseif ($out.Cluster -match '^Compute$')       { $region='US' }
    elseif ($out.Cluster -match 'LD9')             { $region='UK' }
    elseif ($out.Cluster -match 'DB4')             { $region='IE' }
    if (-not $region) { $region = 'US' }

    $siteSuffix = $region
    if ($region -eq 'UK') {
      if ($out.Cluster -match 'LD9') { $siteSuffix = 'LD9' }
      else { $siteSuffix = 'UK' }
    } elseif ($region -eq 'IE') {
      $siteSuffix = 'DB4'
    } elseif ($region -eq 'US') {
      if ($usSite) { $siteSuffix = $usSite } else { $siteSuffix = 'US' }
    }


    $out.LocalTemplateName = ('{0}_{1}' -f $out.TemplateName, $siteSuffix)

    if ($region -eq 'US' -and $usSite) {
      $out.TemplatesFolderName = 'Templates'
      if (-not $out.DatastoreCluster) {
        if     ($usSite -eq 'HAW')     { $out.DatastoreCluster = 'HAW_LOCAL_CLUSTER' }
        elseif ($usSite -eq 'MAR')     { $out.DatastoreCluster = 'MAR_LOCAL_CLUSTER' }
        elseif ($usSite -eq 'COMPUTE') { $out.DatastoreCluster = 'DEV_POD_CLUSTER' }
      }
      if (-not $out.Cluster) {
        if     ($usSite -eq 'HAW')     { $out.Cluster = 'HAW-Local' }
        elseif ($usSite -eq 'MAR')     { $out.Cluster = 'MAR-Local' }
        elseif ($usSite -eq 'COMPUTE') { $out.Cluster = 'COMPUTE' }
      }
    }
  }

  return $out
}

$ConfirmPreference = 'None'

# --- Template materialization ----------------------------------------------
function Ensure-LocalTemplateFromCL {
  [CmdletBinding()] param([Parameter(Mandatory)][object]$Config)

  $libWanted   = $Config.ContentLibraryName
  $itemWanted  = $Config.TemplateName
  $tplName     = $Config.LocalTemplateName
  $clusterObj  = Get-Cluster -Name $Config.Cluster -ErrorAction Stop
  $vmHost      = if ($Config.VMHost) { Get-VMHost -Name $Config.VMHost } else { ($clusterObj | Get-VMHost | Select-Object -First 1) }

  $ds = if ($Config.DatastoreCluster) {
          $sp = Get-DatastoreCluster -Name $Config.DatastoreCluster -ErrorAction Stop
          Write-Host "Using datastore cluster '$($sp.Name)'; selecting datastore with most free space..."
          $d = Select-BestDatastoreFromCluster -ClusterOrStoragePod $sp
          Write-Host "Selected datastore: $($d.Name)"; $d
        } elseif ($Config.Datastore) {
          Get-Datastore -Name $Config.Datastore -ErrorAction Stop
        } else {
          Get-Datastore -RelatedObject $vmHost | Sort-Object FreeSpaceGB -Descending | Select-Object -First 1
        }

  Write-Stamp "Materializing local template '$tplName' (from CL item '$itemWanted' in '$libWanted') ..."

  $existingTemplate = Get-Template -Name $tplName -ErrorAction SilentlyContinue
  if ($existingTemplate) {
    Write-Host "Template '$tplName' exists → removing (force overwrite)..." -ForegroundColor Yellow
    try { Remove-Template -Template $existingTemplate -DeletePermanently -Confirm:$false -ErrorAction Stop }
    catch { throw "Failed to remove existing template '$tplName': $($_.Exception.Message)" }
  }

  $existingVM = Get-VM -Name $tplName -ErrorAction SilentlyContinue
  if ($existingVM) {
    Write-Warning "A VM named '$tplName' already exists. I will NOT delete it. If it resides in the Templates folder, the template create/rename may be blocked."
  }

  $seedVM = $null
  try {
    $lib  = Get-ContentLibrary | Where-Object { $_.Name -ieq $libWanted } | Select-Object -First 1
    if (-not $lib) {
      $lib = Get-ContentLibrary | Where-Object { $_.Name -match 'packer|template' } | Select-Object -First 1
      if ($lib) { Write-Host "Library '$libWanted' not found; using '$($lib.Name)' instead." -ForegroundColor Yellow }
    }
    if (-not $lib) { throw "Content library '$libWanted' not found." }

    try {
      if (Get-Command -Name Sync-ContentLibraryItem -ErrorAction SilentlyContinue) {
        Get-ContentLibraryItem -ContentLibrary $lib | ForEach-Object { Sync-ContentLibraryItem -ContentLibraryItem $_ -ErrorAction SilentlyContinue }
      } elseif (Get-Command -Name Update-ContentLibrary -ErrorAction SilentlyContinue) {
        Update-ContentLibrary -ContentLibrary $lib -ErrorAction SilentlyContinue | Out-Null
      }
      Write-Stamp ("Synchronized content library '{0}' (best-effort)." -f $lib.Name)
    } catch {
      Write-Warning ("Content library sync skipped/failed: {0}" -f $_.Exception.Message)
    }

    $items = @( Get-ContentLibraryItem -ContentLibrary $lib )
    $cli = $items | Where-Object { $_.Name -ieq $itemWanted } | Select-Object -First 1
    if (-not $cli) { $cli = $items | Where-Object { $_.Name -like "$itemWanted*" } | Sort-Object CreationTime -Descending | Select-Object -First 1 }
    if (-not $cli) { throw "No CL item matching '$itemWanted' in library '$($lib.Name)'." }
    if ($cli.Name -ne $itemWanted) {
      Write-Host ("Using closest match CL item: {0} (requested: {1})" -f $cli.Name,$itemWanted) -ForegroundColor Yellow
    }

    Write-Stamp ("Using CL item: {0} (library: {1})" -f $cli.Name,$lib.Name)
    $tempVmName = ("__seed_{0}_{1}" -f $tplName, ([guid]::NewGuid().ToString('N').Substring(0,8)))
    $seedVM = New-VM -Name $tempVmName -ContentLibraryItem $cli -VMHost $vmHost -Datastore $ds -WarningAction SilentlyContinue -ErrorAction Stop -Confirm:$false
  } catch {
    throw "Unable to deploy seed VM from CL item '$itemWanted' in '$libWanted': $($_.Exception.Message)"
  }

  Write-Stamp ("Converting '{0}' to local template '{1}' ..." -f $seedVM.Name, $tplName)
  try {
    $template = Set-VM -VM $seedVM -ToTemplate -Name $tplName -Confirm:$false -ErrorAction Stop
  } catch {
    throw "Failed to create template '$tplName'. If a VM with this name exists in the Templates folder, rename/move that VM or choose a different template name. Error: $($_.Exception.Message)"
  }

  if ($Config.TemplatesFolderName) {
    try {
      $dc        = Get-DatacenterForCluster -Cluster $clusterObj
      $dest      = Ensure-TemplatesFolderStrict -Datacenter $dc -FolderName $Config.TemplatesFolderName
      $tView     = Get-View -Id $template.Id
      $parent    = Get-View -Id $tView.Parent
      if ($parent.Name -ne $dest.Name) {
        Move-Template -Template $template -Destination $dest -Confirm:$false | Out-Null
      } else {
        Write-Stamp ("Template already in folder '{0}'; skipping move." -f $dest.Name)
      }
    } catch {
      Write-Warning ("Move-Template check failed: {0}" -f $_.Exception.Message)
    }
  }

  Write-Stamp ("Local template ready: {0}" -f $template.Name)
  return $template
}

# --- Clone from local template ---------------------------------------------
function New-VMFromLocalTemplate {
  [CmdletBinding()] param([Parameter(Mandatory)][object]$Config)

  $tplName   = $Config.LocalTemplateName
  $template  = VMware.VimAutomation.Core\Get-Template -Name $tplName -ErrorAction Stop
  $clusterObj= VMware.VimAutomation.Core\Get-Cluster -Name $Config.Cluster -ErrorAction Stop
  $vmhost    = if ($Config.VMHost) { VMware.VimAutomation.Core\Get-VMHost -Name $Config.VMHost } else { ($clusterObj | VMware.VimAutomation.Core\Get-VMHost | Select-Object -First 1) }
  $ds        = if ($Config.DatastoreCluster) {
                 $sp = VMware.VimAutomation.Core\Get-DatastoreCluster -Name $Config.DatastoreCluster -ErrorAction Stop
                 Select-BestDatastoreFromCluster -ClusterOrStoragePod $sp
               } elseif ($Config.Datastore) {
                 VMware.VimAutomation.Core\Get-Datastore -Name $Config.Datastore -ErrorAction Stop
               } else {
                 VMware.VimAutomation.Core\Get-Datastore -RelatedObject $vmhost | Sort-Object FreeSpaceGB -Descending | Select-Object -First 1
               }

  $dc       = Get-DatacenterForCluster -Cluster $clusterObj
  $vmFolder = $null
  if ($Config.Folder) { $vmFolder = Resolve-RootFolder -Datacenter $dc -Name $Config.Folder }

  $params = @{ Name=$Config.VMName; Template=$template; VMHost=$vmhost; Datastore=$ds; ErrorAction='Stop' }
  if ($vmFolder) { $params['Location'] = $vmFolder }

  Write-Stamp "Cloning VM '$($Config.VMName)' from local template '$tplName' ..."
  $newVM = VMware.VimAutomation.Core\New-VM @params

  if ($Config.Network) {
    try {
      $pgStd = VMware.VimAutomation.Core\Get-VirtualPortGroup -Standard -ErrorAction SilentlyContinue
      $pgDvs = $null; try { $pgDvs = VMware.VimAutomation.Vds\Get-VDPortgroup -ErrorAction SilentlyContinue } catch {}
      $allPG=@(); if($pgStd){$allPG+=$pgStd}; if($pgDvs){$allPG+=$pgDvs}
      $networkObj = $allPG | Where-Object { $_.Name -eq $Config.Network }
      if ($networkObj) {
        $firstNic = VMware.VimAutomation.Core\Get-NetworkAdapter -VM $newVM | Select-Object -First 1
        if ($firstNic) {
          if ($networkObj -is [VMware.VimAutomation.Vds.Types.V1.VDPortgroup]) {
            VMware.VimAutomation.Core\Set-NetworkAdapter -NetworkAdapter $firstNic -PortGroup $networkObj -Confirm:$false | Out-Null
            Write-Stamp ("Network bound to VDS portgroup '{0}'" -f $networkObj.Name)
          } else {
            VMware.VimAutomation.Core\Set-NetworkAdapter -NetworkAdapter $firstNic -NetworkName $Config.Network -Confirm:$false | Out-Null
            Write-Stamp ("Network bound to Standard PG '{0}'" -f $Config.Network)
          }
        }
      }
    } catch { Write-Warning "Post-clone network set failed: $($_.Exception.Message)" }
  }

  if ($Config.RemoveExtraCDROMs){ Ensure-SingleCDHard -VM $newVM }
  if ($Config.EnsureNICConnected){ Set-VMNicStartConnected -VM $newVM }

  return $newVM
}

# --- Tagging: Cohesity & Zerto ---------------------------------------------
function Invoke-ApplyCohesityZertoTags {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)]
    [VMware.VimAutomation.ViCore.Types.V1.Inventory.VirtualMachine]$VM,

    [Parameter(Mandatory)]
    [VMware.VimAutomation.ViCore.Impl.V1.VIServerImpl]$Server,

    [string]$CohesityCategoryName = "Cohesity-VM-Group",
    [string[]]$CohesityTags = @("VM-Group-1","VM-Group-2","VM-Group-3"),

    [string]$ZertoCategoryName = "ZertoAutoProtect",
    [string]$ZertoTag_DB4_to_LD9 = "DB4toLD9AutoProtectVPGDev:AutoProtect",
    [string]$ZertoTag_LD9_to_DB4 = "LD9toDB4AutoProtectVPGDev:AutoProtect",

    [bool]$RebalanceCohesity = $true
  )

  function Write-Stamp { param([string]$msg) Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg) }

  function Get-RegionFromVMName([string]$n) {
    if ([string]::IsNullOrWhiteSpace($n) -or $n.Length -lt 2) { return $null }
    $n.Substring(0,2).ToUpperInvariant()
  }

  function Ensure-TagCategory([string]$Name,[string]$Cardinality='Single',[string[]]$EntityType=@('VirtualMachine'),$Server){
    $existing = Get-TagCategory -Server $Server -Name $Name -ErrorAction SilentlyContinue
    if (-not $existing) {
      New-TagCategory -Server $Server -Name $Name -Cardinality $Cardinality -EntityType $EntityType | Out-Null
    }
  }

  function Ensure-Tags([string]$Category,[string[]]$TagNames,$Server){
    foreach($t in $TagNames){
      if (-not (Get-Tag -Server $Server -Category $Category -Name $t -ErrorAction SilentlyContinue)) {
        New-Tag -Server $Server -Name $t -Category $Category | Out-Null
      }
    }
  }

  function Get-TagCounts([string]$Category,[string[]]$TagNames,$Server){
    $h = @{}
    foreach($t in $TagNames){
      $tagObj = Get-Tag -Server $Server -Category $Category -Name $t -ErrorAction SilentlyContinue
      if ($tagObj) {
        $h[$t] = (Get-VM -Server $Server -Tag $tagObj -ErrorAction SilentlyContinue | Measure-Object).Count
      } else {
        $h[$t] = 0
      }
    }
    $h
  }

  function Choose-LeastLoadedTag($Counts,[string[]]$TagOrder){
    ($TagOrder | Sort-Object { $Counts[$_] }, { [array]::IndexOf($TagOrder, $_) })[0]
  }

  function Assign-Tag-Safely{
    param(
      [Parameter(Mandatory)]$VM,
      [Parameter(Mandatory)][string]$Category,
      [Parameter(Mandatory)][string]$TagName,
      [Parameter(Mandatory)]$Server,
      [bool]$Rebalance = $true
    )
    $existingAny = Get-TagAssignment -Server $Server -Entity $VM -ErrorAction SilentlyContinue |
                   Where-Object { $_.Tag.Category.Name -eq $Category }

    if ($existingAny) {
      if ($existingAny.Tag.Name -eq $TagName) {
        return [pscustomobject]@{
          Succeeded = $false
          Action    = 'NoChange'
          OldTag    = $existingAny.Tag.Name
        }
      }
      if ($Rebalance) {
        $existingAny | ForEach-Object {
          Remove-TagAssignment -TagAssignment $_ -Confirm:$false -ErrorAction SilentlyContinue
        }
      } else {
        return [pscustomobject]@{
          Succeeded = $false
          Action    = 'SkipExisting'
          OldTag    = $existingAny.Tag.Name
        }
      }
    }

    try {
      $tagObj = Get-Tag -Server $Server -Category $Category -Name $TagName -ErrorAction Stop
      New-TagAssignment -Server $Server -Entity $VM -Tag $tagObj -ErrorAction Stop | Out-Null
      [pscustomobject]@{
        Succeeded = $true
        Action    = if ($existingAny) { 'Replaced' } else { 'Assigned' }
        OldTag    = if ($existingAny -and $existingAny.Tag) { $existingAny.Tag.Name } else { $null }
      }
    } catch {
      [pscustomobject]@{
        Succeeded = $false
        Action    = 'Failed'
        OldTag    = $null
        Error     = $_.Exception.Message
      }
    }
  }

  function Get-VM-SiteForZerto([object]$VM,[string]$Region,[object]$Server){
    $cluster = Get-Cluster -Server $Server -VM $VM -ErrorAction SilentlyContinue
    $cn = if ($cluster) { $cluster.Name } else { $null }

    if ($cn) {
      if ($cn -match '(?i)\bLD9\b|London|LD-?9|LDN9') { return 'LD9' }
      if ($cn -match '(?i)\bDB4\b|Dublin|DB-?4')     { return 'DB4' }
    }
    switch -Regex ($Region) {
      '^UK$' { 'LD9' }
      '^IE$' { 'DB4' }
      '^US$' { 'US'  }
      default { $null }
    }
  }

  $PSDefaultParameterValues['*:Confirm'] = $false
  if (-not $Server) { throw "Invoke-ApplyCohesityZertoTags: -Server cannot be null." }
  if (-not $VM)     { throw "Invoke-ApplyCohesityZertoTags: -VM cannot be null." }

  Write-Stamp ("[Tags] Processing '{0}' on vCenter '{1}'..." -f $VM.Name, $Server.Name)

  Ensure-TagCategory $CohesityCategoryName 'Single' @('VirtualMachine') -Server $Server
  Ensure-Tags $CohesityCategoryName $CohesityTags -Server $Server
  Ensure-TagCategory $ZertoCategoryName 'Single' @('VirtualMachine') -Server $Server
  Ensure-Tags $ZertoCategoryName @($ZertoTag_DB4_to_LD9,$ZertoTag_LD9_to_DB4) -Server $Server

  # Cohesity: skip only if VM name starts with "DV"
  if ($VM.Name.ToUpper().StartsWith('DV')) {
    Write-Stamp "[Tags] Cohesity: skipped (name starts with 'DV')."
  } else {
    $counts = Get-TagCounts $CohesityCategoryName $CohesityTags -Server $Server
    $chosen = Choose-LeastLoadedTag $counts $CohesityTags
    $res = Assign-Tag-Safely -VM $VM -Category $CohesityCategoryName -TagName $chosen -Server $Server -Rebalance:$RebalanceCohesity
    Write-Stamp ("[Tags] Cohesity: {0} → {1}" -f $res.Action,$chosen)
  }

  $region = Get-RegionFromVMName $VM.Name
  $site   = Get-VM-SiteForZerto -VM $VM -Region $region -Server $Server
  switch ($site) {
    'LD9' {
      $r = Assign-Tag-Safely -VM $VM -Category $ZertoCategoryName -TagName $ZertoTag_LD9_to_DB4 -Server $Server -Rebalance:$true
      Write-Stamp ("[Tags] Zerto: {0} (LD9→DB4)" -f $r.Action)
    }
    'DB4' {
      $r = Assign-Tag-Safely -VM $VM -Category $ZertoCategoryName -TagName $ZertoTag_DB4_to_LD9 -Server $Server -Rebalance:$true
      Write-Stamp ("[Tags] Zerto: {0} (DB4→LD9)" -f $r.Action)
    }
    'US' { Write-Stamp "[Tags] Zerto: US site — skipped." }
    default { Write-Stamp "[Tags] Zerto: site not inferred — skipped." }
  }
}


# --- AD Join + OU Move (Windows guests) ------------------------------------
function Invoke-ADPlacement {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)]
    [VMware.VimAutomation.ViCore.Types.V1.Inventory.VirtualMachine]$VM,

    [Parameter()][string]$TargetOU,
    [Parameter()][string]$ADServer,

    # Kept so the call-site stays the same, but we won't prompt here
    [Parameter()][switch]$UseVCenterCreds,
    [Parameter()][string]$ADCredFile,

    [Parameter()][int]$WaitForSeconds = 60,

    # Passed in from Deploy-FromCLv2 (same creds you used for vCenter)
    [Parameter()][pscredential]$DomainCredential
  )

  try {
    Write-Stamp ("[AD] OU placement for '{0}'…" -f $VM.Name)

    if (-not $TargetOU) {
      Write-Stamp "[AD] No target OU specified; nothing to do."
      return
    }

    # Decide which creds to use (no prompts here)
    $creds = $null
    if ($UseVCenterCreds -and $DomainCredential) {
      $creds = $DomainCredential           # reuse vCenter creds (domain admin)
    } elseif ($ADCredFile -and (Test-Path $ADCredFile)) {
      try { $creds = Import-Clixml -Path $ADCredFile } catch {}
    }

    if (-not $creds) {
      Write-Warning "[AD] No credentials available (vCenter or CredFile). Skipping OU move."
      return
    }

    # Give AD/DNS a little time after customization join
    if ($WaitForSeconds -gt 0) {
      Write-Stamp ("[AD] Waiting {0}s for AD object to appear…" -f $WaitForSeconds)
      Start-Sleep -Seconds $WaitForSeconds
    }

    # OU move from the management host (requires RSAT ActiveDirectory)
    try {
      Import-Module ActiveDirectory -ErrorAction Stop

      $getParams = @{
        Identity    = $VM.Name
        ErrorAction = 'Stop'
      }
      if ($ADServer)     { $getParams['Server']     = $ADServer }
      if ($creds)        { $getParams['Credential'] = $creds }

      $comp = Get-ADComputer @getParams
      $currentDN = $comp.DistinguishedName

      if ($currentDN -notmatch [regex]::Escape($TargetOU)) {
        $moveParams = @{
          Identity    = $comp.DistinguishedName
          TargetPath  = $TargetOU
          Confirm     = $false
          ErrorAction = 'Stop'
        }
        if ($ADServer) { $moveParams['Server']     = $ADServer }
        if ($creds)    { $moveParams['Credential'] = $creds }

        Move-ADObject @moveParams
        Write-Stamp ("[AD] Moved computer '{0}' to '{1}'." -f $VM.Name,$TargetOU)
      } else {
        Write-Stamp "[AD] Already in target OU."
      }
    } catch {
      Write-Warning ("[AD] OU move failed: {0}" -f $_.Exception.Message)
    }

  } catch {
    Write-Warning ("[AD] Unexpected error: {0}" -f $_.Exception.Message)
  }
}

# --- Post: CrowdStrike + SCOM installer ------------------------------------

# --- SCOM remote push helper (install / remove / check + approve) ----------
$script:ScomMgmtServer = 'UKPRAP184.bfl.local'
$script:ScomCredential = $null

function Invoke-SCOMAgentPush {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [ValidateSet('Install','Remove','Check')]
        [string]$Action,

        [Parameter(Mandatory)]
        [string]$ServerName,

        [string]$SCOMServer = $script:ScomMgmtServer,

        [pscredential]$Credential
    )

    if (-not $Credential) {
        Write-Host "Enter credential for remote SCOM server connection (e.g. bfl\a1_wg7)" -ForegroundColor Cyan
        $Credential = Get-Credential
    }

    $sb = {
        param($Action,$ServerName,$SCOMServer)

        Import-Module OperationsManager -ErrorAction Stop
        try { Import-Module ActiveDirectory -ErrorAction SilentlyContinue | Out-Null } catch {}

        function Resolve-FQDN {
            param([string]$Name)
            try {
                $obj = Get-ADComputer $Name -Properties DNSHostName -ErrorAction Stop
                return $obj.DNSHostName
            } catch {
                return "$Name.bfl.local"
            }
        }

        function Connect-MG {
            param([string]$MgmtSrv)
            if (-not (Get-SCOMManagementGroupConnection -ErrorAction SilentlyContinue)) {
                New-SCOMManagementGroupConnection -ComputerName $MgmtSrv | Out-Null
            }
        }

        function Get-AgentObject {
            param([string]$FQDN,[string]$Short)
            return Get-SCOMAgent | Where-Object {
                $_.DNSHostName -eq $FQDN -or
                $_.DNSHostName -eq $Short -or
                $_.Name        -eq $FQDN -or
                $_.Name        -eq $Short
            }
        }

        function Approve-Pending {
            param([string]$Short)
            Start-Sleep 10
            $pending = Get-SCOMPendingManagement | Where-Object { $_.Computer -like "$Short*" }
            if ($pending) {
                Approve-SCOMPendingManagement -Instance $pending -ErrorAction SilentlyContinue
                return $true
            }
            return $false
        }

        # --- EXECUTION -------------------------------------------------------
        Connect-MG -MgmtSrv $SCOMServer
        $mgmtObj  = Get-SCOMManagementServer -Name $SCOMServer -ErrorAction Stop
        $fqdn     = Resolve-FQDN $ServerName
        $agentObj = Get-AgentObject -FQDN $fqdn -Short $ServerName

        switch ($Action) {

            'Check' {
                if ($agentObj) { return "[$ServerName] REGISTERED" }
                else           { return "[$ServerName] NOT REGISTERED" }
            }

            'Install' {
                if ($agentObj) {
                    Enable-SCOMAgentProxy -Agent $agentObj -ErrorAction SilentlyContinue
                    return "[$ServerName] Already registered."
                }

                Install-SCOMAgent -DNSHostName $fqdn -PrimaryManagementServer $mgmtObj -ErrorAction Stop

                if (Approve-Pending -Short $ServerName) {
                    Start-Sleep 8
                    $agentObj = Get-AgentObject -FQDN $fqdn -Short $ServerName

                    if ($agentObj) {
                        Enable-SCOMAgentProxy -Agent $agentObj -ErrorAction SilentlyContinue
                        return "[$ServerName] SUCCESS: Installed + Registered"
                    }
                }
                return "[$ServerName] WARNING: Installed but not visible yet"
            }

            'Remove' {
                if (-not $agentObj) { return "[$ServerName] No agent found." }
                Uninstall-SCOMAgent -Agent $agentObj -Confirm:$false -ErrorAction Stop
                return "[$ServerName] SUCCESS: Removed"
            }
        }
    }

    $result = Invoke-Command -ComputerName $SCOMServer -Credential $Credential `
                              -ScriptBlock $sb `
                              -ArgumentList $Action,$ServerName,$SCOMServer

    return $result
}

# --- Post: CrowdStrike + SCOM installer (using SCOM push) ------------------
function Install-CS-And-SCOM-IfNeeded {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][VMware.VimAutomation.ViCore.Types.V1.Inventory.VirtualMachine]$VM,

    # CrowdStrike bits
    [Parameter()][string]$LocalCS = "C:\Beazley\Software\Crowdstrike\FalconSensor_Windows.exe",
    [Parameter()][string]$CS_CID = "ADB1C14C8F2B4BF6BAAE8ACC90511E6C-71",

    # Legacy SCOM MSI params (kept for backwards compat but not used anymore)
    [Parameter()][string]$LocalSCOM = "C:\Beazley\Software\SCOM\MOMAgent.msi",
    [Parameter()][string]$SCOM_MG = "SCOMPROD",
    [Parameter()][string]$SCOM_MGMT_SVR = "UKPRAP184.bfl.local",

    [Parameter()][pscredential]$Credential
  )

  try {
    $isPR   = ($VM.Name -match 'PR')     # Only PR servers get SCOM
    $target = $VM.Name

    # Wait for WinRM before attempting installers (retry helper)
    $wsok = $false
    try {
      $wsok = Test-WSManWithRetry -ComputerName $target `
                            -TimeoutSeconds 600 `
                            -IntervalSeconds 20 `
                            -FlushDnsOnRetry
    } catch {}
    if (-not $wsok) {
      Write-Stamp ("[Installers] {0}: WinRM unreachable after retries, skipping installers." -f $target)
      return
    }


    if (-not $Credential) {
      $Credential = Get-Credential -Message "[Installers] Creds for $target"
    }

    $Remote_CS_Folder   = "C:\Beazley\Software\Crowdstrike"
    $Remote_CS_Exe      = Join-Path $Remote_CS_Folder "FalconSensor_Windows.exe"

    if (-not (Test-Path $LocalCS)) {
      Write-Warning "[Installers] CrowdStrike not found at $LocalCS"
    } else {
      # --------------------- CrowdStrike Install ----------------------------
      $sess = New-PSSession -ComputerName $target -Credential $Credential
      try {
        Invoke-Command -Session $sess -ScriptBlock {
          param($csFolder)
          if (-not (Test-Path $csFolder)) {
            New-Item -Path $csFolder -ItemType Directory -Force | Out-Null
          }
        } -ArgumentList $Remote_CS_Folder

        $needCopyCS = Invoke-Command -Session $sess -ScriptBlock {
          param($p) -not (Test-Path $p)
        } -ArgumentList $Remote_CS_Exe

        if ($needCopyCS) {
          Copy-Item -ToSession $sess -Path $LocalCS -Destination $Remote_CS_Exe -Force
        }

        $cs = Invoke-Command -Session $sess -ScriptBlock {
          param($exe,$cid)
          $svc = Get-Service -Name CSFalconService -ErrorAction SilentlyContinue
          if ($svc) { return "CS already ($($svc.Status))" }
          if (-not (Test-Path $exe)) { return "CS skipped (installer missing)" }

          $args = "/install /quiet /norestart CID=$cid"
          $p = Start-Process -FilePath $exe -ArgumentList $args -Wait -PassThru
          if ($p.ExitCode -ne 0) { return "CS install failed ($($p.ExitCode))" }

          Start-Sleep -Seconds 3
          $svc2 = Get-Service -Name CSFalconService -ErrorAction SilentlyContinue
          if ($svc2) {
            if ($svc2.Status -ne 'Running') { Start-Service CSFalconService -ErrorAction SilentlyContinue }
            $svc2.Refresh()
            return "CS installed ($($svc2.Status))"
          }
          return "CS installed (svc not detected yet)"
        } -ArgumentList $Remote_CS_Exe,$CS_CID

        Write-Stamp ("[Installers] {0}: {1}" -f $target,$cs)
      } finally {
        if ($sess) { Remove-PSSession $sess }
      }
    }

    # ------------------------- SCOM via PUSH from SCOM server ---------------
    if ($isPR) {
      if (-not $script:ScomCredential) {
        $script:ScomMgmtServer = $SCOM_MGMT_SVR
        $script:ScomCredential = Get-Credential -Message ("[Installers] Creds for SCOM server {0}" -f $script:ScomMgmtServer)
      }

      Write-Stamp ("[Installers] {0}: requesting SCOM push install from {1}..." -f $target,$script:ScomMgmtServer)
      $scomResult = Invoke-SCOMAgentPush -Action 'Install' `
                                         -ServerName $target `
                                         -SCOMServer $script:ScomMgmtServer `
                                         -Credential $script:ScomCredential

      Write-Stamp ("[Installers] {0}: SCOM result → {1}" -f $target,$scomResult)
    } else {
      Write-Stamp ("[Installers] {0}: SCOM skipped (not PR server)." -f $target)
    }

  } catch {
    Write-Warning ("[Installers] {0}: {1}" -f $VM.Name,$_.Exception.Message)
  }
}


# --- Disk helpers: PVSCSI + Round Robin ------------------------------------
function Ensure-PvScsiControllers {
  param([Parameter(Mandatory)]$VM,[int[]]$Controllers=(0,1,2,3))
  $vmView = Get-View -Id $VM.Id
  $have = @($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.ParaVirtualSCSIController] })
  $haveNumbers = $have|ForEach-Object{$_.BusNumber}
  $add = $Controllers | Where-Object { $haveNumbers -notcontains $_ }
  if (-not $add) { return }
  $spec = New-Object VMware.Vim.VirtualMachineConfigSpec
  $changes = @()
  foreach($n in $add){
    $ctrl = New-Object VMware.Vim.ParaVirtualSCSIController
    $ctrl.BusNumber = $n
    $ctrl.SharedBus = 'noSharing'
    $ctrl.Key = -100 - $n
    $devSpec = New-Object VMware.Vim.VirtualDeviceConfigSpec
    $devSpec.Operation = 'add'
    $devSpec.Device = $ctrl
    $changes += $devSpec
  }
  $spec.DeviceChange = $changes
  $vmView.ReconfigVM_Task($spec) | Out-Null
}

function Add-DataDisks-RoundRobin {
  param(
    [Parameter(Mandatory)]$VM,
    [Parameter(Mandatory)][int[]]$SizesGB,
    [int[]]$Controllers = (1,2,3)
  )
  Ensure-PvScsiControllers -VM $VM -Controllers (0,1,2,3)
  $i = 0
  foreach($sz in $SizesGB){
    $bus = $Controllers[$i % $Controllers.Count]
    New-HardDisk -VM $VM -CapacityGB $sz -StorageFormat Thin -Confirm:$false -ErrorAction Stop | Out-Null
    $vmView = Get-View -Id $VM.Id
    $disks = @($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.VirtualDisk] })
    $last = $disks | Sort-Object { $_.Key } | Select-Object -Last 1
    $ctrl = @($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.ParaVirtualSCSIController] -and $_.BusNumber -eq $bus })[0]
    if ($last -and $ctrl) {
      $spec = New-Object VMware.Vim.VirtualMachineConfigSpec
      $edit = New-Object VMware.Vim.VirtualDeviceConfigSpec
      $edit.Operation = 'edit'
      $last.ControllerKey = $ctrl.Key
      $edit.Device = $last
      $spec.DeviceChange = @($edit)
      $vmView.ReconfigVM_Task($spec) | Out-Null
    }
    $i++
  }
}

# --- Cleanup: Remove from AD, shutdown & delete VM --------------------------
function Remove-VMAndAD {
  [CmdletBinding()]
  param(
    [Parameter()][string]$VMName,
    [Parameter()][string]$ADServer
  )
  try {
    if (-not $VMName) { $VMName = Ask -Prompt 'VM name to remove' }
    if ([string]::IsNullOrWhiteSpace($VMName)) { Write-Warning "No VM name provided. Aborting."; return }

    Write-Stamp ("[Cleanup] Starting cleanup for VM '{0}'" -f $VMName)

    $adRemoved = $false
    try {
      Import-Module ActiveDirectory -ErrorAction Stop

      if (-not $ADServer -or [string]::IsNullOrWhiteSpace($ADServer)) {
        $ADServer = Ask -Prompt 'AD Server (blank to auto)' -AllowEmpty
      }

      $creds = $null
      try { $creds = Get-Credential -Message "[AD] Credentials to remove computer '$VMName' (e.g. BFL\\admin)" } catch {}

      $adParams = @{ Identity = $VMName; ErrorAction = 'Stop' }
      if ($ADServer) { $adParams['Server'] = $ADServer }
      if ($creds)    { $adParams['Credential'] = $creds }

      $comp = $null
      try { $comp = Get-ADComputer @adParams } catch {}

      if ($comp) {
        $rmParams = @{ Identity = $comp.DistinguishedName; Confirm = $false; ErrorAction='Stop' }
        if ($ADServer) { $rmParams['Server'] = $ADServer }
        if ($creds)    { $rmParams['Credential'] = $creds }
        Remove-ADComputer @rmParams
        Write-Stamp ("[Cleanup] AD computer removed: {0}" -f $comp.DistinguishedName)
        $adRemoved = True
      } else {
        Write-Stamp "[Cleanup] AD computer not found (skip)."
      }
    } catch {
      Write-Warning ("[Cleanup] AD removal failed/skipped: {0}" -f $_.Exception.Message)
    }

    $vm = VMware.VimAutomation.Core\Get-VM -Name $VMName -ErrorAction SilentlyContinue
    if (-not $vm) {
      Write-Stamp "[Cleanup] VM not found in vCenter (skip delete)."
    } else {
      try {
        if ($vm.PowerState -eq 'PoweredOn') {
          Write-Stamp "[Cleanup] Requesting guest shutdown…"
          try { VMware.VimAutomation.Core\Shutdown-VMGuest -VM $vm -Confirm:$false -ErrorAction SilentlyContinue | Out-Null } catch {}
          $deadline = (Get-Date).AddSeconds(60)
          while ($vm.PowerState -ne 'PoweredOff' -and (Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 5
            $vm = VMware.VimAutomation.Core\Get-VM -Id $vm.Id -ErrorAction SilentlyContinue
          }
          if ($vm.PowerState -ne 'PoweredOff') {
            Write-Stamp "[Cleanup] Forcing power off…"
            VMware.VimAutomation.Core\Stop-VM -VM $vm -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
          }
        }
      } catch {
        Write-Warning ("[Cleanup] Power off step warning: {0}" -f $_.Exception.Message)
      }

      try {
        Write-Stamp "[Cleanup] Deleting VM from inventory & disk…"
        VMware.VimAutomation.Core\Remove-VM -VM $vm -DeletePermanently -Confirm:$false -ErrorAction Stop | Out-Null
        Write-Stamp "[Cleanup] VM deleted."
      } catch {
        Write-Warning ("[Cleanup] Remove-VM failed: {0}" -f $_.Exception.Message)
      }
    }

    if ($adRemoved) {
      Write-Host ("✅ Cleanup complete for '{0}' (AD removed, VM deleted where found)." -f $VMName) -ForegroundColor Green
    } else {
      Write-Host ("✅ Cleanup complete for '{0}' (VM deleted where found; AD remove may have been skipped/failed)." -f $VMName) -ForegroundColor Green
    }

  } catch {
    Write-Warning ("[Cleanup] Unexpected error: {0}" -f $_.Exception.Message)
  }
}

# --- Builder (interactive) --------------------------------------------------

function Build-DeployConfig {

  $RegionToVCSA = @{ 'UK'='ukprim098.bfl.local'; 'US'='usprim004.bfl.local'; 'IE'='ieprim018.bfl.local' }
  $RegionToContentLib = @{ 'UK'='Template Library'; 'IE'='DB4 Packer Templates'; 'US'='Packer templates' }

  $NetworkMap = @{
    'US|DEV' = 'VLAN_2032_Stretch'; 'US|PROD' = 'VLAN_2002_Stretch';
    'UK|PROD' = 'BZY|ProdVMs_AP|ProdVMs_EPG'; 'UK|DEV' = 'BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG';
    'IE|DEV' = 'BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG'; 'IE|PROD' = 'BZY|ProdVMs_AP|ProdVMs_EPG'
  }

  $USOptions = @(
    @{ Name='US Stretched'; Cluster='Compute';       DSCluster='PROD_POD_CLUSTER'; Site=$null },
    @{ Name='US HAW';       Cluster='HAW-Local';     DSCluster='HAW_LOCAL_CLUSTER'; Site='HAW'  },
    @{ Name='US MAR';       Cluster='MAR-Local';     DSCluster='MAR_LOCAL_CLUSTER'; Site='MAR'  },
    @{ Name='US COMPUTE';   Cluster='Compute'; DSCluster='DEV_POD_CLUSTER'; Site='COMPUTE' }
  )

  $OU = @{
    'UK|DEV'      = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=London,OU=Accounts Computer,DC=bfl,DC=local';
    'UK|PROD'     = 'OU=Application Servers,OU=Servers & Exceptions,OU=London,OU=Accounts Computer,DC=bfl,DC=local';
    'IE|DEV'      = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Ireland,OU=Accounts Computer,DC=bfl,DC=local';
    'IE|PROD'     = 'OU=Application Servers,OU=Servers & Exceptions,OU=Ireland,OU=Accounts Computer,DC=bfl,DC=local';
    'US|DEV|HAW'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local';
    'US|PROD|HAW' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local';
    'US|DEV|MAR'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local';
    'US|PROD|MAR' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local';
    'US|DEV|COMPUTE'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local';
    'US|PROD|COMPUTE' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local'
  }

  $ADServerMap = @{
    'UK'          = 'UKPRDC011.bfl.local';
    'IE'          = 'IEPRDC010.bfl.local';
    'US|HAW'      = 'USPRDC036.bfl.local';
    'US|MAR'      = 'USPRDC036.bfl.local';
    'US|COMPUTE'  = 'USPRDC036.bfl.local'
  }

  $TemplateItemMap = [ordered]@{
    'Windows 2022'   = [ordered]@{ UK='PKR_windows_server_2022_std_Current'; IE='PKR_windows_server_2022_std_Current'; US='PKR_windows_server_2022_std_Current' };
    'Windows 2019'   = [ordered]@{ UK='PKR_windows_server_2019_std_Current'; IE='PKR_windows_server_2019_std_Current'; US='PKR_windows_server_2019_std_Current' };
    'Linux CentOS 7' = [ordered]@{ UK='PKR_centos-7_Current'; IE='PKR_centos-7_Current'; US='PKR_centos-7_Current' };
    'Linux RHEL 7'   = [ordered]@{ UK='PKR_redhat-7_Current'; IE='PKR_redhat-7_Current'; US='PKR_redhat-7_Current' };
    'Linux RHEL 9'   = [ordered]@{ UK='PKR_redhat-9_Current'; IE='PKR_redhat-9_Current'; US='PKR_redhat-9_Current' };
    'Linux SLES 15'  = [ordered]@{ UK='sles-15-sp4_Current'; IE='sles-15-sp4_Current'; US='sles-15-sp4_Current' };
  }

  $SpecMap = @{
    'Windows 2022'=@{ UK='Win2022_Template_LD9'; IE='Win2022_Template_DB4'; US='Win2022_Template' };
    'Windows 2019'=@{ UK='Win2019_Template_LD9'; IE='Win2019_Template_DB4'; US='Win2019_Template' };
    'Linux CentOS 7'=@{ UK=$null; IE=$null; US=$null };
    'Linux RHEL 7'  =@{ UK=$null; IE=$null; US=$null };
    'Linux RHEL 9'  =@{ UK=$null; IE=$null; US=$null };
    'Linux SLES 15' =@{ UK=$null; IE=$null; US=$null };
  }

  $region = AskChoice -Prompt 'Select VCSA Region' -Choices @('UK','US','IE') -DefaultIndex 0
  $vcsa = $RegionToVCSA[$region]
  $clib = $RegionToContentLib[$region]

  $env = AskChoice -Prompt 'Environment' -Choices @('PROD','DEV') -DefaultIndex 1
  $network = $NetworkMap["$region|$env"]

  $vmFolder = if ($env -eq 'PROD') { 'Production' } else { 'Development' }
  $templatesFolder = 'Templates'

  $cluster=$null; $dsCluster=$null; $usSite=$null
  if ($region -eq 'UK') { $cluster='LD9Compute'; $dsCluster='LD9_DatastoreCluster' }
  elseif ($region -eq 'IE') { $cluster='DB4Compute'; $dsCluster='DB4_DatastoreCluster' }
  else {
    $picked = AskChoice -Prompt 'US Cluster' -Choices (@('US Stretched','US HAW','US MAR','US COMPUTE')) -DefaultIndex 0
    $row = $USOptions | Where-Object { $_.Name -eq $picked } | Select-Object -First 1
    $cluster=$row.Cluster; $dsCluster=$row.DSCluster; $usSite=$row.Site
    if ($row.Site -eq $null -and $picked -eq 'US Stretched') {
      $usSite = AskChoice -Prompt 'Select US site for OU' -Choices @('HAW','MAR') -DefaultIndex 0
    }
  }
  $ouKey = if ($region -eq 'US') { "$region|$env|$usSite" } else { "$region|$env" }
  $adOU = $null
  if ($OU.ContainsKey($ouKey)) { $adOU = $OU[$ouKey] }
  $adServer = if ($region -eq 'US') { $ADServerMap["US|$usSite"] } else { $ADServerMap[$region] }

  $osChoices = @('Windows 2022','Windows 2019','Linux CentOS 7','Linux RHEL 7','Linux RHEL 9','Linux SLES 15')
  $tmplChoice = AskChoice -Prompt 'Choose OS / Template' -Choices $osChoices -DefaultIndex 0

  # NEW — determine if OS is Windows
  $osIsWindows = $tmplChoice -like 'Windows*'

  $templateItem = $TemplateItemMap[$tmplChoice][$region]
  $customSpec = $SpecMap[$tmplChoice][$region]

  $cpu = AskInt -Prompt 'Total vCPU' -Default 4
  if ($cpu -gt 48) {
    if (($cpu % 2) -ne 0) { Write-Host 'CPU > 48 → 2 sockets; rounding vCPU up to even.' -ForegroundColor Yellow; $cpu++ }
    $sockets=2; $coresPerSocket=[int]($cpu/2)
  } else { $sockets=1; $coresPerSocket=$cpu }
  $memGB = AskInt -Prompt 'Memory (GB)' -Default 12
  $diskGB = AskInt -Prompt 'System disk (GB)' -Default 100

  $addCount = AskInt -Prompt 'How many additional data disks?' -Default 0
  $additionalDisks = @()
  for ($i=1; $i -le $addCount; $i++){
    $sz = AskInt -Prompt (" Size of additional disk #{0} (GB)" -f $i) -Default 50
    $additionalDisks += $sz
  }

  do {
    $vmName1 = Ask -Prompt 'VM Name'
    $vmName2 = Ask -Prompt 'Confirm VM Name'
    if ($vmName1 -ne $vmName2){ Write-Host 'Names did not match. Try again.' -ForegroundColor Yellow }
  } until ($vmName1 -eq $vmName2)
  $vmName = $vmName1

  $changeNum = Ask -Prompt 'Change Number (goes into VM Notes)' -AllowEmpty

  # NEW — build LocalAdmin groups (global + per-VM)
  $localAdminGroups = @(
    'BFL\Server Admins',
    ("BFL\{0}-LocalAdmins" -f $vmName)
  )

  $vmw = [ordered]@{
    VCSA=$vcsa; Cluster=$cluster; VMHost=$null; VMFolder=$vmFolder; TemplatesFolderName=$templatesFolder;
    Network=$network; Datastore=''; DatastoreCluster=$dsCluster;
    ContentLibrary=[ordered]@{ Library=$clib; Item=$templateItem; LocalTemplateName=$null; ForceReplace=$true };
    VMName=$vmName; CustomizationSpec=$customSpec;
    Hardware=[ordered]@{ CPU=$cpu; MemoryGB=$memGB; DiskGB=$diskGB; AdditionalDisks=$additionalDisks; Sockets=$sockets; CoresPerSocket=$coresPerSocket }
  }

  $out = [ordered]@{
    VMware  = $vmw
    Options = [ordered]@{
      PowerOn           = $true
      EnsureNICConnected= $true
      RemoveExtraCDROMs = $true
      ToolsWaitSeconds  = 60
      PostPowerOnDelay  = 15
    }
    PostDeploy = [ordered]@{
      EnableTags        = $true
      EnableInstallers  = $osIsWindows          # Only install on Windows
      LocalAdminGroups  = $localAdminGroups     # Default domain admin groups
      PowerPlan         = 'High performance'
    }

  }

  # Only include AD block if this is a Windows OS choice
  if ($osIsWindows) {
    $out.AD = [ordered]@{
      TargetOU        = $adOU
      Server          = $adServer
      UseVCenterCreds = $true
      WaitForSeconds  = 60
    }
  }

  if ($changeNum) { $out.ChangeRequestNumber = $changeNum }

  $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } elseif ($MyInvocation.MyCommand.Path) { Split-Path -Parent $MyInvocation.MyCommand.Path } else { (Get-Location).Path }
  $outPath = Join-Path $scriptDir ("{0}.json" -f $vmName)
  ($out | ConvertTo-Json -Depth 8) | Set-Content -Path $outPath -Encoding UTF8 -NoNewline
  Write-Host "Saved configuration → $outPath" -ForegroundColor Green
  return $outPath
}
function Ensure-LocalAdminADGroup {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][string]$ComputerName,
    [Parameter()][pscredential]$Credential,
    # UPDATED default OU for all local admin groups
    [Parameter()][string]$GroupOU = "OU=Local Admin Access,OU=Shared Groups,DC=bfl,DC=local"
  )

  try {
    Import-Module ActiveDirectory -ErrorAction Stop
  } catch {
    Write-Warning "[AD] ActiveDirectory module not available; cannot create local admin group."
    return $null
  }

  $groupName = "$ComputerName-LocalAdmins"
  $sam       = $groupName

  try {
    $getParams = @{
      Filter      = "SamAccountName -eq '$sam'"
      ErrorAction = 'Stop'
    }
    if ($Credential) { $getParams['Credential'] = $Credential }

    $existing = Get-ADGroup @getParams -ErrorAction SilentlyContinue
    if ($existing) {
      Write-Stamp ("[AD] Local admin group '{0}' already exists." -f $existing.SamAccountName)
      return $existing
    }
  } catch {
    # ignore lookup failure; we’ll try to create
  }

  try {
    $newParams = @{
      Name          = $groupName
      SamAccountName= $sam
      GroupScope    = 'Global'
      GroupCategory = 'Security'
      Path          = $GroupOU
      ErrorAction   = 'Stop'
    }
    if ($Credential) { $newParams['Credential'] = $Credential }

    $new = New-ADGroup @newParams
    Write-Stamp ("[AD] Created local admin group '{0}' in '{1}'." -f $new.SamAccountName, $GroupOU)
    return $new
  } catch {
    Write-Warning ("[AD] Failed to create local admin group '{0}': {1}" -f $sam,$_.Exception.Message)
    return $null
  }
}


function Invoke-PostDeployGuestConfig {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][VMware.VimAutomation.ViCore.Types.V1.Inventory.VirtualMachine]$VM,
    [Parameter()][string[]]$LocalAdminGroups = @(),
    [Parameter()][string]$PowerPlan,
    [Parameter()][pscredential]$Credential
  )


  try {
    $target = $VM.Name

    # Reuse an existing credential if the caller passed one; otherwise prompt once
    if (-not $Credential) { $Credential = Get-Credential -Message "[PostDeploy] Creds for $target" }

    # Wait for WinRM if we need to touch local groups or power plans
    $wsok = $false
    try {
      $wsok = Test-WSManWithRetry -ComputerName $target `
                            -TimeoutSeconds 600 `
                            -IntervalSeconds 20 `
                            -FlushDnsOnRetry

    } catch {}
    if (-not $wsok) {
      Write-Stamp ("[PostDeploy] {0}: WinRM unreachable after retries, skipping guest config." -f $target)
      # NOTE: Ivanti / AD bits from the management host can still run if you keep them outside
      $LocalAdminGroups = @()
      $PowerPlan        = $null
    }


    $sess = $null
    if ($LocalAdminGroups.Count -or $PowerPlan) {
      $sess = New-PSSession -ComputerName $target -Credential $Credential
    }

    try {
      # 1) Add domain groups to local Administrators on the guest
      if ($sess -and $LocalAdminGroups -and $LocalAdminGroups.Count -gt 0) {
        Invoke-Command -Session $sess -ScriptBlock {
          param([string[]]$groups)
          Import-Module Microsoft.PowerShell.LocalAccounts -ErrorAction SilentlyContinue
          foreach ($g in $groups) {
            try {
              $exists = (Get-LocalGroupMember -Group 'Administrators' -ErrorAction SilentlyContinue |
                         Where-Object { $_.Name -eq $g })
              if (-not $exists) {
                Add-LocalGroupMember -Group 'Administrators' -Member $g -ErrorAction Stop
                Write-Output "[LocalAdmins] Added $g"
              } else {
                Write-Output "[LocalAdmins] $g already present"
              }
            } catch {
              Write-Output "[LocalAdmins] $g failed: $($_.Exception.Message)"
            }
          }
        } -ArgumentList @($LocalAdminGroups)
      }

      # 2) Set power plan
      if ($sess -and $PowerPlan) {
        $plan = $PowerPlan.Trim().ToLowerInvariant()
        $guid = switch ($plan) {
          'high performance' { '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c' }
          'balanced'         { '381b4222-f694-41f0-9685-ff5bb260df2e' }
          'power saver'      { 'a1841308-3541-4fab-bc81-f71556f20b4a' }
          default            { $null }
        }
        if ($guid) {
          Invoke-Command -Session $sess -ScriptBlock {
            param($g) powercfg -setactive $g
          } -ArgumentList $guid
          Write-Stamp ("[PowerPlan] Set to '{0}'" -f $PowerPlan)
        } else {
          Write-Warning ("[PowerPlan] Unknown plan '{0}' (expected: Balanced | High performance | Power saver)" -f $PowerPlan)
        }
      }

    } finally {
      if ($sess) { Remove-PSSession $sess }
    }

  } catch {
    Write-Warning ("[PostDeploy] {0}: {1}" -f $VM.Name,$_.Exception.Message)
  }
}

function Set-IvantiPatchGroupForServer {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][string]$ComputerName
  )

  try {
    Import-Module ActiveDirectory -ErrorAction Stop
  } catch {
    Write-Warning "[Ivanti] ActiveDirectory module not available; skipping Ivanti patch group assignment."
    return
  }

  function Get-RoleFromName {
    param([string]$Name)
    if ($Name -match '(?i)\bDB\b' -or $Name -match '(?i)DB') { return 'DB' }
    elseif ($Name -match '(?i)\bAP\b' -or $Name -match '(?i)AP') { return 'AP' }
    else { return 'Other' }
  }

  function Get-RegionEnvFromName {
    param([string]$Name)
    $u = $Name.ToUpper()
    if ($u -match '^UKPR' -or $u -match '^IEPR') { return @{ Region='EMEA'; Env='Prod' } }
    if ($u -match '^UKDV' -or $u -match '^IEDV') { return @{ Region='EMEA'; Env='NonProd' } }
    if ($u -match '^USPR') { return @{ Region='US'; Env='Prod' } }
    if ($u -match '^USDV') { return @{ Region='US'; Env='NonProd' } }
    return $null
  }

  function Get-TargetGroup {
    param([hashtable]$Meta, [string]$Role)

    switch ($Meta.Region) {
      'EMEA' {
        switch ($Role) {
          'DB'    { return 'app.WindowsUpdate.EMEA.SQLServers' }
          'AP'    { if ($Meta.Env -eq 'Prod') { return 'app.WindowsUpdate.EMEA.ProdServers.4' } else { return 'app.WindowsUpdate.EMEA.NonProdServers.2' } }
          default { if ($Meta.Env -eq 'Prod') { return 'app.WindowsUpdate.EMEA.ProdServers.1' } else { return 'app.WindowsUpdate.EMEA.NonProdServers.1' } }
        }
      }
      'US' {
        switch ($Role) {
          'DB'    { return 'app.WindowsUpdate.US.SQLServers' }
          'AP'    { if ($Meta.Env -eq 'Prod') { return 'app.WindowsUpdate.US.ProdServers.4' } else { return 'app.WindowsUpdate.US.NonProdServers.2' } }
          default { if ($Meta.Env -eq 'Prod') { return 'app.WindowsUpdate.US.ProdServers.1' } else { return 'app.WindowsUpdate.US.NonProdServers.1' } }
        }
      }
    }
  }

  $SchedulingSets = @{
    'EMEA' = @(
      'app.WindowsUpdate.EMEA.SQLServers',
      'app.WindowsUpdate.EMEA.ProdServers.1',
      'app.WindowsUpdate.EMEA.ProdServers.2',
      'app.WindowsUpdate.EMEA.ProdServers.3',
      'app.WindowsUpdate.EMEA.ProdServers.4',
      'app.WindowsUpdate.EMEA.ProdServers.5',
      'app.WindowsUpdate.EMEA.ProdServers.6',
      'app.WindowsUpdate.EMEA.NonProdServers.1',
      'app.WindowsUpdate.EMEA.NonProdServers.2'
    )
    'US' = @(
      'app.WindowsUpdate.US.SQLServers',
      'app.WindowsUpdate.US.ProdServers.1',
      'app.WindowsUpdate.US.ProdServers.2',
      'app.WindowsUpdate.US.ProdServers.3',
      'app.WindowsUpdate.US.ProdServers.4',
      'app.WindowsUpdate.US.ProdServers.5',
      'app.WindowsUpdate.US.ProdServers.6',
      'app.WindowsUpdate.US.NonProdServers.1',
      'app.WindowsUpdate.US.NonProdServers.2'
    )
  }

  $meta = Get-RegionEnvFromName -Name $ComputerName
  if (-not $meta) {
    Write-Stamp ("[Ivanti] {0}: name did not match region/env rules; skipping." -f $ComputerName)
    return
  }

  $role   = Get-RoleFromName -Name $ComputerName
  $target = Get-TargetGroup -Meta $meta -Role $role
  if (-not $target) {
    Write-Stamp ("[Ivanti] {0}: target group could not be determined; skipping." -f $ComputerName)
    return
  }

  $schedSet = $SchedulingSets[$meta.Region]
  if (-not $schedSet) {
    Write-Stamp ("[Ivanti] {0}: no scheduling set for region {1}; skipping." -f $ComputerName,$meta.Region)
    return
  }

  $computer = $null
  try {
    $computer = Get-ADComputer -Identity $ComputerName -ErrorAction Stop
  } catch {
    Write-Warning ("[Ivanti] {0}: AD computer not found; skipping. ({1})" -f $ComputerName,$_.Exception.Message)
    return
  }

  $currentGroups = @()
  try {
    $currentGroups = Get-ADPrincipalGroupMembership -Identity $computer -ErrorAction SilentlyContinue |
                     Select-Object -ExpandProperty Name
  } catch {}

  $inScope = $currentGroups | Where-Object { $schedSet -contains $_ }

  foreach ($g in $inScope) {
    if ($g -ne $target) {
      try {
        Remove-ADGroupMember -Identity $g -Members $computer -Confirm:$false -ErrorAction SilentlyContinue
        Write-Stamp ("[Ivanti] {0}: removed from '{1}'." -f $ComputerName,$g)
      } catch {
        Write-Warning ("[Ivanti] {0}: failed to remove from '{1}' - {2}" -f $ComputerName,$g,$_.Exception.Message)
      }
    }
  }

  if (-not ($inScope -contains $target)) {
    try {
      Add-ADGroupMember -Identity $target -Members $computer -ErrorAction Stop
      Write-Stamp ("[Ivanti] {0}: added to '{1}'." -f $ComputerName,$target)
    } catch {
      Write-Warning ("[Ivanti] {0}: failed to add to '{1}' - {2}" -f $ComputerName,$target,$_.Exception.Message)
    }
  } else {
    Write-Stamp ("[Ivanti] {0}: already in correct Ivanti group '{1}'." -f $ComputerName,$target)
  }
}

# --- Deploy ---------------------------------------------------------------
function Deploy-FromCLv2 {
  param([Parameter(Mandatory = $true)][string]$ConfigPath)

  # --- Helper: ensure per-VM Local Admin AD group exists ----------------------
  function Ensure-LocalAdminGroupForVM {
    [CmdletBinding()]
    param(
      [Parameter(Mandatory)][string]$ComputerName,
      [string]$ADServer,
      [pscredential]$Credential
    )

    try {
      Import-Module ActiveDirectory -ErrorAction Stop
    } catch {
      Write-Warning ("[ADGroup] ActiveDirectory module not available: {0}" -f $_.Exception.Message)
      return
    }

    $groupOu   = "OU=Local Admin Access,OU=Shared Groups,DC=bfl,DC=local"
    $groupName = "{0}-LocalAdmins" -f $ComputerName

    $getParams = @{
      Filter     = "SamAccountName -eq '$groupName'"
      ErrorAction = 'Stop'
    }
    if ($ADServer)    { $getParams['Server']     = $ADServer }
    if ($Credential)  { $getParams['Credential'] = $Credential }

    try {
      $existing = Get-ADGroup @getParams
    } catch {
      $existing = $null
    }

    if ($existing) {
      Write-Stamp ("[ADGroup] {0} already exists in AD." -f $groupName)
      return
    }

    $newParams = @{
      Name          = $groupName
      SamAccountName= $groupName
      GroupCategory = 'Security'
      GroupScope    = 'Global'
      Path          = $groupOu
      ErrorAction   = 'Stop'
    }
    if ($ADServer)   { $newParams['Server']     = $ADServer }
    if ($Credential) { $newParams['Credential'] = $Credential }

    try {
      New-ADGroup @newParams | Out-Null
      Write-Stamp ("[ADGroup] Created AD group '{0}' in '{1}'." -f $groupName, $groupOu)
    } catch {
      Write-Warning ("[ADGroup] Failed to create '{0}': {1}" -f $groupName, $_.Exception.Message)
    }
  }

  # --- Logging -----------------------------------------------------------------
  $deployLog = Join-Path $env:TEMP ("VMDeploy-{0:yyyyMMdd-HHmmss}-{1}.log" -f (Get-Date), (Split-Path -LeafBase $ConfigPath))
  try { Start-Transcript -Path $deployLog -Append -ErrorAction SilentlyContinue | Out-Null } catch {}
  Write-Stamp ("Logging to: {0}" -f $deployLog)

  $oldEap = $ErrorActionPreference
  $ErrorActionPreference = 'Stop'
  try {
    # --- Load + normalise config ----------------------------------------------
    $rawConfig = Read-Config -Path $ConfigPath
    $config    = Convert-StructuredConfig -In $rawConfig

    foreach ($key in @('VCSA','Cluster','Network','ContentLibraryName','TemplateName','Folder','TemplatesFolderName','VMName')) {
      if (-not $config.$key) { throw "Missing required config field: '$key'." }
    }
    if (-not $config.LocalTemplateName) {
      $config.LocalTemplateName = ('{0}_US' -f $config.TemplateName)
    }

    # --- Connect to vCenter ----------------------------------------------------
    try {
      VMware.VimAutomation.Core\Set-PowerCLIConfiguration -InvalidCertificateAction Ignore -Confirm:$false | Out-Null
    } catch {}

    Write-Host ("Enter credentials for {0} (e.g. administrator@vsphere.local)" -f $config.VCSA)
    $cred  = Get-Credential

    $vcConn = Invoke-Step -Name ("VMware.VimAutomation.Core\Connect-VIServer {0}" -f $config.VCSA) -Script {
      VMware.VimAutomation.Core\Connect-VIServer -Server $config.VCSA -Credential $cred -ErrorAction Stop
    }

    # --- Resolve Datacenter / Templates folder --------------------------------
    $clusterObj      = VMware.VimAutomation.Core\Get-Cluster -Name $config.Cluster -ErrorAction Stop
    $dc              = Get-DatacenterForCluster -Cluster $clusterObj
    $templatesFolder = Ensure-TemplatesFolderStrict -Datacenter $dc -FolderName $config.TemplatesFolderName

    # --- Ensure local template from Content Library ---------------------------
    $template = Ensure-LocalTemplateFromCL -Config $config

    # --- Clone VM -------------------------------------------------------------
    if (VMware.VimAutomation.Core\Get-VM -Name $config.VMName -ErrorAction SilentlyContinue) {
      throw ("A VM named '{0}' already exists." -f $config.VMName)
    }
    $newVM = New-VMFromLocalTemplate -Config $config

    # --- Customisation spec ---------------------------------------------------
    if ($config.CustomizationSpec) {
      $oscSpec = VMware.VimAutomation.Core\Get-OSCustomizationSpec -Name $config.CustomizationSpec -ErrorAction SilentlyContinue
      if ($oscSpec) {
        VMware.VimAutomation.Core\Set-VM -VM $newVM -OSCustomizationSpec $oscSpec -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
      } else {
        Write-Warning ("OS Customization Spec '{0}' not found; continuing without it." -f $config.CustomizationSpec)
      }
    }

    # --- Hardware & disks -----------------------------------------------------
    if ($config.CPU -or $config.MemoryGB -or $config.CoresPerSocket) {
      $setParams = @{ VM=$newVM; Confirm=$false; ErrorAction='SilentlyContinue' }
      if ($config.CPU)           { $setParams['NumCPU']        = [int]$config.CPU }
      if ($config.MemoryGB)      { $setParams['MemoryGB']      = [int]$config.MemoryGB }
      if ($config.CoresPerSocket){ $setParams['CoresPerSocket']= [int]$config.CoresPerSocket }
      VMware.VimAutomation.Core\Set-VM @setParams | Out-Null
    }

    if ($config.DiskGB) {
      $sysDisk = VMware.VimAutomation.Core\Get-HardDisk -VM $newVM | Select-Object -First 1
      if ($sysDisk -and $config.DiskGB -gt $sysDisk.CapacityGB) {
        VMware.VimAutomation.Core\Set-HardDisk -HardDisk $sysDisk -CapacityGB $config.DiskGB -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
      }
    }

    if ($config.AdditionalDisks -and $config.AdditionalDisks.Count -gt 0) {
      Add-DataDisks-RoundRobin -VM $newVM -SizesGB (@($config.AdditionalDisks | ForEach-Object { [int]$_ })) -Controllers (1,2,3)
    }

    # --- Notes ----------------------------------------------------------------
    if ($config.ChangeRequestNumber) {
      $note = ("CR: {0} — deployed {1}" -f $config.ChangeRequestNumber, (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))
      try {
        VMware.VimAutomation.Core\Set-VM -VM $newVM -Notes $note -Confirm:$false | Out-Null
        Write-Stamp ("Notes set: {0}" -f $note)
      } catch {
        Write-Warning ("Failed to set Notes: {0}" -f $_.Exception.Message)
      }
    }

    # --- Power on & initial tools wait ---------------------------------------
    if ($config.PowerOn) {
      Invoke-Step -Name "Power on VM" -Script {
        VMware.VimAutomation.Core\Start-VM -VM $newVM -Confirm:$false | Out-Null
      }

      if ($config.PostPowerOnDelay -gt 0) {
        Write-Stamp ("Sleeping {0}s post power-on…" -f $config.PostPowerOnDelay)
        Start-Sleep -Seconds ([int]$config.PostPowerOnDelay)
      }

      $tw = 60
      if ($config.ToolsWaitSeconds) { $tw = [int]$config.ToolsWaitSeconds }
      try {
        $null = Invoke-Step -Name ("Wait for VMware Tools healthy ({0}s)" -f $tw) -Script {
          Wait-ToolsHealthy -VM $newVM -TimeoutSec $tw
        }
      } catch {
        Write-Warning ("Wait-ToolsHealthy threw: {0}" -f $_.Exception.Message)
      }
    }

    # --- Finalise VM (Tools policy, reboot, HW upgrade, revalidate tools) -----
    Invoke-FinalizeVm -VM $newVM -ToolsWaitSeconds ([int]$config.ToolsWaitSeconds)

    try {
      $vmObj = VMware.VimAutomation.Core\Get-VM -Id $newVM.Id -ErrorAction SilentlyContinue
      $toolsState = 'unknown'
      try {
        $guest = VMware.VimAutomation.Core\Get-VMGuest -VM $newVM -ErrorAction SilentlyContinue
        if ($guest) { $toolsState = $guest.ToolsStatus }
      } catch {}
      $hwVer = 'Unknown'
      if ($vmObj -and $vmObj.HardwareVersion) { $hwVer = $vmObj.HardwareVersion }
      Write-Stamp ("Final status → Tools: {0}; HW: {1}; Power: {2}" -f $toolsState, $hwVer, $vmObj.PowerState)
    } catch {}

    # ====================== POST-DEPLOY ORDER ================================
    # 1) TAGS
    # 2) AD PLACEMENT + AD LOCAL-ADMIN GROUP
    # 3) INSTALLERS (CrowdStrike / SCOM)
    # 4) POST CONFIG (local admin groups, power plan, Ivanti)
    # ========================================================================

    # --- 1) Tags first -------------------------------------------------------
    if ($config.PSObject.Properties['PostDeploy'] -and $config.PostDeploy.EnableTags) {
      try {
        Invoke-ApplyCohesityZertoTags -VM $newVM -Server $vcConn -ErrorAction Stop
      } catch {
        Write-Warning ("Post-deploy tagging failed: {0}" -f $_.Exception.Message)
      }
    } else {
      Write-Stamp "[Tags] Skipped by config."
    }

    # --- 2) AD (join/move) + AD LocalAdmins group ----------------------------
    $hasADConfig =
      ($null -ne $config.ADTargetOU)     -or
      ($null -ne $config.ADServer)       -or
      ($true  -eq $config.ADUseVCenterCreds) -or
      ($config.ADCredFile)

        if ($hasADConfig) {
      Write-Stamp "[AD] Starting AD join/move…"
      try {
        Invoke-ADPlacement -VM $newVM `
                           -TargetOU         $config.ADTargetOU `
                           -ADServer         $config.ADServer `
                           -UseVCenterCreds:($config.ADUseVCenterCreds) `
                           -ADCredFile       $config.ADCredFile `
                           -WaitForSeconds   ([int]$config.ADWaitForSeconds) `
                           -DomainCredential $cred
      } catch {
        Write-Warning ("[AD] Placement step failed: {0}" -f $_.Exception.Message)
      }

      # Ensure the per-VM AD LocalAdmins group exists
      try {
        Ensure-LocalAdminGroupForVM -ComputerName $config.VMName `
                                    -ADServer    $config.ADServer `
                                    -Credential  $cred
      } catch {
        Write-Warning ("[ADGroup] Failed to ensure LocalAdmins group: {0}" -f $_.Exception.Message)
      }

      # Ensure Ivanti patching group membership (exclusive scheduling set)
      try {
        Set-IvantiPatchGroupForServer -ComputerName $config.VMName
      } catch {
        Write-Warning ("[Ivanti] Failed to set patch group for '{0}': {1}" -f $config.VMName,$_.Exception.Message)
      }
    } else {
      Write-Stamp "[AD] Skipped: no AD settings in config."
    }


    # --- 3) Installers (CrowdStrike + SCOM) ----------------------------------
    if ($config.PSObject.Properties['PostDeploy'] -and $config.PostDeploy.EnableInstallers) {
      try {
        Install-CS-And-SCOM-IfNeeded -VM $newVM -Credential $cred
      } catch {
        Write-Warning ("[Installers] Failed: {0}" -f $_.Exception.Message)
      }
    } else {
      Write-Stamp "[Installers] Skipped by config."
    }

    # --- 4) Guest post-config (LocalAdmins, Ivanti, PowerPlan) --------------
    if ($config.PSObject.Properties['PostDeploy']) {
      try {
        # Start with config-defined groups
        $ladm = @()
        if ($config.PostDeploy.PSObject.Properties['LocalAdminGroups']) {
          $ladm = @($config.PostDeploy.LocalAdminGroups)
        }

        # Add the per-VM AD group as a local admin: BFL\<VMName>-LocalAdmins
        if ($config.VMName -and -not [string]::IsNullOrWhiteSpace($config.VMName)) {
          $vmLocalGroup = "BFL\{0}-LocalAdmins" -f $config.VMName
          if ($ladm -notcontains $vmLocalGroup) {
            $ladm += $vmLocalGroup
          }
        }

        $pplan = $null
        if ($config.PostDeploy.PSObject.Properties['PowerPlan']) {
          $pplan = $config.PostDeploy.PowerPlan
        }

        if ($ladm.Count -or $pplan) {
        Invoke-PostDeployGuestConfig -VM $newVM `
            -LocalAdminGroups $ladm `
            -PowerPlan        $pplan `
            -Credential       $cred
        } else {
        Write-Stamp "[PostDeploy] No guest config requested."
        }


      } catch {
        Write-Warning ("[PostDeploy] Guest config failed: {0}" -f $_.Exception.Message)
      }
    }

    Write-Host ("✅ VM '{0}' deployed." -f $config.VMName) -ForegroundColor Green

  } finally {
    try { Stop-Transcript | Out-Null } catch {}
    $ErrorActionPreference = $oldEap
  }
}


# --- Menu -------------------------------------------------------------------
$global:LastConfigPath = $null
function Show-Menu {
  Write-Host ""
  Write-Host "==== VM Builder & Deployer (Integrated, Content-Optional) ====" -ForegroundColor Cyan
  Write-Host " [1] Build new JSON (guided)"
  Write-Host " [10] Deploy VM from JSON"
  Write-Host " [99] Build then Deploy"
  Write-Host " [50] Cleanup: Remove AD computer, shutdown & delete VM"
  Write-Host " [0] Exit"
}
while ($true) {
  Show-Menu
  $sel = Read-Host "Select option"
  switch ($sel) {
    '1'  { try { $global:LastConfigPath = Build-DeployConfig } catch { Write-Warning $_ } }
    '10' {
      $path = $null
      if ($global:LastConfigPath) { $path = Ask -Prompt 'ConfigPath' -Default $global:LastConfigPath } else { $path = Ask -Prompt 'ConfigPath' }
      if (-not [string]::IsNullOrWhiteSpace($path)) { try { Deploy-FromCLv2 -ConfigPath $path } catch { Write-Warning $_ } }
    }
    '99' { try { $global:LastConfigPath = Build-DeployConfig; if ($global:LastConfigPath) { Deploy-FromCLv2 -ConfigPath $global:LastConfigPath } } catch { Write-Warning $_ } }
    '50' { try { Remove-VMAndAD } catch { Write-Warning $_ } }

    '0'  { return }

    default { Write-Host 'Unknown option.' -ForegroundColor Yellow }
  }
}

