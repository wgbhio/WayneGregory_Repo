<# ======================================================================
VM Builder & Deployer (Integrated, Content-Optional, Clean Finalize)
- Uses Content Library if available (deploy → convert to site-suffixed template)
- Fallback to local unsuffixed template if CL item is not deployable
- US site templating & placement (MAR / HAW / COMPUTE) baked in
- Finalize: enforce Tools policy → restart → HW upgrade → power cycle → tools wait
====================================================================== #>

# --- Modules ---------------------------------------------------------------
Import-Module VMware.VimAutomation.Core -ErrorAction Stop
try { Import-Module VMware.VimAutomation.Vds -ErrorAction SilentlyContinue | Out-Null } catch {}

function TryImport-Module {
  param([Parameter(Mandatory)][string]$Name)
  try { Import-Module $Name -ErrorAction Stop; return $true } catch { return $false }
}
$global:__hasContentModule = TryImport-Module -Name 'VMware.VimAutomation.Content'

# --- Utilities -------------------------------------------------------------
function Write-Stamp { param([string]$msg) Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg) }

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

# --- HOTFIX: enable Content Library calls even if the legacy module isn't importable
$script:HasContentLibCmds = [bool](Get-Command Get-ContentLibraryItem -ErrorAction SilentlyContinue)

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
      if($g.State -eq 'Running' -and $g.ToolsStatus -in 'toolsOk','toolsOld'){ return $g.ToolsStatus }
    } catch{}
    Start-Sleep -Seconds $PollSec
  } while((Get-Date) -lt $deadline)
  return $null
}
function Invoke-FinalizeVm {
  param([Parameter(Mandatory)]$VM,[int]$ToolsWaitSeconds = 180)
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
function Get-SiteSuffix {
  [CmdletBinding()] param(
    [Parameter(Mandatory)][string]$Region,
    [Parameter()][string]$Cluster,
    [Parameter()][string]$UsSite
  )
  switch ($Region) {
    'UK' {
      if ($Cluster -match 'LD9') { return 'LD9' }
      elseif ($Cluster -match 'MAR') { return 'MAR' }
      elseif ($Cluster -match 'HAW') { return 'HAW' }
      else { return 'UK' }
    }
    'IE' { return 'DB4' }
    'US' { if ($UsSite) { return $UsSite } else { return 'US' } }
    default { return $Region }
  }
}

function Convert-StructuredConfig {
  param([Parameter(Mandatory)]$In)

  function Pick { param([object[]]$v) foreach($x in $v){ if($null -ne $x -and "$x" -ne ''){ return $x } } $null }
  function Get-Prop($obj, $name) { if ($null -eq $obj) { return $null }; ($obj.PSObject.Properties[$name])?.Value }

  if (-not $In.VMware -and -not $In.AD -and -not $In.Options) { return $In }
  $vmw=$In.VMware; $ad=$In.AD; $opt=$In.Options; $cl=$vmw.ContentLibrary; $hw=$vmw.Hardware

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
    ChangeRequestNumber= Pick @((Get-Prop $In 'ChangeRequestNumber'), (Get-Prop $opt 'ChangeRequestNumber'), $null)
    PostDeploy         = Pick @((Get-Prop $In 'PostDeploy'), @{})
  }

  # Ensure AdditionalDisks is an array if provided as one value
  if ($out.AdditionalDisks -and ($out.AdditionalDisks -isnot [System.Array])) {
    $out.AdditionalDisks = @($out.AdditionalDisks)
  }

  # Compute site suffix & LocalTemplateName if not provided
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

    $siteSuffix = switch ($region) {
      'UK' { if ($out.Cluster -match 'LD9') { 'LD9' } elseif ($out.Cluster -match 'MAR') { 'MAR' } elseif ($out.Cluster -match 'HAW') { 'HAW' } else { 'UK' } }
      'IE' { 'DB4' }
      'US' { if ($usSite) { $usSite } else { 'US' } }
      default { $region }
    }

    $out.LocalTemplateName = '{0}_{1}' -f $out.TemplateName, $siteSuffix

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
        elseif ($usSite -eq 'COMPUTE') { $out.Cluster = 'COMPUTE-Local' }
      }
    }
  }

  return $out
}

# --- Template materialization ----------------------------------------------
$script:HasContentLibCmds = [bool](Get-Command Get-ContentLibraryItem -ErrorAction SilentlyContinue)
$ConfirmPreference = 'None'

function Ensure-LocalTemplateFromCL {
  [CmdletBinding()] param([Parameter(Mandatory)][object]$Config)

  # Values from your normalized config
  $libWanted   = $Config.ContentLibraryName
  $itemWanted  = $Config.TemplateName
  $tplName     = $Config.LocalTemplateName
  $clusterObj  = Get-Cluster -Name $Config.Cluster -ErrorAction Stop
  $vmHost      = if ($Config.VMHost) { Get-VMHost -Name $Config.VMHost } else { ($clusterObj | Get-VMHost | Select-Object -First 1) }

  # Pick a datastore (supports datastore cluster)
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

  # --- Force overwrite TEMPLATE only (never delete VMs) ----------------------
  $existingTemplate = Get-Template -Name $tplName -ErrorAction SilentlyContinue
  if ($existingTemplate) {
    Write-Host "Template '$tplName' exists → removing (force overwrite)..." -ForegroundColor Yellow
    try {
      Remove-Template -Template $existingTemplate -DeletePermanently -Confirm:$false -ErrorAction Stop
    } catch {
      throw "Failed to remove existing template '$tplName': $($_.Exception.Message)"
    }
  }

  # If a VM with the same name exists, do NOT delete it; warn because vSphere
  # can block creating a template with the same name in the same folder.
  $existingVM = Get-VM -Name $tplName -ErrorAction SilentlyContinue
  if ($existingVM) {
    Write-Warning "A VM named '$tplName' already exists. I will NOT delete it. If it resides in the target template folder, the template create/rename may be blocked."
  }

  # --- Deploy a seed VM from Content Library item ----------------------------
  $seedVM = $null
  try {
    # Use unqualified cmdlets (works even if VMware.VimAutomation.Content cannot be imported)
    $lib  = Get-ContentLibrary | Where-Object { $_.Name -ieq $libWanted } | Select-Object -First 1
    if (-not $lib) {
      $lib = Get-ContentLibrary | Where-Object { $_.Name -match 'packer|template' } | Select-Object -First 1
      if ($lib) { Write-Host "Library '$libWanted' not found; using '$($lib.Name)' instead." -ForegroundColor Yellow }
    }
    if (-not $lib) { throw "Content library '$libWanted' not found." }

    $items = @( Get-ContentLibraryItem -ContentLibrary $lib )
    $cli = $items | Where-Object { $_.Name -ieq $itemWanted } | Select-Object -First 1
    if (-not $cli) { $cli = $items | Where-Object { $_.Name -like "$itemWanted*" } | Sort-Object CreationTime -Descending | Select-Object -First 1 }
    if (-not $cli) { throw "No CL item matching '$itemWanted' in library '$($lib.Name)'." }

    Write-Stamp ("Using CL item: {0} (library: {1})" -f $cli.Name,$lib.Name)
    $tempVmName = "__seed_{0}_{1}" -f $tplName, ([guid]::NewGuid().ToString('N').Substring(0,8))
    $seedVM = New-VM -Name $tempVmName -ContentLibraryItem $cli -VMHost $vmHost -Datastore $ds `
              -WarningAction SilentlyContinue -ErrorAction Stop -Confirm:$false
  } catch {
    throw "Unable to deploy seed VM from CL item '$itemWanted' in '$libWanted': $($_.Exception.Message)"
  }

  # --- Convert seed VM to template with target name --------------------------
  Write-Stamp ("Converting '{0}' to local template '{1}' ..." -f $seedVM.Name, $tplName)
  try {
    $template = Set-VM -VM $seedVM -ToTemplate -Name $tplName -Confirm:$false -ErrorAction Stop
  } catch {
    # Most common cause: a VM with same name in the same destination folder
    throw "Failed to create template '$tplName'. If a VM with this name exists in the Templates folder, rename/move that VM or choose a different template name. Error: $($_.Exception.Message)"
  }

  # --- Move into Templates folder if not already there -----------------------
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

  $tplName = $Config.LocalTemplateName
  $template = VMware.VimAutomation.Core\Get-Template -Name $tplName -ErrorAction Stop
  $clusterObj = VMware.VimAutomation.Core\Get-Cluster -Name $Config.Cluster -ErrorAction Stop
  $vmhost = if ($Config.VMHost) { VMware.VimAutomation.Core\Get-VMHost -Name $Config.VMHost } else { ($clusterObj | VMware.VimAutomation.Core\Get-VMHost | Select-Object -First 1) }
  $ds   = if ($Config.DatastoreCluster) {
            $sp = VMware.VimAutomation.Core\Get-DatastoreCluster -Name $Config.DatastoreCluster -ErrorAction Stop
            Select-BestDatastoreFromCluster -ClusterOrStoragePod $sp
          } elseif ($Config.Datastore) {
            VMware.VimAutomation.Core\Get-Datastore -Name $Config.Datastore -ErrorAction Stop
          } else {
            VMware.VimAutomation.Core\Get-Datastore -RelatedObject $vmhost | Sort-Object FreeSpaceGB -Descending | Select-Object -First 1
          }

  $dc = Get-DatacenterForCluster -Cluster $clusterObj
  $vmFolder = if ($Config.Folder) { Resolve-RootFolder -Datacenter $dc -Name $Config.Folder } else { $null }

  $params = @{
    Name      = $Config.VMName
    Template  = $template
    VMHost    = $vmhost
    Datastore = $ds
    ErrorAction = 'Stop'
  }
  if ($vmFolder) { $params['Location'] = $vmFolder }

  Write-Stamp "Cloning VM '$($Config.VMName)' from local template '$tplName' ..."
  $newVM = VMware.VimAutomation.Core\New-VM @params

  # NIC mapping
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
          } else {
            VMware.VimAutomation.Core\Set-NetworkAdapter -NetworkAdapter $firstNic -NetworkName $Config.Network -Confirm:$false | Out-Null
          }
        }
      }
    } catch { Write-Warning "Post-clone network set failed: $($_.Exception.Message)" }
  }

  if ($Config.RemoveExtraCDROMs){ Ensure-SingleCDHard -VM $newVM }
  if ($Config.EnsureNICConnected){ Set-VMNicStartConnected -VM $newVM }

  return $newVM
}

# --- Builder (interactive) --------------------------------------------------
function Build-DeployConfig {

  # Fixed maps
  $RegionToVCSA = @{ 'UK'='ukprim098.bfl.local'; 'US'='usprim004.bfl.local'; 'IE'='ieprim018.bfl.local' }
  $RegionToContentLib = @{ 'UK'='Template Library'; 'IE'='DB4 Packer Templates'; 'US'='Packer templates' }

  # Region+Env -> Network
  $NetworkMap = @{
    'US|DEV' = 'VLAN_2032_Stretch'; 'US|PROD' = 'VLAN_2002_Stretch';
    'UK|PROD' = 'BZY|ProdVMs_AP|ProdVMs_EPG'; 'UK|DEV' = 'BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG';
    'IE|DEV' = 'BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG'; 'IE|PROD' = 'BZY|ProdVMs_AP|ProdVMs_EPG'
  }

  $USOptions = @(
    @{ Name='US Stretched'; Cluster='Compute';       DSCluster='PROD_POD_CLUSTER'; Site=$null },
    @{ Name='US HAW';       Cluster='HAW-Local';     DSCluster='HAW_LOCAL_CLUSTER'; Site='HAW'  },
    @{ Name='US MAR';       Cluster='MAR-Local';     DSCluster='MAR_LOCAL_CLUSTER'; Site='MAR'  },
    @{ Name='US COMPUTE';   Cluster='COMPUTE-Local'; DSCluster='DEV_POD_CLUSTER';   Site='COMPUTE' }
  )

  # OU map (DNs)
  $OU = @{
    'UK|DEV'      = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=London,OU=Accounts Computer,DC=bfl,DC=local';
    'UK|PROD'     = 'OU=Application Servers,OU=Servers & Exceptions,OU=London,OU=Accounts Computer,DC=bfl,DC=local';
    'IE|DEV'      = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Ireland,OU=Accounts Computer,DC=bfl,DC=local';
    'IE|PROD'     = 'OU=Application Servers,OU=Servers & Exceptions,OU=Ireland,OU=Accounts Computer,DC=bfl,DC=local';
    'US|DEV|HAW'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local';
    'US|PROD|HAW' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local';
    'US|DEV|MAR'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local';
    'US|PROD|MAR' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local'
  }

  # AD servers
  $ADServerMap = @{
    'UK'='UKPRDC011.bfl.local'; 'IE'='IEPRDC010.bfl.local';
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
    'Windows 2022'=@{ UK='Win2022_Template_LD9'; IE='Win2022_Template_DB4'; US='Win2022_Template' };
    'Windows 2019'=@{ UK='Win2019_Template_LD9'; IE='Win2019_Template_DB4'; US='Win2019_Template' };
    'Linux CentOS 7'=@{ UK=$null; IE=$null; US=$null };
    'Linux RHEL 7'  =@{ UK=$null; IE=$null; US=$null };
    'Linux RHEL 9'  =@{ UK=$null; IE=$null; US=$null };
    'Linux SLES 15' =@{ UK=$null; IE=$null; US=$null };
  }

  # 1: Region
  $region = AskChoice -Prompt 'Select VCSA Region' -Choices @('UK','US','IE') -DefaultIndex 0
  $vcsa = $RegionToVCSA[$region]
  $clib = $RegionToContentLib[$region]

  # 2: Env
  $env = AskChoice -Prompt 'Environment' -Choices @('PROD','DEV') -DefaultIndex 1
  $network = $NetworkMap["$region|$env"]

  # Root folders
  $vmFolder = if ($env -eq 'PROD') { 'Production' } else { 'Development' }
  $templatesFolder = 'Templates'

  # 3: Cluster + OU
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
  $adOU = if ($OU.ContainsKey($ouKey)) { $OU[$ouKey] } else { $null }
  $adServer = if ($region -eq 'US') { $ADServerMap["US|$usSite"] } else { $ADServerMap[$region] }

  # 4: Template & Spec
  $osChoices = @('Windows 2022','Windows 2019','Linux CentOS 7','Linux RHEL 7','Linux RHEL 9','Linux SLES 15')
  $tmplChoice = AskChoice -Prompt 'Choose OS / Template' -Choices $osChoices -DefaultIndex 0
  $templateItem = $TemplateItemMap[$tmplChoice][$region]
  $customSpec = $SpecMap[$tmplChoice][$region]

  # 5/6/7: CPU/RAM/Disk
  $cpu = AskInt -Prompt 'Total vCPU' -Default 4
  if ($cpu -gt 48) { if ($cpu % 2 -ne 0) { Write-Host 'CPU > 48 → 2 sockets; rounding vCPU up to even.' -ForegroundColor Yellow; $cpu++ } ; $sockets=2; $coresPerSocket=[int]($cpu/2) }
  else { $sockets=1; $coresPerSocket=$cpu }
  $memGB = AskInt -Prompt 'Memory (GB)' -Default 12
  $diskGB = AskInt -Prompt 'System disk (GB)' -Default 100

  # Additional data disks
  $addCount = AskInt -Prompt 'How many additional data disks?' -Default 0
  $additionalDisks = @()
  for ($i=1; $i -le $addCount; $i++){
    $sz = AskInt -Prompt (" Size of additional disk #{0} (GB)" -f $i) -Default 50
    $additionalDisks += $sz
  }

  # 8: VM name
  do {
    $vmName1 = Ask -Prompt 'VM Name'
    $vmName2 = Ask -Prompt 'Confirm VM Name'
    if ($vmName1 -ne $vmName2){ Write-Host 'Names did not match. Try again.' -ForegroundColor Yellow }
  } until ($vmName1 -eq $vmName2)
  $vmName = $vmName1

  # 9: Change number
  $changeNum = Ask -Prompt 'Change Number (goes into VM Notes)' -AllowEmpty

  # Build config
  $vmw = [ordered]@{
    VCSA=$vcsa; Cluster=$cluster; VMHost=$null; VMFolder=$vmFolder; TemplatesFolderName=$templatesFolder;
    Network=$network; Datastore=''; DatastoreCluster=$dsCluster;
    ContentLibrary=[ordered]@{ Library=$clib; Item=$templateItem; LocalTemplateName=$null; ForceReplace=$true };
    VMName=$vmName; CustomizationSpec=$customSpec;
    Hardware=[ordered]@{ CPU=$cpu; MemoryGB=$memGB; DiskGB=$diskGB; AdditionalDisks=$additionalDisks; Sockets=$sockets; CoresPerSocket=$coresPerSocket }
  }
  $out = [ordered]@{
    VMware=$vmw
    Options=[ordered]@{ PowerOn=$true; EnsureNICConnected=$true; RemoveExtraCDROMs=$true; ToolsWaitSeconds=120; PostPowerOnDelay=15 }
    AD=[ordered]@{ TargetOU=$adOU; Server=$adServer; UseVCenterCreds=$true; WaitForSeconds=180; ForceSync=$true }
    PostDeploy=[ordered]@{ EnableTags=$false; Tags=@{}; EnableSCOMCrowdStrike=$false; LocalAdminGroups=@(); IvantiGroup=$null; PowerPlan=$null }
  }
  if (-not $adOU) { $out.Remove('AD') }
  if ($changeNum) { $out.ChangeRequestNumber = $changeNum }

  $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } elseif ($MyInvocation.MyCommand.Path) { Split-Path -Parent $MyInvocation.MyCommand.Path } else { (Get-Location).Path }
  $outPath = Join-Path $scriptDir ("{0}.json" -f $vmName)
  ($out | ConvertTo-Json -Depth 8) | Set-Content -Path $outPath -Encoding UTF8 -NoNewline
  Write-Host "Saved configuration → $outPath" -ForegroundColor Green
  return $outPath
}

# --- Deploy ---------------------------------------------------------------
function Deploy-FromCLv2 {
  param([Parameter(Mandatory=$true)][string]$ConfigPath)

  $deployLog = Join-Path $env:TEMP ("VMDeploy-{0:yyyyMMdd-HHmmss}-{1}.log" -f (Get-Date),(Split-Path -LeafBase $ConfigPath))
  try { Start-Transcript -Path $deployLog -Append -ErrorAction SilentlyContinue | Out-Null } catch {}
  Write-Stamp ("Logging to: {0}" -f $deployLog)

  $oldEap = $ErrorActionPreference; $ErrorActionPreference = 'Stop'
  try {
    $rawConfig = Read-Config -Path $ConfigPath
    $config = Convert-StructuredConfig -In $rawConfig
    foreach ($key in @('VCSA','Cluster','Network','ContentLibraryName','TemplateName','Folder','TemplatesFolderName','VMName')) {
      if (-not $config.$key) { throw "Missing required config field: '$key'." }
    }
    if (-not $config.LocalTemplateName) { $config.LocalTemplateName = '{0}_US' -f $config.TemplateName }

    # vCenter credentials (per-VCSA cache)
    try { VMware.VimAutomation.Core\Set-PowerCLIConfiguration -InvalidCertificateAction Ignore -Confirm:$false | Out-Null } catch {}
    $cred = $null
    Write-Host "Enter credentials for $($config.VCSA) (e.g. administrator@vsphere.local)"
    $cred = Get-Credential
    $null = Invoke-Step -Name ("VMware.VimAutomation.Core\Connect-VIServer {0}" -f $config.VCSA) -Script { VMware.VimAutomation.Core\Connect-VIServer -Server $config.VCSA -Credential $cred -ErrorAction Stop }

    # Placement context
    $clusterObj = VMware.VimAutomation.Core\Get-Cluster -Name $config.Cluster -ErrorAction Stop
    $dc = Get-DatacenterForCluster -Cluster $clusterObj
    $templatesFolder= Ensure-TemplatesFolderStrict -Datacenter $dc -FolderName $config.TemplatesFolderName

    # Ensure local site-suffixed template
    $template = Ensure-LocalTemplateFromCL -Config $config

    # Deploy VM (powered off)
    if (VMware.VimAutomation.Core\Get-VM -Name $config.VMName -ErrorAction SilentlyContinue){ throw "A VM named '$($config.VMName)' already exists." }
    $newVM = New-VMFromLocalTemplate -Config $config

    # OS Customization (optional)
    if ($config.CustomizationSpec) {
      $oscSpec=VMware.VimAutomation.Core\Get-OSCustomizationSpec -Name $config.CustomizationSpec -ErrorAction SilentlyContinue
      if ($oscSpec) { VMware.VimAutomation.Core\Set-VM -VM $newVM -OSCustomizationSpec $oscSpec -Confirm:$false -ErrorAction SilentlyContinue | Out-Null }
      else { Write-Warning "OS Customization Spec '$($config.CustomizationSpec)' not found; continuing without it." }
    }

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
    if ($config.AdditionalDisks -and $config.AdditionalDisks.Count -gt 0) {
      foreach($sz in $config.AdditionalDisks){ VMware.VimAutomation.Core\New-HardDisk -VM $newVM -CapacityGB ([int]$sz) -Confirm:$false -ErrorAction Stop | Out-Null }
    }

    # Notes
    if ($config.ChangeRequestNumber){
      $note = "CR: $($config.ChangeRequestNumber) — deployed $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
      try { VMware.VimAutomation.Core\Set-VM -VM $newVM -Notes $note -Confirm:$false | Out-Null; Write-Stamp "Notes set: $note" } catch { Write-Warning "Failed to set Notes: $($_.Exception.Message)" }
    }

    # Power on & settle
    if ($config.PowerOn){
      Invoke-Step -Name "Power on VM" -Script { VMware.VimAutomation.Core\Start-VM -VM $newVM -Confirm:$false | Out-Null }
      if ($config.PostPowerOnDelay -gt 0) { Write-Stamp ("Sleeping {0}s post power-on…" -f $config.PostPowerOnDelay); Start-Sleep -Seconds ([int]$config.PostPowerOnDelay) }
      $tw = if ($config.ToolsWaitSeconds) { [int]$config.ToolsWaitSeconds } else { 180 }
      try { $null = Invoke-Step -Name ("Wait for VMware Tools healthy ({0}s)" -f $tw) -Script { Wait-ToolsHealthy -VM $newVM -TimeoutSec $tw } } catch { Write-Warning ("Wait-ToolsHealthy threw: {0}" -f $_.Exception.Message) }
    }

    # Finalize
    Invoke-FinalizeVm -VM $newVM -ToolsWaitSeconds ([int]$config.ToolsWaitSeconds)

    # Final status
    try{
      $vmObj = VMware.VimAutomation.Core\Get-VM -Id $newVM.Id -ErrorAction SilentlyContinue
      $toolsState = 'unknown'
      try { $guest = VMware.VimAutomation.Core\Get-VMGuest -VM $newVM -ErrorAction SilentlyContinue; if ($guest) { $toolsState = $guest.ToolsStatus } } catch {}
      Write-Stamp ("Final status → Tools: {0}; HW: {1}; Power: {2}" -f $toolsState, ($vmObj.HardwareVersion ?? 'Unknown'), $vmObj.PowerState)
    } catch {}

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
