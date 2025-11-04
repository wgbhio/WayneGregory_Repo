<# ============================================================================
 Baseline V5 — Hardened VM Deploy (Template-safe, CL optional, API fallback)
 Author: Wayne/GPT assistant
 Date: 2025-09-30

 Highlights
 - Logs to Temp by default, or to a fixed folder with -LogDir
 - Strict, noise-free Template resolution + defensive logging
 - New-VM guarded; falls back to vSphere CloneVM_Task on PowerCLI index bug
 - Customization applied at Start-VM stage to avoid index/mapping flakiness
 - NIC mapping sanity checks; warns if spec mismatch
 - Minimal Notes pattern: "CR: {Change} — deployed {yyyy-MM-dd HH:mm:ss}"
 - Preserves Baseline V2 vibes: Tools wait + post-power-on delays (tunable)

 Usage
   .\BaselineV5_Deploy.ps1 -ConfigPath C:\path\VM.json -Verbose
   .\BaselineV5_Deploy.ps1 -ConfigPath C:\path\VM.json -LogDir C:\Logs -Verbose

 Config JSON (example)
 {
   "VCSA": "usprim004.bfl.local",
   "Datacenter": "US-DC",
   "DatastoreCluster": "HAW_LOCAL_CLUSTER",
   "Datastore": null,
   "Folder": "Production",
   "Network": "VLAN_123-Prod",
   "TemplateName": "PKR_windows_server_2022_std_Current",
   "LocalTemplateName": null,
   "ContentLibraryName": "CoreOS-Templates",
   "ForceReplace": false,
   "CustomizationSpec": "Win2022-Std",
   "VMName": "USPRAP180",
   "ChangeNumber": "CHG0012345",
   "RemoveExtraCDROMs": true,
   "EnsureNICConnected": true,
   "ToolsWaitSeconds": 90,
   "PostPowerOnDelay": 15
 }
============================================================================ #>

#Requires -Modules VMware.VimAutomation.Core
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

param(
  [Parameter(Mandatory)] [string]$ConfigPath,
  [string]$LogDir
)

# --- Utility: logging --------------------------------------------------------
function New-RunLog {
  param([string]$VmName)
  $base = if ($LogDir) { $LogDir } else { Join-Path $env:LOCALAPPDATA 'Temp' }
  if (-not (Test-Path -LiteralPath $base)) { New-Item -ItemType Directory -Path $base -Force | Out-Null }
  $ts = Get-Date -Format 'yyyyMMdd-HHmmss'
  $path = Join-Path $base ("VMDeploy-{0}-{1}.log" -f $ts,$VmName)
  return $path
}
function Write-Stamp([string]$msg){
  $line = "[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
  Write-Host $line
  if ($script:LogFile) { Add-Content -LiteralPath $script:LogFile -Value $line }
}

# --- Module helpers ----------------------------------------------------------
function TryImport-Module { param([Parameter(Mandatory)][string]$Name)
  try { Import-Module $Name -ErrorAction Stop; return $true } catch { return $false }
}
Import-Module VMware.VimAutomation.Core -ErrorAction Stop
$script:HasContent = TryImport-Module -Name 'VMware.VimAutomation.Content'
TryImport-Module -Name 'VMware.VimAutomation.Vds' | Out-Null

# --- JSON helpers ------------------------------------------------------------
function Read-JsonConfig {
  param([Parameter(Mandatory)][string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) { throw "Config file '$Path' not found." }
  $raw = Get-Content -LiteralPath $Path -Raw -ErrorAction Stop
  return ($raw | ConvertFrom-Json)
}

# --- vSphere helpers ---------------------------------------------------------
function Resolve-SingleTemplate {
  [CmdletBinding()]
  [OutputType('VMware.VimAutomation.ViCore.Types.V1.Inventory.Template')]
  param(
    [Parameter(Mandatory)][string]$Name,
    [Parameter(Mandatory)][string]$Server,
    [string]$TemplatesFolderName = 'Templates'
  )
  Write-Verbose "Resolving template '$Name' on $Server ..."
  $templatesFolder = VMware.VimAutomation.Core\Get-Folder -Name $TemplatesFolderName -Type VM -Server $Server -ErrorAction SilentlyContinue
  $candidates = if ($templatesFolder) {
    VMware.VimAutomation.Core\Get-Template -Name $Name -Server $Server -Location $templatesFolder -ErrorAction SilentlyContinue
  } else {
    VMware.VimAutomation.Core\Get-Template -Name $Name -Server $Server -ErrorAction SilentlyContinue
  }
  if (-not $candidates) { throw "Template '$Name' not found on $Server." }
  $template = $candidates | Where-Object { $_ -is [VMware.VimAutomation.ViCore.Types.V1.Inventory.Template] } | Select-Object -First 1
  if (-not $template) { throw "Resolved object for '$Name' is not a Template." }
  return $template
}
function Use-SingleTemplate {  # harden pipeline variable
  param([Parameter(Mandatory, ValueFromPipeline)][object]$InputObject)
  process {
    $tmpl = @($InputObject | Where-Object { $_ -is [VMware.VimAutomation.ViCore.Types.V1.Inventory.Template] })[0]
    if (-not $tmpl) {
      $tname = ($InputObject | Select-Object -First 1).GetType().FullName
      throw "Template variable isn't a Template (got $tname)."
    }
    return $tmpl
  }
}
function Select-BestDatastoreFromCluster {
  param([Parameter(Mandatory)]$ClusterOrStoragePod)
  $ds = VMware.VimAutomation.Core\Get-Datastore -RelatedObject $ClusterOrStoragePod -ErrorAction Stop |
        Sort-Object FreeSpaceGB -Descending | Select-Object -First 1
  if (-not $ds) { throw "No datastore found under '$($ClusterOrStoragePod.Name)'" }
  return $ds
}

function New-VM-SafeClone {
  <#
    Creates VM from template with robust fallback to CloneVM_Task if New-VM hits
    the intermittent "Index was out of range (Parameter 'index')" exception.
  #>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][string]$Name,
    [Parameter(Mandatory)][object]$Template,  # TemplateImpl
    [Parameter(Mandatory)][object]$Datastore, # DatastoreImpl
    [Parameter(Mandatory)][object]$Location   # FolderImpl
  )
  $createParams = @{ Name=$Name; Template=$Template; Datastore=$Datastore; Location=$Location; ErrorAction='Stop' }
  try {
    return VMware.VimAutomation.Core\New-VM @createParams
  } catch {
    $msg = $_.Exception.Message
    if ($msg -like '*Index was out of range*Parameter*index*') {
      Write-Stamp "Hit PowerCLI New-VM index bug; falling back to CloneVM_Task..."
      # Resolve a host that sees the datastore
      $vmhost = VMware.VimAutomation.Core\Get-VMHost -Datastore $Datastore -ErrorAction SilentlyContinue |
                Where-Object {$_.ConnectionState -eq 'Connected'} |
                Sort-Object CpuUsageMhz,MemoryUsageGB | Select-Object -First 1
      if (-not $vmhost) { throw "No connected host found that can access datastore '$($Datastore.Name)'." }
      $cluster = VMware.VimAutomation.Core\Get-Cluster -VMHost $vmhost -ErrorAction SilentlyContinue | Select-Object -First 1
      $pool = if ($cluster) { VMware.VimAutomation.Core\Get-ResourcePool -Location $cluster -Name 'Resources' -ErrorAction SilentlyContinue | Select-Object -First 1 } else { $null }
      if (-not $pool) { $pool = VMware.VimAutomation.Core\Get-ResourcePool -Name 'Resources' -ErrorAction SilentlyContinue | Select-Object -First 1 }
      if (-not $pool) { throw "Could not resolve a Resource Pool for host '$($vmhost.Name)'." }

      $tmplView  = Get-View -Id $Template.Id
      $folderView= Get-View -Id $Location.Id
      $poolView  = Get-View -Id $pool.Id
      $hostView  = Get-View -Id $vmhost.Id
      $dsView    = Get-View -Id $Datastore.Id

      $relocateSpec = New-Object VMware.Vim.VirtualMachineRelocateSpec
      $relocateSpec.Datastore = $dsView.MoRef
      $relocateSpec.Pool      = $poolView.MoRef
      $relocateSpec.Host      = $hostView.MoRef

      $cloneSpec = New-Object VMware.Vim.VirtualMachineCloneSpec
      $cloneSpec.Location = $relocateSpec
      $cloneSpec.PowerOn  = $false
      $cloneSpec.Template = $false

      $taskMoRef = $tmplView.CloneVM_Task($folderView.MoRef, $Name, $cloneSpec)
      $task = Get-View $taskMoRef
      while ($task.Info.State -in 'queued','running') { Start-Sleep 2; $task = Get-View $taskMoRef }
      if ($task.Info.State -ne 'success') { throw ("CloneVM_Task failed: {0}" -f $task.Info.Error.LocalizedMessage) }
      return (VMware.VimAutomation.Core\Get-VM -Id $task.Info.Result -ErrorAction Stop)
    }
    throw
  }
}

function Ensure-SingleCDHard { param([Parameter(Mandatory)]$VM)
  $cds = VMware.VimAutomation.Core\Get-CDDrive -VM $VM -ErrorAction SilentlyContinue
  if ($cds.Count -gt 1){ $cds | Select-Object -Skip 1 | ForEach-Object { VMware.VimAutomation.Core\Remove-CDDrive -CDDrive $_ -Confirm:$false } }
}
function Set-VMNicStartConnected { param([Parameter(Mandatory)]$VM)
  VMware.VimAutomation.Core\Get-NetworkAdapter -VM $VM | ForEach-Object { if (-not $_.StartConnected) { VMware.VimAutomation.Core\Set-NetworkAdapter -NetworkAdapter $_ -StartConnected:$true -Confirm:$false | Out-Null } }
}

# --- Main orchestrator -------------------------------------------------------
function Deploy-FromConfig {
  [CmdletBinding()]
  param([Parameter(Mandatory)][string]$ConfigPath)

  $cfg = Read-JsonConfig -Path $ConfigPath
  $vmName = $cfg.VMName
  $script:LogFile = New-RunLog -VmName $vmName
  Write-Stamp ("Logging to: {0}" -f $script:LogFile)

  # Defaults (preserve Baseline V2 feel)
  $toolsWait = if ($cfg.ToolsWaitSeconds) { [int]$cfg.ToolsWaitSeconds } else { 90 }
  $postDelay = if ($cfg.PostPowerOnDelay){ [int]$cfg.PostPowerOnDelay } else { 15 }

  # Connect to vCenter
  Write-Stamp ("VMware.VimAutomation.Core\\Connect-VIServer {0}" -f $cfg.VCSA)
  VMware.VimAutomation.Core\Connect-VIServer -Server $cfg.VCSA | Out-Null

  # Datacenter / Folder
  $dc = VMware.VimAutomation.Core\Get-Datacenter -Name $cfg.Datacenter -ErrorAction Stop
  $vmFolder = if ($cfg.Folder) { VMware.VimAutomation.Core\Get-Folder -Name $cfg.Folder -Location $dc -ErrorAction Stop } else { VMware.VimAutomation.Core\Get-Folder -Name 'vm' -Location $dc -ErrorAction Stop }

  # Datastore (cluster preferred)
  $datastoreObj = $null
  if ($cfg.DatastoreCluster) {
    Write-Stamp ("Using datastore cluster '{0}'; selecting datastore with most free space..." -f $cfg.DatastoreCluster)
    $sp = VMware.VimAutomation.Core\Get-DatastoreCluster -Name $cfg.DatastoreCluster -ErrorAction Stop
    $datastoreObj = Select-BestDatastoreFromCluster -ClusterOrStoragePod $sp
    Write-Stamp ("Selected datastore: {0}" -f $datastoreObj.Name)
  }
  if (-not $datastoreObj -and $cfg.Datastore) {
    $datastoreObj = VMware.VimAutomation.Core\Get-Datastore -Name $cfg.Datastore -ErrorAction Stop
  }
  if (-not $datastoreObj) { throw "Datastore/DatastoreCluster not resolved." }

  # Network (supports Std PG and VDS PG)
  $pgStd = VMware.VimAutomation.Core\Get-VirtualPortGroup -Standard -ErrorAction SilentlyContinue
  $pgDvs = $null; try { $pgDvs = VMware.VimAutomation.Vds\Get-VDPortgroup -ErrorAction SilentlyContinue } catch {}
  $allPG = @(); if ($pgStd){$allPG += $pgStd}; if ($pgDvs){$allPG += $pgDvs}
  $networkObj = $allPG | Where-Object { $_.Name -eq $cfg.Network }
  if (-not $networkObj) { throw "Portgroup '$($cfg.Network)' not found in vCenter '$($cfg.VCSA)'." }
  $usePortGroup = $false; try { $usePortGroup = ($networkObj -is [VMware.VimAutomation.Vds.Types.V1.VDPortgroup]) } catch { $usePortGroup=$false }

  # Template resolution — prefer local template if provided, else TemplateName
  $tmplName = if ($cfg.LocalTemplateName) { $cfg.LocalTemplateName } else { $cfg.TemplateName }
  $localTemplate = Resolve-SingleTemplate -Name $tmplName -Server $cfg.VCSA
  $localTemplate = $localTemplate | Use-SingleTemplate
  Write-Stamp ("Using existing local template '{0}' (skipping Content Library refresh)..." -f $localTemplate.Name)

  # Pre-flight
  if (VMware.VimAutomation.Core\Get-VM -Name $vmName -ErrorAction SilentlyContinue) { throw "A VM named '$vmName' already exists." }

  # Customization (defer to Start-VM stage)
  $oscSpec = $null
  if ($cfg.CustomizationSpec) {
    $oscSpec = VMware.VimAutomation.Core\Get-OSCustomizationSpec -Name $cfg.CustomizationSpec -ErrorAction SilentlyContinue
    if (-not $oscSpec) { Write-Stamp ("Customization Spec '{0}' not found; will deploy without it." -f $cfg.CustomizationSpec) }
  }

  Write-Stamp ("`nDeploying VM '{0}' from local template '{1}'..." -f $vmName,$localTemplate.Name)
  $newVM = New-VM-SafeClone -Name $vmName -Template $localTemplate -Datastore $datastoreObj -Location $vmFolder

  # First NIC mapping tweak (post-clone)
  try {
    $firstNic = VMware.VimAutomation.Core\Get-NetworkAdapter -VM $newVM | Select-Object -First 1
    if ($firstNic) {
      if ($usePortGroup) { VMware.VimAutomation.Core\Set-NetworkAdapter -NetworkAdapter $firstNic -PortGroup $networkObj -Confirm:$false | Out-Null }
      else { VMware.VimAutomation.Core\Set-NetworkAdapter -NetworkAdapter $firstNic -NetworkName $cfg.Network -Confirm:$false | Out-Null }
    }
  } catch { Write-Warning "NIC mapping tweak failed: $($_.Exception.Message)" }

  if ($cfg.RemoveExtraCDROMs) { Ensure-SingleCDHard -VM $newVM }
  if ($cfg.EnsureNICConnected) { Set-VMNicStartConnected -VM $newVM }

  # Notes (minimal Baseline V2 style)
  if ($cfg.ChangeNumber) {
    $stamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    $notes = "CR: $($cfg.ChangeNumber) — deployed $stamp"
    VMware.VimAutomation.Core\Set-VM -VM $newVM -Notes $notes -Confirm:$false | Out-Null
  }

  # Power on & optional customization
  $nicCount = (VMware.VimAutomation.Core\Get-NetworkAdapter -VM $newVM | Measure-Object).Count
  $mapCount = 0
  if ($oscSpec) { try { $mapCount = (Get-OSCustomizationNicMapping -OSCustomizationSpec $oscSpec | Measure-Object).Count } catch { $mapCount = 0 } }

  if ($oscSpec -and ($mapCount -eq 0 -or $mapCount -eq $nicCount)) {
    Write-Stamp ("Starting VM with customization spec '{0}' (NIC maps: {1})..." -f $oscSpec.Name,$mapCount)
    VMware.VimAutomation.Core\Start-VM -VM $newVM -OSCustomizationSpec $oscSpec -Confirm:$false | Out-Null
  } elseif ($oscSpec) {
    Write-Warning ("Customization spec NIC mappings ({0}) don't match VM NICs ({1}); starting without spec." -f $mapCount,$nicCount)
    VMware.VimAutomation.Core\Start-VM -VM $newVM -Confirm:$false | Out-Null
  } else {
    VMware.VimAutomation.Core\Start-VM -VM $newVM -Confirm:$false | Out-Null
  }

  # Post-power-on waits
  if ($postDelay > 0) { Start-Sleep -Seconds $postDelay }
  if ($toolsWait > 0) {
    $deadline = (Get-Date).AddSeconds($toolsWait)
    do {
      $tools = (VMware.VimAutomation.Core\Get-VM -Id $newVM.Id).ExtensionData.Guest.ToolsRunningStatus
      if ($tools -eq 'guestToolsRunning') { break }
      Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)
  }

  Write-Stamp ("Deployment complete: {0}" -f $vmName)
  return $newVM
}

# --- Entry ------------------------------------------------------------------
try {
  $vm = Deploy-FromConfig -ConfigPath $ConfigPath -Verbose:$VerbosePreference
  Write-Stamp ("Done. VM: {0}" -f $vm.Name)
} catch {
  Write-Error $_
  if ($script:LogFile) { Write-Stamp ("ERROR: $($_.Exception.Message)") }
  exit 1
}
