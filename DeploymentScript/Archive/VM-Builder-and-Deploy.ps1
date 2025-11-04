<#
.SYNOPSIS
  Combined console tool to BUILD a deploy JSON and DEPLOY a VM (option 10).

.DESCRIPTION
  This single script contains:
    • A guided JSON builder tailored to UK/IE/US regions, Env (PROD/DEV), cluster & OU rules,
      content library selection, template list, CPU/RAM/disk, VM name confirm, and change number.
    • A full v2 deploy routine that reads the JSON, refreshes the local template, deploys the VM,
      sets NICs to connect at boot, keeps one CD, writes Notes with CR, sets VMware Tools policy
      (UpgradeAtPowerCycle), triggers Tools upgrade, upgrades HW (safely), optionally powers on,
      moves the AD computer account, and prints a final “check status”.

  Menu:
    [1] Build new JSON (guided)
    [10] Deploy VM from an existing JSON
    [99] Build then Deploy (one pass)
    [0] Exit

  JSON is saved into the script directory as <VMName>.json.

.REQUIREMENTS
  - PowerShell 5.1+
  - VMware.PowerCLI (Connect-VIServer, New-VM, etc.)
  - (Optional) ActiveDirectory module for AD OU move

.NOTES
  - CPU default 4, RAM default 12 GB, Disk default 100 GB
  - Folder auto-sets to Production/Development based on Env
  - DatastoreCluster auto-sets per region/US site selection
  - Suppresses the known harmless SDRS OVF warning during CL→seed deployment only
#>

#Requires -Version 5.1
#Requires -Modules VMware.PowerCLI

# =============================================================
# Shared helpers (prompting, stamps, JSON cleaning)
# =============================================================
function Write-Stamp([string]$msg) {
  Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg)
}

function Ask {
  param([string]$Prompt,[string]$Default=$null,[switch]$AllowEmpty)
  while ($true) {
    $full = if ($null -ne $Default -and $Default -ne '') { "$(($Prompt)) [$(($Default))]: " } else { "$(($Prompt)): " }
    $ans = Read-Host $full
    if ([string]::IsNullOrWhiteSpace($ans)) { $ans = $Default }
    if ($AllowEmpty -or -not [string]::IsNullOrWhiteSpace($ans)) { return $ans }
  }
}
function AskInt {
  param([string]$Prompt,[int]$Default)
  while ($true){
    $v = Ask -Prompt $Prompt -Default $Default
    if ($v -as [int] -ne $null){ return [int]$v }
    Write-Host "Please enter a whole number." -ForegroundColor Yellow
  }
}
function AskChoice {
  param([string]$Prompt,[string[]]$Choices,[int]$DefaultIndex=0)
  Write-Host $Prompt -ForegroundColor Cyan
  for ($i=0; $i -lt $Choices.Count; $i++) {
    $m = if ($i -eq $DefaultIndex) { '*' } else { ' ' }
    Write-Host ("  [{0}] {1} {2}" -f $i,$Choices[$i],$m)
  }
  while ($true) {
    $ans = Read-Host "Enter number (default $DefaultIndex)"
    if ([string]::IsNullOrWhiteSpace($ans)) { return $Choices[$DefaultIndex] }
    if ($ans -as [int] -ne $null) {
      $idx = [int]$ans
      if ($idx -ge 0 -and $idx -lt $Choices.Count) { return $Choices[$idx] }
    }
  }
}

function Remove-JsonComments { # allow // and /* */ in JSON
  param([Parameter(Mandatory)][string]$Text)
  $sb = New-Object System.Text.StringBuilder
  $inString=$false; $inLine=$false; $inBlock=$false; $escape=$false
  for ($i=0; $i -lt $Text.Length; $i++) {
    $ch = $Text[$i]
    $nx = if ($i+1 -lt $Text.Length) { $Text[$i+1] } else { [char]0 }
    if ($inLine) { if ($ch -eq "`r" -or $ch -eq "`n") { $inLine=$false; $sb.Append($ch) | Out-Null } continue }
    if ($inBlock){ if ($ch -eq '*' -and $nx -eq '/') { $inBlock=$false; $i++ } continue }
    if ($inString){ $sb.Append($ch) | Out-Null; if (-not $escape -and $ch -eq '"'){ $inString=$false }; $escape = ($ch -eq '\\') -and -not $escape; continue }
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
  $raw   = Get-Content -Raw -Path $Path
  $clean = Remove-JsonComments -Text $raw
  # remove trailing commas
  $clean = [regex]::Replace($clean, ',(?=\s*[\]}])', '')
  try { $clean | ConvertFrom-Json } catch { throw "Config parse error: $_" }
}

# =============================================================
# JSON Builder (guided flow)
# =============================================================
function Build-DeployConfig {
  <# Returns: the full path to the saved JSON file #>

  # Fixed maps
  $RegionToVCSA = @{ 'UK'='ukprim098.bfl.local'; 'US'='usprim004.bfl.local'; 'IE'='ieprim018.bfl.local' }
  $RegionToContentLib = @{ 'UK'='Template Library'; 'IE'='DB4 Packer Templates'; 'US'='Packer templates' }
  $PortGroups = @{
    'PROD'='BZY|ProdVMs_AP|ProdVMs_EPG'
    'DEV'='BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG'
  }
  $USOptions = @(
    @{ Name='US Stretched'; Cluster='Compute';   DSCluster='PROD_POD_CLUSTER'; Site=$null },
    @{ Name='US HAW';       Cluster='HAW-Local'; DSCluster='HAW_LOCAL_CLUSTER'; Site='HAW' },
    @{ Name='US MAR';       Cluster='HAW-Local'; DSCluster='MAR_LOCAL_CLUSTER'; Site='MAR' }
  )
  # OU map (DNs)
  $OU = @{
    'UK|DEV'      = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=London,OU=Accounts Computer,DC=bfl,DC=local'
    'UK|PROD'     = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=London,OU=Accounts Computer,DC=bfl,DC=local'
    'IE|DEV'      = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Ireland,OU=Accounts Computer,DC=bfl,DC=local'
    'IE|PROD'     = 'OU=Application Servers,OU=Servers & Exceptions,OU=Ireland,OU=Accounts Computer,DC=bfl,DC=local'
    'US|DEV|HAW'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local'
    'US|PROD|HAW' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local'
    'US|DEV|MAR'  = 'OU=Application Servers,OU=Development,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local'
    'US|PROD|MAR' = 'OU=Application Servers,OU=Servers & Exceptions,OU=Marlborough,OU=Accounts Computer,DC=bfl,DC=local'
  }
  # DCs (auto set by region/site)
  $ADServerMap = @{
    'UK'      = 'UKPRDC011.bfl.local'
    'IE'      = 'IEPRDC010.bfl.local'
    'US|HAW'  = 'USPRDC030.bfl.local'
    'US|MAR'  = 'USPRDC030.bfl.local'
  }

  # Template item names (Content Library) – standardised to PKR_* where applicable
  $TemplateItemMap = @{
    'Windows 2022'   = @{ UK='PKR_windows_server_2022_std_Current'; IE='PKR_windows_server_2022_std_Current'; US='PKR_windows_server_2022_std_Current' }
    'Windows 2019'   = @{ UK='PKR_windows_server_2019_std_Current'; IE='PKR_windows_server_2019_std_Current'; US='PKR_windows_server_2019_std_Current' }
    'Linux CentOS 7' = @{ UK='PKR_centos-7_Current';                 IE='PKR_centos-7_Current';   US='PKR_centos-7_Current' }
    'Linux RHEL 7'   = @{ UK='PKR_redhat-7_Current';                  IE='PKR_redhat-7_Current';    US='PKR_redhat-7_Current' }
    'Linux RHEL 9'   = @{ UK='PKR_redhat-9_Current';                  IE='PKR_redhat-9_Current';    US='PKR_redhat-9_Current' }
    'Linux SLES 15'  = @{ UK='sles-15-sp4_Current';                   IE='sles-15-sp4_Current';     US='sles-15-sp4_Current' }
  }

  # Customization Spec names per region/OS (set to $null if not used)
  $SpecMap = @{
    'Windows 2022' = @{ UK='Win2022_Template_LD9'; IE='Win2022_Template_DB4'; US='Win2022_Template_US' }
    'Windows 2019' = @{ UK='Win2019_Template_LD9'; IE='Win2019_Template_DB4'; US='Win2019_Template_US' }
    'Linux CentOS 7' = @{ UK=$null; IE=$null; US=$null }
    'Linux RHEL 7'   = @{ UK=$null; IE=$null; US=$null }
    'Linux RHEL 9'   = @{ UK=$null; IE=$null; US=$null }
    'Linux SLES 15'  = @{ UK=$null; IE=$null; US=$null }
  }

  # Step 1: Region
  $region = AskChoice -Prompt 'Select VCSA Region' -Choices @('UK','US','IE') -DefaultIndex 0
  $vcsa   = $RegionToVCSA[$region]
  $clib   = $RegionToContentLib[$region]

  # Step 2: Env -> network + folder
  $env      = AskChoice -Prompt 'Environment' -Choices @('PROD','DEV') -DefaultIndex 1
  $network  = $PortGroups[$env]
  $vmFolder = if ($env -eq 'PROD') { 'Production' } else { 'Development' }
  $templatesFolder = 'Templates'

  # Step 3: Cluster (+US site) + DS cluster + OU
  $cluster=$null; $dsCluster=$null; $usSite=$null
  if ($region -eq 'UK') { $cluster='LD9Compute'; $dsCluster='LD9_DatastoreCluster' }
  elseif ($region -eq 'IE') { $cluster='DB4Compute'; $dsCluster='DB4_DatastoreCluster' }
  else {
    $picked = AskChoice -Prompt 'US Cluster' -Choices ($USOptions | ForEach-Object { $_.Name }) -DefaultIndex 0
    $row = $USOptions | Where-Object { $_.Name -eq $picked }
    $cluster=$row.Cluster; $dsCluster=$row.DSCluster
    if ($row.Site) { $usSite=$row.Site }
    elseif ($picked -eq 'US Stretched') {
      $usSite = AskChoice -Prompt 'Select US site for OU' -Choices @('HAW','MAR') -DefaultIndex 0
    }
  }
  $ouKey = if ($region -eq 'US') { "$region|$env|$usSite" } else { "$region|$env" }
  $adOU = if ($OU.ContainsKey($ouKey)) { $OU[$ouKey] } else { $null }

  # Determine AD Server automatically based on region/site (no prompt)
  $adServerKey = if ($region -eq 'US') { "US|$usSite" } else { $region }
  $adServer = $ADServerMap[$adServerKey]

  # Step 4: Template & CustomizationSpec
  $osChoices = @('Windows 2022','Windows 2019','Linux CentOS 7','Linux RHEL 7','Linux RHEL 9','Linux SLES 15')
  $tmplChoice = AskChoice -Prompt 'Choose OS / Template' -Choices $osChoices -DefaultIndex 0
  $templateItem = $TemplateItemMap[$tmplChoice][$region]
  $customSpec   = $SpecMap[$tmplChoice][$region]

  # Step 5/6/7: CPU/RAM/Disk
  $cpu = AskInt -Prompt 'Total vCPU' -Default 4
  if ($cpu -gt 48) {
    if ($cpu % 2 -ne 0) { Write-Host 'CPU > 48 → 2 sockets; rounding vCPU up to even.' -ForegroundColor Yellow; $cpu++ }
    $sockets=2; $coresPerSocket=[int]($cpu/2)
  } else {
    $sockets=1; $coresPerSocket=$cpu
  }
  $memGB  = AskInt -Prompt 'Memory (GB)' -Default 12
  $diskGB = AskInt -Prompt 'System disk (GB)' -Default 100

  # Step 8: VM name (confirm)
  do {
    $vmName1 = Ask -Prompt 'VM Name'
    $vmName2 = Ask -Prompt 'Confirm VM Name'
    if ($vmName1 -ne $vmName2){ Write-Host 'Names did not match. Try again.' -ForegroundColor Yellow }
  } until ($vmName1 -eq $vmName2)
  $vmName = $vmName1

  # Step 9: Change number
  $changeNum = Ask -Prompt 'Change Number (goes into VM Notes)' -AllowEmpty

  # Compose JSON object
  $vmw = [ordered]@{
    VCSA=$vcsa; Cluster=$cluster; VMHost=$null; VMFolder=$vmFolder; TemplatesFolderName=$templatesFolder; Network=$network;
    Datastore=''; DatastoreCluster=$dsCluster;
    ContentLibrary = [ordered]@{ Library=$clib; Item=$templateItem; LocalTemplateName=$null; ForceReplace=$true };
    VMName=$vmName; CustomizationSpec=$customSpec;
    Hardware=[ordered]@{ CPU=$cpu; MemoryGB=$memGB; DiskGB=$diskGB; Sockets=$sockets; CoresPerSocket=$coresPerSocket }
  }
  $out = [ordered]@{ VMware=$vmw; Options=[ordered]@{ PowerOn=$true; EnsureNICConnected=$true; RemoveExtraCDROMs=$true } }
  if ($adOU) { $out.AD = [ordered]@{ TargetOU=$adOU; Server=$adServer; UseVCenterCreds=$true; WaitForSeconds=600 } }
  if ($changeNum) { $out.ChangeRequestNumber = $changeNum }

  # Save to script directory as <VMName>.json
  $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } elseif ($MyInvocation.MyCommand.Path) { Split-Path -Parent $MyInvocation.MyCommand.Path } else { (Get-Location).Path }
  $outPath = Join-Path $scriptDir ("{0}.json" -f $vmName)
  ($out | ConvertTo-Json -Depth 8) | Set-Content -Path $outPath -Encoding UTF8
  Write-Host "Saved configuration → $outPath" -ForegroundColor Green
  return $outPath
}

# =============================================================
# Deployment core (v2) — functions
# =============================================================
function Convert-StructuredConfig {
  param([Parameter(Mandatory)]$In)
  function Pick { param([object[]]$v) foreach($x in $v){ if($null -ne $x -and "$x" -ne ''){ return $x } } $null }
  if (-not $In.VMware -and -not $In.AD -and -not $In.Options) { return $In }
  $vmw=$In.VMware; $ad=$In.AD; $opt=$In.Options; $cl=$vmw.ContentLibrary; $hw=$vmw.Hardware
  [pscustomobject]@{
    VCSA=Pick @($vmw.VCSA,$vmw.vCenter)
    Cluster=$vmw.Cluster
    VMHost=Pick @($vmw.VMHost,$vmw.Host)
    Folder=Pick @($vmw.VMFolder,$vmw.Folder)
    TemplatesFolderName=Pick @($vmw.TemplatesFolderName,'Templates')
    Network=$vmw.Network
    Datastore=$vmw.Datastore
    DatastoreCluster=$vmw.DatastoreCluster
    ContentLibraryName=Pick @($cl.Library,$In.ContentLibraryName)
    TemplateName=Pick @($cl.Item,$In.TemplateName)
    LocalTemplateName=$cl.LocalTemplateName
    ForceReplace=Pick @($cl.ForceReplace,$In.ForceReplace,$true)
    VMName=$vmw.VMName
    CustomizationSpec=$vmw.CustomizationSpec
    CPU=$hw.CPU; MemoryGB=$hw.MemoryGB; DiskGB=$hw.DiskGB; Sockets=$hw.Sockets; CoresPerSocket=$hw.CoresPerSocket
    ADTargetOU=$ad.TargetOU; ADServer=$ad.Server; ADUseVCenterCreds=Pick @($ad.UseVCenterCreds,$true); ADWaitForSeconds=Pick @($ad.WaitForSeconds,600); ADCredFile=$ad.CredFile
    PowerOn=Pick @($opt.PowerOn,$true); EnsureNICConnected=Pick @($opt.EnsureNICConnected,$true); RemoveExtraCDROMs=Pick @($opt.RemoveExtraCDROMs,$true)
    ChangeRequestNumber=Pick @($opt.ChangeRequestNumber,$In.ChangeRequestNumber,$null)
  }
}
function Select-BestDatastoreFromCluster { param([Parameter(Mandatory)]$Cluster)
  $candidates = Get-Datastore -RelatedObject $Cluster | Where-Object { $_.State -eq 'Available' -and -not $_.MaintenanceMode } | Sort-Object FreeSpaceGB -Descending
  if (-not $candidates) { throw "No suitable datastores found in datastore cluster '$($Cluster.Name)'." }
  return $candidates[0]
}
function Get-DatacenterForCluster { param([Parameter(Mandatory)]$Cluster)
  foreach ($dc in (Get-Datacenter)) {
    $clustersInDc = Get-Cluster -Location $dc -ErrorAction SilentlyContinue
    if ($clustersInDc -and ($clustersInDc | Where-Object { $_.Id -eq $Cluster.Id })) { return $dc }
  }
  throw "Could not resolve Datacenter for cluster '$($Cluster.Name)'."
}
function Ensure-TemplatesFolder { param([Parameter(Mandatory)]$Datacenter,[string]$FolderName='Templates')
  $vmRoot = Get-Folder -Name 'vm' -Location $Datacenter -Type VM -ErrorAction Stop
  $folder = Get-Folder -Name $FolderName -Location $vmRoot -Type VM -ErrorAction SilentlyContinue
  if (-not $folder) { $folder = New-Folder -Name $FolderName -Location $vmRoot -ErrorAction Stop }
  return $folder
}
function Set-VMNicStartConnected { param([Parameter(Mandatory)]$VM)
  $vmView=Get-View -Id $VM.Id; $spec=New-Object VMware.Vim.VirtualMachineConfigSpec; $changes=@()
  foreach ($dev in $vmView.Config.Hardware.Device) {
    if ($dev -is [VMware.Vim.VirtualEthernetCard]) {
      if (-not $dev.Connectable){ $dev.Connectable=New-Object VMware.Vim.VirtualDeviceConnectInfo }
      $dev.Connectable.Connected=$true; $dev.Connectable.StartConnected=$true; $dev.Connectable.AllowGuestControl=$true
      $chg=New-Object VMware.Vim.VirtualDeviceConfigSpec; $chg.operation='edit'; $chg.device=$dev; $changes+=$chg
    }
  }
  if ($changes.Count -gt 0){ $spec.deviceChange=$changes; $vmView.ReconfigVM_Task($spec) | Out-Null }
}
function Ensure-SingleCDHard { param([Parameter(Mandatory)]$VM)
  $vmView=Get-View -Id $VM.Id; $cds=@($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.VirtualCdrom] })
  if ($cds.Count -gt 1){
    $spec=New-Object VMware.Vim.VirtualMachineConfigSpec; $changes=@()
    for($i=1;$i -lt $cds.Count;$i++){ $devSpec=New-Object VMware.Vim.VirtualDeviceConfigSpec; $devSpec.operation='remove'; $devSpec.device=$cds[$i]; $changes+=$devSpec }
    $spec.deviceChange=$changes; $vmView.ReconfigVM_Task($spec)|Out-Null
  }
  $vmView=Get-View -Id $VM.Id; $firstCd=(@($vmView.Config.Hardware.Device | Where-Object { $_ -is [VMware.Vim.VirtualCdrom] }))[0]
  if ($firstCd){
    if (-not $firstCd.Connectable){ $firstCd.Connectable=New-Object VMware.Vim.VirtualDeviceConnectInfo }
    $firstCd.Connectable.Connected=$false; $firstCd.Connectable.StartConnected=$false; $firstCd.Connectable.AllowGuestControl=$true
    $spec2=New-Object VMware.Vim.VirtualMachineConfigSpec; $edit=New-Object VMware.Vim.VirtualDeviceConfigSpec; $edit.operation='edit'; $edit.device=$firstCd
    $spec2.deviceChange=@($edit); $vmView.ReconfigVM_Task($spec2)|Out-Null
  }
}
function Wait-ForADComputer { param([string]$Name,[string]$BaseDN,[int]$TimeoutSec=600,[System.Management.Automation.PSCredential]$Credential,[string]$Server)
  $elapsed=0
  while($elapsed -lt $TimeoutSec){
    try{
      $p=@{ LDAPFilter="(sAMAccountName=$Name`$)"; SearchBase=$BaseDN; SearchScope='Subtree'; ErrorAction='SilentlyContinue' }
      if($Credential){$p.Credential=$Credential}; if($Server){$p.Server=$Server}
      $obj=Get-ADComputer @p
      if($obj){return $obj}
    } catch{}
    Start-Sleep -Seconds 10; $elapsed+=10
  }
  return $null
}
function Get-BaseDNFromTargetOU { param([Parameter(Mandatory)][string]$TargetOU)
  $dcParts = ($TargetOU -split ',') | Where-Object { $_ -match '^\s*DC=' }
  if (-not $dcParts) { throw "TargetOU does not appear to be a distinguished name (missing DC=... parts)." }
  ($dcParts -join ',').Trim()
}
function Get-AdCredential { param([bool]$UseVCenterCreds,[System.Management.Automation.PSCredential]$VcCred,[string]$CredFile)
  if ($UseVCenterCreds -and $VcCred) { return $VcCred }
  if ($CredFile -and (Test-Path $CredFile)) { try { return Import-Clixml -Path $CredFile } catch {} }
  return $null
}
function Wait-ToolsHealthy { param([Parameter(Mandatory)]$VM,[int]$TimeoutSec=600,[int]$PollSec=5)
  $deadline=(Get-Date).AddSeconds($TimeoutSec)
  do{
    try{
      $g=Get-VMGuest -VM $VM -ErrorAction Stop
      if($g.State -eq 'Running' -and $g.ToolsStatus -in 'toolsOk','toolsOld'){ return $g.ToolsStatus }
    } catch{}
    Start-Sleep -Seconds $PollSec
  } while((Get-Date) -lt $deadline)
  return $null
}
# Run New-VM while capturing and re-emitting warnings except the *known* SDRS OVF warning
function New-VM_FilteringOvfWarning {
  param([hashtable]$Params)
  $warn = @()
  $vm = New-VM @Params -WarningAction SilentlyContinue -WarningVariable +warn
  if ($warn) {
    $ignore = [regex]'com\.vmware\.vcenter\.ovf\.ovf_warning.*target_datastore.*sdrs'
    foreach($w in $warn){
      $wStr = [string]$w
      if (-not $ignore.IsMatch($wStr)) { Write-Warning $wStr }
    }
  }
  return $vm
}
function Upgrade-VMHardwareSafely {
  param([Parameter(Mandatory)]$VM,[switch]$RePowerOn)
  try {
    Set-VM -VM $VM -UpgradeHardware -Confirm:$false -ErrorAction Stop | Out-Null
    Write-Stamp "Hardware upgrade request issued."
  } catch {
    $msg = "$($_.Exception.Message)"
    if ($msg -match 'current state.*Powered on') {
      Write-Stamp "Powering off to upgrade hardware…"
      Stop-VM -VM $VM -Confirm:$false -ErrorAction Stop | Out-Null
      do { Start-Sleep 2 } while ((Get-VM -Id $VM.Id).PowerState -ne 'PoweredOff')
      Set-VM -VM $VM -UpgradeHardware -Confirm:$false -ErrorAction Stop | Out-Null
      Write-Stamp "Hardware upgraded while powered off."
      if ($RePowerOn) { Start-VM -VM $VM -Confirm:$false | Out-Null; Write-Stamp "Powered VM back on." }
    } else {
      Write-Warning "Hardware upgrade skipped/failed: $msg"
    }
  }
}

function Deploy-FromCLv2 {
  param([Parameter(Mandatory=$true)][string]$ConfigPath)

  $rawConfig = Read-Config -Path $ConfigPath
  $config = Convert-StructuredConfig -In $rawConfig
  foreach ($key in @('VCSA','Cluster','Network','ContentLibraryName','TemplateName','Folder')) {
    if (-not $config.$key) { throw "Missing required config field: '$key'." }
  }

  # Credential cache
  $CredPathRoot = "C:\Users\GrWay\OneDrive\OneDrive - Beazley Group\Documents\Scripts\DeploymentScript"
  if (-not (Test-Path $CredPathRoot)) { New-Item -Path $CredPathRoot -ItemType Directory -Force | Out-Null }
  $SafeVcsaName = ($config.VCSA -replace '[^a-zA-Z0-9\.-]', '_')
  $CredFile = Join-Path $CredPathRoot ("{0}-cred.xml" -f $SafeVcsaName)
  $cred=$null
  if (Test-Path $CredFile) {
    try { $cred=Import-Clixml -Path $CredFile; if (-not ($cred -and $cred.UserName)) { $cred=$null } else { [void]$cred.GetNetworkCredential().Password } }
    catch { $cred=$null }
  }
  if (-not $cred) {
    Write-Host "Enter credentials for $($config.VCSA) (e.g. administrator@vsphere.local)"
    $cred=Get-Credential
    try { $cred | Export-Clixml -Path $CredFile -Force | Out-Null } catch {}
  }
  try { Set-PowerCLIConfiguration -InvalidCertificateAction Ignore -Confirm:$false | Out-Null } catch {}
  $null = Connect-VIServer -Server $config.VCSA -Credential $cred -ErrorAction Stop

  # Placement
  $clusterObj = Get-Cluster -Name $config.Cluster -ErrorAction Stop
  $dc = Get-DatacenterForCluster -Cluster $clusterObj
  $tplFolderName = if ($config.TemplatesFolderName) { $config.TemplatesFolderName } else { 'Templates' }
  $templatesFolder = Ensure-TemplatesFolder -Datacenter $dc -FolderName $tplFolderName
  $hosts = Get-VMHost -Location $clusterObj | Sort-Object Name
  $vmHost = if ($config.VMHost) {
    $h=$hosts | Where-Object { $_.Name -eq $config.VMHost }
    if (-not $h) { throw "VMHost '$($config.VMHost)' not found in cluster '$($clusterObj.Name)'." }
    $h
  } else { $hosts | Get-Random }

  $datastoreObj=$null
  if ($config.DatastoreCluster) {
    $dsCluster = Get-DatastoreCluster -Name $config.DatastoreCluster -ErrorAction SilentlyContinue
    if ($dsCluster) {
      Write-Host "Using datastore cluster '$($dsCluster.Name)'; selecting datastore with most free space..."
      $datastoreObj = Select-BestDatastoreFromCluster -Cluster $dsCluster
      Write-Host "Selected datastore: $($datastoreObj.Name)"
    }
  }
  if (-not $datastoreObj -and $config.Datastore) { try { $datastoreObj = Get-Datastore -Name $config.Datastore -ErrorAction Stop } catch {} }
  if (-not $datastoreObj) { throw "Provide 'Datastore' or 'DatastoreCluster' in config; none resolved." }

  $pgStd = Get-VirtualPortGroup -Standard -ErrorAction SilentlyContinue
  $pgDvs = Get-VDPortgroup -ErrorAction SilentlyContinue
  $allPG=@(); if($pgStd){$allPG+=$pgStd}; if($pgDvs){$allPG+=$pgDvs}
  $networkObj = $allPG | Where-Object { $_.Name -eq $config.Network }
  if (-not $networkObj) { throw "Portgroup '$($config.Network)' not found." }

  $usePortGroup=$false
  try { $usePortGroup = ($networkObj -is [VMware.VimAutomation.Vds.Types.V1.VDPortgroup]) -or ($networkObj.GetType().Name -eq 'VDPortgroup') }
  catch { $usePortGroup=$false }

  $vmFolder = Get-Folder -Name $config.Folder -Type VM -ErrorAction Stop

  # Source CL item
  $clItem = Get-ContentLibraryItem -ContentLibrary $config.ContentLibraryName -Name $config.TemplateName -ErrorAction SilentlyContinue
  if (-not $clItem) { throw "Content Library item '$($config.TemplateName)' not found in library '$($config.ContentLibraryName)'." }
  $localTemplateName = if ($config.LocalTemplateName) { $config.LocalTemplateName } else { $config.TemplateName }

  # Phase 1: refresh local template
  if ($config.ForceReplace) {
    if ($t=Get-Template -Name $localTemplateName -ErrorAction SilentlyContinue){
      Write-Host "Template '$localTemplateName' exists. Removing (force)..."
      Remove-Template -Template $t -DeletePermanently -Confirm:$false -ErrorAction Stop
    }
    if ($v=Get-VM -Name $localTemplateName -ErrorAction SilentlyContinue){
      Write-Host "VM '$localTemplateName' exists. Removing (force)..."
      Remove-VM -VM $v -DeletePermanently -Confirm:$false -ErrorAction Stop
    }
  }

  Write-Host "`nRefreshing local template from CL item '$($clItem.Name)' → '$localTemplateName'..."
  $seedParams=@{ Name=$localTemplateName; ContentLibraryItem=$clItem; VMHost=$vmHost; Datastore=$datastoreObj; Location=$templatesFolder; ErrorAction='Stop' }

  # OVF/OVA mapping (target cluster to avoid prompt)
  $ovfCfg=$null
  try {
    $ovfCfg = Get-OvfConfiguration -ContentLibraryItem $clItem -Target $clusterObj -ErrorAction Stop
    Write-Host 'Detected OVF/OVA; mapping networks via OVF configuration...'
  } catch {
    Write-Host 'Detected VM template CL item; setting network parameter...'
  }

  if ($ovfCfg){
    if ($ovfCfg.NetworkMapping){ foreach($k in $ovfCfg.NetworkMapping.Keys){ $ovfCfg.NetworkMapping[$k].Value=$networkObj } }
    $seedParams['OvfConfiguration']=$ovfCfg
  } else {
    if ($usePortGroup){ $seedParams['PortGroup']=$networkObj }
    else { $seedParams['NetworkName']=$networkObj.Name }
  }

  # Create seed VM while filtering the single noisy OVF+SDRS warning
  $seedVM = New-VM_FilteringOvfWarning -Params $seedParams
  if (-not $seedVM){ throw 'Seed VM deployment failed.' }

  if ($config.RemoveExtraCDROMs){ Ensure-SingleCDHard -VM $seedVM }
  if ($config.EnsureNICConnected){ Set-VMNicStartConnected -VM $seedVM }
  if ($seedVM.PowerState -ne 'PoweredOff'){ Stop-VM -VM $seedVM -Confirm:$false | Out-Null }

  Write-Host "Converting '$localTemplateName' to a local vSphere Template in '$tplFolderName'..."
  $localTemplate = Set-VM -VM $seedVM -ToTemplate -Name $localTemplateName -Confirm:$false -ErrorAction Stop
  Write-Host ("✅ Local template ready: '{0}'." -f $localTemplate.Name)

  # Phase 2: deploy final VM
  $vmName=$config.VMName
  if (-not $vmName){ throw 'VMName is required in config for the VM deployment phase.' }
  if (Get-VM -Name $vmName -ErrorAction SilentlyContinue){ throw "A VM named '$vmName' already exists." }

  $cpu=$null;$memGB=$null;$diskGB=$null;$coresPerSocket=$null;$sockets=$null
  if ($config.CPU){$cpu=[int]$config.CPU}
  if ($config.MemoryGB){$memGB=[int]$config.MemoryGB}
  if ($config.DiskGB){$diskGB=[int]$config.DiskGB}
  if ($config.CoresPerSocket){$coresPerSocket=[int]$config.CoresPerSocket}
  if ($config.Sockets){$sockets=[int]$config.Sockets}
  if ($coresPerSocket -and $sockets -and -not $cpu){ $cpu = $coresPerSocket * $sockets }
  if ($cpu -and $coresPerSocket){
    if ($cpu % $coresPerSocket -ne 0){ throw "CPU ($cpu) must be divisible by CoresPerSocket ($coresPerSocket)." }
    if ($sockets -and ($sockets -ne ($cpu / $coresPerSocket))){ throw "Sockets ($sockets) must equal CPU/CoresPerSocket ($($cpu / $coresPerSocket))." }
  }

  $oscSpec=$null
  if ($config.CustomizationSpec){
    $oscSpec=Get-OSCustomizationSpec -Name $config.CustomizationSpec -ErrorAction SilentlyContinue
    if (-not $oscSpec){ Write-Warning "OS Customization Spec '$($config.CustomizationSpec)' not found; continuing without it." }
  }

  Write-Host ("`nDeploying VM '{0}' from local template '{1}'..." -f $vmName,$localTemplate.Name)
  $createParams=@{ Name=$vmName; Template=$localTemplate; VMHost=$vmHost; Datastore=$datastoreObj; Location=$vmFolder; ErrorAction='Stop' }
  if ($oscSpec){ $createParams['OSCustomizationSpec']=$oscSpec }
  $newVM = New-VM @createParams
  if (-not $newVM){ throw 'VM deployment from local template failed.' }

  try{
    $firstNic=Get-NetworkAdapter -VM $newVM | Select-Object -First 1
    if($firstNic){
      if($usePortGroup){ Set-NetworkAdapter -NetworkAdapter $firstNic -PortGroup $networkObj -Confirm:$false | Out-Null }
      else { Set-NetworkAdapter -NetworkAdapter $firstNic -NetworkName $config.Network -Confirm:$false | Out-Null }
      Write-Stamp ("Ensured NIC 0 on '{0}'" -f $networkObj.Name)
    }
  } catch { Write-Warning "Post-clone network set failed: $_" }

  if ($config.RemoveExtraCDROMs){ Ensure-SingleCDHard -VM $newVM }
  if ($config.EnsureNICConnected){ Set-VMNicStartConnected -VM $newVM }

  if ($cpu -or $memGB -or $coresPerSocket){
    $setParams=@{ VM=$newVM; Confirm=$false; ErrorAction='SilentlyContinue' }
    if($cpu){$setParams['NumCPU']=$cpu}
    if($memGB){$setParams['MemoryGB']=$memGB}
    if($coresPerSocket){$setParams['CoresPerSocket']=$coresPerSocket}
    Set-VM @setParams | Out-Null
  }
  if ($diskGB){
    $sysDisk=Get-HardDisk -VM $newVM | Select-Object -First 1
    if($sysDisk -and $diskGB -gt $sysDisk.CapacityGB){
      Set-HardDisk -HardDisk $sysDisk -CapacityGB $diskGB -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
    }
  }

  # Notes / Tools policy
  if ($config.ChangeRequestNumber){
    $note = "CR: $($config.ChangeRequestNumber) — deployed $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
    try{ Set-VM -VM $newVM -Notes $note -Confirm:$false | Out-Null; Write-Stamp "Notes set: $note" } catch { Write-Warning "Failed to set Notes: $_" }
  } else { Write-Stamp 'No ChangeRequestNumber in config; leaving Notes as-is.' }
  try{
    Set-VM -VM $newVM -ToolsUpgradePolicy UpgradeAtPowerCycle -Confirm:$false | Out-Null
    $view=Get-View -Id $newVM.Id
    Write-Stamp ("Tools policy now: {0}" -f $view.Config.Tools.ToolsUpgradePolicy)
  } catch { Write-Warning "Failed to set Tools upgrade policy: $_" }

  # Power on & AD move
  if ($config.PowerOn){
    Write-Stamp 'Powering on VM...'
    Start-VM -VM $newVM -Confirm:$false | Out-Null
    $status = Wait-ToolsHealthy -VM $newVM -TimeoutSec 600
    $statusText = if ($status) { $status } else { 'unknown' }
    Write-Stamp ("Initial VMware Tools status: {0}" -f $statusText)
  }

  if ($config.ADTargetOU){
    $adLoaded=$false
    try{ Import-Module ActiveDirectory -ErrorAction Stop; $adLoaded=$true }
    catch { Write-Warning 'ActiveDirectory module not available; skipping AD move.' }

    if ($adLoaded){
      $adCred=Get-AdCredential -UseVCenterCreds:$config.ADUseVCenterCreds -VcCred:$cred -CredFile:$config.ADCredFile
      $adServer=$config.ADServer
      $ouExists=$false
      try{
        $p=@{ Identity=$config.ADTargetOU; ErrorAction='Stop' }
        if($adServer){$p.Server=$adServer}; if($adCred){$p.Credential=$adCred}
        Get-ADOrganizationalUnit @p | Out-Null
        $ouExists=$true
      } catch {
        $srvText = if ([string]::IsNullOrEmpty($adServer)) { '' } else { " on server '$($adServer)'" }
        Write-Warning ("Target OU DN not found: '{0}'{1}; skipping AD move." -f $config.ADTargetOU,$srvText)
      }

      if ($ouExists){
        $baseDn=Get-BaseDNFromTargetOU -TargetOU $config.ADTargetOU
        $timeoutSec= if ($config.ADWaitForSeconds){ [int]$config.ADWaitForSeconds } else { 600 }
        Write-Stamp ("Waiting up to {0}s for AD computer '{1}'..." -f $timeoutSec,$vmName)
        $adComp=Wait-ForADComputer -Name $vmName -BaseDN $baseDn -TimeoutSec $timeoutSec -Credential $adCred -Server $adServer
        if($adComp){
          try{
            $p=@{ Identity=$adComp.DistinguishedName; TargetPath=$config.ADTargetOU; ErrorAction='Stop' }
            if($adCred){$p.Credential=$adCred}; if($adServer){$p.Server=$adServer}
            Move-ADObject @p
            $srvText = if ([string]::IsNullOrEmpty($adServer)) { '' } else { " on '$($adServer)'" }
            Write-Stamp ("Moved AD computer '{0}' to '{1}'{2}." -f $vmName,$config.ADTargetOU,$srvText)
          } catch {
            $srvText = if ([string]::IsNullOrEmpty($adServer)) { '' } else { " on '$($adServer)'" }
            Write-Warning ("Move-ADObject failed{0} to '{1}': {2}" -f $srvText,$config.ADTargetOU,$_)
          }
        } else {
          Write-Warning ("AD computer '{0}' not found within timeout ({1}s); skipping move." -f $vmName,$timeoutSec)
        }
      }
    }
  }

  # Finalize: Tools upgrade (while powered on), then safe HW upgrade, then status
  try{
    Update-Tools -VM $newVM -NoReboot -ErrorAction SilentlyContinue | Out-Null
    Write-Stamp 'Triggered VMware Tools upgrade.'
    $status2 = Wait-ToolsHealthy -VM $newVM -TimeoutSec 600
    $statusText2 = if ($status2) { $status2 } else { 'unknown' }
    Write-Stamp ("Post-upgrade VMware Tools status: {0}" -f $statusText2)
  } catch { Write-Warning "Update-Tools threw an error: $_" }

  Upgrade-VMHardwareSafely -VM $newVM -RePowerOn:($config.PowerOn)

  # Refresh view + quick status
  try{
    $v=Get-View -Id $newVM.Id; $v.UpdateViewData(); [void](Get-VM -Id $newVM.Id)
    $guest = Get-VMGuest -VM $newVM -ErrorAction SilentlyContinue
    $toolsState = if ($guest) { $guest.ToolsStatus } else { 'unknown' }
    $hwVer = (Get-VM -Id $newVM.Id).Version
    Write-Stamp ("Final status → Tools: {0}; HW: {1}; Power: {2}" -f $toolsState,$hwVer,(Get-VM -Id $newVM.Id).PowerState)
  } catch { Write-Warning "View/status refresh failed: $_" }

  $powerMsg = if ($config.PowerOn){ 'powered on; ' } else { 'not powered on; ' }
  Write-Host ("✅ VM '{0}' deployed (v2). NICs connect at boot; Tools policy set & upgrade attempted; HW updated; extra CD drives removed; {1}AD move attempted if 'ADTargetOU' provided." -f $vmName,$powerMsg)
}

# =============================================================
# Menu
# =============================================================
$global:LastConfigPath = $null

function Show-Menu {
  Write-Host ""
  Write-Host "==== VM Builder & Deployer ====" -ForegroundColor Cyan
  Write-Host "  [1]  Build new JSON (guided)"
  Write-Host "  [10] Deploy VM from JSON"
  Write-Host "  [99] Build then Deploy"
  Write-Host "  [0]  Exit"
}

while ($true) {
  Show-Menu
  $sel = Read-Host "Select option"
  switch ($sel) {
    '1' {
      try { $global:LastConfigPath = Build-DeployConfig } catch { Write-Warning $_ }
    }
    '10' {
      $path = if ($global:LastConfigPath) { Ask -Prompt 'ConfigPath' -Default $global:LastConfigPath } else { Ask -Prompt 'ConfigPath' }
      if (-not [string]::IsNullOrWhiteSpace($path)) {
        try { Deploy-FromCLv2 -ConfigPath $path } catch { Write-Warning $_ }
      }
    }
    '99' {
      try {
        $global:LastConfigPath = Build-DeployConfig
        if ($global:LastConfigPath) { Deploy-FromCLv2 -ConfigPath $global:LastConfigPath }
      } catch { Write-Warning $_ }
    }
    '0' { break }
    default { Write-Host 'Unknown option.' -ForegroundColor Yellow }
  }
}
