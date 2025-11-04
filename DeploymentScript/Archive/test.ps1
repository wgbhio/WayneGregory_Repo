#Requires -Version 5.1
#Requires -Modules VMware.PowerCLI

<#
VM Deploy V2 – Fully Integrated (Configurable waits + PVSCSI + AD reliability)
- OU move happens immediately after power-on and BEFORE waiting for VMware Tools
- Wait knobs in JSON: Options.ToolsWaitSeconds (default 90), Options.PostPowerOnDelay (default 15), AD.WaitForSeconds (default 180)
- Clear success/failure/skip logging for AD move (with reason)
- Pre-create computer account in target OU (optional, automatic) to avoid race
- Additional data disks: interactive prompt → round-robin across PVSCSI0..3 (wrap)
- Deploy-from-Content Library (refresh local template)
- Uses root VM folders: Templates, Production, Development
- Step-by-step logging (Invoke-Step) + transcript to %TEMP%
#>

function Write-Stamp([string]$msg) {
  Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg)
}

function Invoke-Step {
  param(
    [Parameter(Mandatory)][string]$Name,
    [Parameter(Mandatory)][scriptblock]$Script
  )
  Write-Stamp ("▶ {0}" -f $Name)
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  try {
    $r = & $Script
    $sw.Stop()
    Write-Stamp ("✓ {0} ({1:g})" -f $Name,$sw.Elapsed)
    return $r
  } catch {
    $sw.Stop()
    Write-Host ("✗ {0} FAILED after {1:g}: {2}" -f $Name,$sw.Elapsed,$_.Exception.Message) -ForegroundColor Red
    throw
  }
}

# --- Prompt helpers ---------------------------------------------------------
function Ask {
  param([string]$Prompt,[string]$Default=$null,[switch]$AllowEmpty)
  while ($true) {
    $full = if ($null -ne $Default -and $Default -ne '') { "${Prompt} [${Default}]: " } else { "${Prompt}: " }
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
  $clean = [regex]::Replace($clean, ',(?=\s*[\]}])', '')
  try { $clean | ConvertFrom-Json } catch { throw "Config parse error: $_" }
}

# --- Folder resolvers -------------------------------------------------------
function Resolve-RootFolder {
  param([Parameter(Mandatory)]$Datacenter,[Parameter(Mandatory)][string]$Name)
  $vmRoot  = Get-Folder -Name 'vm' -Location $Datacenter -Type VM -ErrorAction Stop | Select-Object -First 1
  $folder  = Get-Folder -Location $vmRoot -Type VM -ErrorAction SilentlyContinue |
             Where-Object { $_.Name -eq $Name } |
             Select-Object -First 1
  if (-not $folder) { $folder = New-Folder -Name $Name -Location $vmRoot -ErrorAction Stop }
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
      $p=@{ LDAPFilter="(sAMAccountName=$Name`$)"; SearchBase=$BaseDN; SearchScope='Subtree'; ErrorAction='SilentlyContinue' }
      if($Credential){$p.Credential=$Credential}; if($Server){$p.Server=$Server}
      $obj=Get-ADComputer @p
      if($obj){return $obj}
    } catch{}
    Start-Sleep -Seconds 5; $elapsed+=5
  }
  return $null
}
function Get-BaseDNFromTargetOU { param([Parameter(Mandatory)][string]$TargetOU)
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
    Write-Host "Enter credentials for Active Directory domain (e.g. bfl\\administrator)" -ForegroundColor Cyan
    $c = Get-Credential
    try { $dir = Split-Path -Parent $DefaultCredFilePath; if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null } } catch {}
    try { if ($DefaultCredFilePath) { $c | Export-Clixml -Path $DefaultCredFilePath -Force | Out-Null } } catch {}
    return $c
  }
  return $null
}
function Resolve-SearchDC {
  param([string]$Region,[string]$UsSite,[string]$Preferred)
  try {
    Import-Module ActiveDirectory -ErrorAction Stop | Out-Null
    $dcs = Get-ADDomainController -Filter * -ErrorAction Stop | Where-Object { -not $_.IsReadOnly }
    if ($Region -eq 'US') {
      $siteMatches = switch ($UsSite) { 'HAW' {'Hawthorne','HAW'} 'MAR' {'Marlborough','MAR'} default { @() } }
      $byExact = if ($siteMatches){ $dcs | Where-Object { $_.Site -in $siteMatches } }
      $byName  = $dcs | Where-Object { $_.HostName -like 'USPRDC*' }
      $candidate =
        ($byExact | Where-Object { $_.HostName -match 'USPRDC036' } | Select-Object -First 1) `
        ?? ($byExact | Select-Object -First 1) `
        ?? ($byName  | Where-Object { $_.HostName -match 'USPRDC036' } | Select-Object -First 1) `
        ?? ($byName  | Select-Object -First 1)
      if ($candidate) { return $candidate.HostName }
    }
    ($dcs | Select-Object -First 1 -ExpandProperty HostName) ?? $Preferred
  } catch { return $Preferred }
}

# --- VMware helpers ---------------------------------------------------------
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
function Wait-ToolsHealthy { param([Parameter(Mandatory)]$VM,[int]$TimeoutSec=180,[int]$PollSec=5)
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
function New-VM_FilteringOvfWarning {
  param([hashtable]$Params)
  $warn = @()
  $vm = New-VM @Params -WarningAction SilentlyContinue -WarningVariable +warn
  if ($warn) {
    $ignore = [regex]'com\.vmware\.vcenter\.ovf\.ovf_warning.*target_datastore.*sdrs'
    foreach($w in $warn){ $wStr = [string]$w; if (-not $ignore.IsMatch($wStr)) { Write-Warning $wStr } }
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
    } else { Write-Warning "Hardware upgrade skipped/failed: $msg" }
  }
}

# --- PVSCSI helpers (PowerCLI version-compatible) ---------------------------
function Ensure-PvScsiControllers {
  param(
    [Parameter(Mandatory)]$VM,
    [int]$DesiredCount=4    # create up to this many ParaVirtual controllers
  )
  # Get current controllers
  $allCtrls = Get-ScsiController -VM $VM -ErrorAction SilentlyContinue
  $pvCtrls  = @($allCtrls | Where-Object { $_.Type -eq 'ParaVirtual' })

  # Create missing PVSCSI controllers without specifying bus number
  # (older PowerCLI assigns the next available bus automatically)
  while ($pvCtrls.Count -lt $DesiredCount) {
    try {
      New-ScsiController -Type ParaVirtual -BusSharingMode NoSharing -VM $VM -Confirm:$false | Out-Null
      Write-Stamp ("Created PVSCSI controller (now {0})" -f ($pvCtrls.Count + 1))
    } catch {
      Write-Warning ("Failed to create PVSCSI controller: {0}" -f $_.Exception.Message)
      break
    }
    $allCtrls = Get-ScsiController -VM $VM -ErrorAction SilentlyContinue
    $pvCtrls  = @($allCtrls | Where-Object { $_.Type -eq 'ParaVirtual' })
  }

  # Return PVSCSI controllers sorted (stable order for round-robin)
  return ($pvCtrls | Sort-Object ControllerKey)
}

function Add-AdditionalDisksRoundRobin {
  param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)]$Datastore,[Parameter(Mandatory)][int[]]$SizesGB)
  if (-not $SizesGB -or $SizesGB.Count -eq 0) { return }
  $pvCtrls = Ensure-PvScsiControllers -VM $VM -Count 4
  if (-not $pvCtrls -or $pvCtrls.Count -lt 1) { throw "No PVSCSI controllers available after Ensure-PvScsiControllers." }
  for ($i=0; $i -lt $SizesGB.Count; $i++) {
    $ctrl = $pvCtrls[ $i % 4 ]
    $size = [int]$SizesGB[$i]
    New-HardDisk -VM $VM -CapacityGB $size -Datastore $Datastore -Controller $ctrl -Confirm:$false | Out-Null
    Write-Stamp ("Added data disk #{0} ({1} GB) on {2}" -f ($i+1), $size, $ctrl.Name)
  }
}

# --- Builder ---------------------------------------------------------------
function Build-DeployConfig {
  # Fixed maps
  $RegionToVCSA = @{ 'UK'='ukprim098.bfl.local'; 'US'='usprim004.bfl.local'; 'IE'='ieprim018.bfl.local' }
  $RegionToContentLib = @{ 'UK'='Template Library'; 'IE'='DB4 Packer Templates'; 'US'='Packer templates' }

  # Region+Env -> Network
  $NetworkMap = @{
    'US|DEV'  = 'VLAN_2032_Stretch'; 'US|PROD' = 'VLAN_2002_Stretch'
    'UK|PROD' = 'BZY|ProdVMs_AP|ProdVMs_EPG'; 'UK|DEV'  = 'BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG'
    'IE|DEV'  = 'BZY|DevVMsDITestDC_AP|DevVMsDITestDC_EPG'; 'IE|PROD' = 'BZY|ProdVMs_AP|ProdVMs_EPG'
  }

  $USOptions = @(
    @{ Name='US Stretched'; Cluster='Compute';   DSCluster='PROD_POD_CLUSTER'; Site=$null },
    @{ Name='US HAW';       Cluster='HAW-Local'; DSCluster='HAW_LOCAL_CLUSTER'; Site='HAW' },
    @{ Name='US MAR';       Cluster='MAR-Local'; DSCluster='MAR_LOCAL_CLUSTER'; Site='MAR' }
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
    'Linux RHEL 7'=@{ UK=$null; IE=$null; US=$null }
    'Linux RHEL 9'=@{ UK=$null; IE=$null; US=$null }
    'Linux SLES 15'=@{ UK=$null; IE=$null; US=$null }
  }

  # Step 1: Region
  $region = AskChoice -Prompt 'Select VCSA Region' -Choices @('UK','US','IE') -DefaultIndex 0
  $vcsa   = $RegionToVCSA[$region]
  $clib   = $RegionToContentLib[$region]

  # Step 2: Env
  $env      = AskChoice -Prompt 'Environment' -Choices @('PROD','DEV') -DefaultIndex 1
  $netKey   = "$region|$env"
  $network  = $NetworkMap[$netKey]

  # Root folders
  $vmFolder        = if ($env -eq 'PROD') { 'Production' } else { 'Development' }
  $templatesFolder = 'Templates'

  # Step 3: Cluster + OU selection
  $cluster=$null; $dsCluster=$null; $usSite=$null
  if ($region -eq 'UK') { $cluster='LD9Compute'; $dsCluster='LD9_DatastoreCluster' }
  elseif ($region -eq 'IE') { $cluster='DB4Compute'; $dsCluster='DB4_DatastoreCluster' }
  else {
    $picked = AskChoice -Prompt 'US Cluster' -Choices ($USOptions | ForEach-Object { $_.Name }) -DefaultIndex 0
    $row = $USOptions | Where-Object { $_.Name -eq $picked }
    $cluster=$row.Cluster; $dsCluster=$row.DSCluster
    if ($row.Site) { $usSite=$row.Site }
    elseif ($picked -eq 'US Stretched') { $usSite = AskChoice -Prompt 'Select US site for OU' -Choices @('HAW','MAR') -DefaultIndex 0 }
  }
  $ouKey = if ($region -eq 'US') { "$region|$env|$usSite" } else { "$region|$env" }
  $adOU = if ($OU.ContainsKey($ouKey)) { $OU[$ouKey] } else { $null }
  $adServer = if ($region -eq 'US') { $ADServerMap["US|$usSite"] } else { $ADServerMap[$region] }

  # Step 4: Template & Spec
  $osChoices = @('Windows 2022','Windows 2019','Linux CentOS 7','Linux RHEL 7','Linux RHEL 9','Linux SLES 15')
  $tmplChoice = AskChoice -Prompt 'Choose OS / Template' -Choices $osChoices -DefaultIndex 0
  $templateItem = $TemplateItemMap[$tmplChoice][$region]
  $customSpec   = $SpecMap[$tmplChoice][$region]

  # Step 5/6/7: CPU/RAM/Disk
  $cpu = AskInt -Prompt 'Total vCPU' -Default 4
  if ($cpu -gt 48) {
    if ($cpu % 2 -ne 0) { Write-Host 'CPU > 48 → 2 sockets; rounding vCPU up to even.' -ForegroundColor Yellow; $cpu++ }
    $sockets=2; $coresPerSocket=[int]($cpu/2)
  } else { $sockets=1; $coresPerSocket=$cpu }
  $memGB  = AskInt -Prompt 'Memory (GB)' -Default 12
  $diskGB = AskInt -Prompt 'System disk (GB)' -Default 100

  # NEW: interactive additional disks
  $addCount = AskInt -Prompt 'How many additional data disks?' -Default 0
  $additionalDisks = @()
  for ($i=1; $i -le $addCount; $i++){
    $sz = AskInt -Prompt ("  Size of additional disk #{0} (GB)" -f $i) -Default 50
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

  $vmw = [ordered]@{
    VCSA=$vcsa; Cluster=$cluster; VMHost=$null; VMFolder=$vmFolder; TemplatesFolderName=$templatesFolder; Network=$network;
    Datastore=''; DatastoreCluster=$dsCluster;
    ContentLibrary=[ordered]@{ Library=$clib; Item=$templateItem; LocalTemplateName=$null; ForceReplace=$true };
    VMName=$vmName; CustomizationSpec=$customSpec;
    Hardware=[ordered]@{
      CPU=$cpu; MemoryGB=$memGB; DiskGB=$diskGB;
      AdditionalDiskGB=0;                 # back-compat (unused by wizard)
      AdditionalDisks=$additionalDisks;   # interactive list
      Sockets=$sockets; CoresPerSocket=$coresPerSocket
    }
  }

  $out = [ordered]@{
    VMware=$vmw
    Options=[ordered]@{
      PowerOn=$true; EnsureNICConnected=$true; RemoveExtraCDROMs=$true
      ToolsWaitSeconds=90
      PostPowerOnDelay=15
    }
  }
  if ($adOU) {
    $adObj = [ordered]@{ TargetOU=$adOU; Server=$adServer; UseVCenterCreds=$true; WaitForSeconds=180; ForceSync=$true }
    if ($region -eq 'US') { $adObj.PostSyncPeers = @('UKPRDC011.bfl.local') }
    $out.AD = $adObj
  }
  if ($changeNum) { $out.ChangeRequestNumber = $changeNum }

  $scriptDir = if ($PSScriptRoot) { $PSScriptRoot } elseif ($MyInvocation.MyCommand.Path) { Split-Path -Parent $MyInvocation.MyCommand.Path } else { (Get-Location).Path }
  $outPath = Join-Path $scriptDir ("{0}.json" -f $vmName)
  ($out | ConvertTo-Json -Depth 8) | Set-Content -Path $outPath -Encoding UTF8 -NoNewline
  Write-Host "Saved configuration → $outPath" -ForegroundColor Green
  return $outPath
}

# --- Config normalizer -----------------------------------------------------
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
    CPU=$hw.CPU; MemoryGB=$hw.MemoryGB; DiskGB=$hw.DiskGB;
    AdditionalDiskGB=$hw.AdditionalDiskGB;      # single disk (legacy)
    AdditionalDisks=$hw.AdditionalDisks;        # array
    Sockets=$hw.Sockets; CoresPerSocket=$hw.CoresPerSocket
    ADTargetOU=$ad.TargetOU; ADServer=$ad.Server; ADUseVCenterCreds=Pick @($ad.UseVCenterCreds,$true); ADWaitForSeconds=Pick @($ad.WaitForSeconds,180); ADCredFile=$ad.CredFile
    ADForceSync=Pick @($ad.ForceSync,$false); ADPostSyncPeers=$ad.PostSyncPeers
    PowerOn=Pick @($opt.PowerOn,$true); EnsureNICConnected=Pick @($opt.EnsureNICConnected,$true); RemoveExtraCDROMs=Pick @($opt.RemoveExtraCDROMs,$true)
    ToolsWaitSeconds=Pick @($opt.ToolsWaitSeconds,180)
    PostPowerOnDelay=Pick @($opt.PostPowerOnDelay,15)
    ChangeRequestNumber=Pick @($opt.ChangeRequestNumber,$In.ChangeRequestNumber,$null)
  }
}

# --- Main deploy -----------------------------------------------------------

function Deploy-FromCLv2 {
  param([Parameter(Mandatory=$true)][string]$ConfigPath)

  # --- Local helpers (scoped) ----------------------------------------------
  function Write-Stamp([string]$msg){ Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg) }
  function Invoke-Step {
    param([Parameter(Mandatory)][string]$Name,[Parameter(Mandatory)][scriptblock]$Script)
    Write-Stamp ("▶ {0}" -f $Name); $sw=[System.Diagnostics.Stopwatch]::StartNew()
    try{ $r=& $Script; $sw.Stop(); Write-Stamp ("✓ {0} ({1:g})" -f $Name,$sw.Elapsed); return $r }
    catch{
      $sw.Stop()
      Write-Host ("✗ {0} FAILED after {1:g}: {2}" -f $Name,$sw.Elapsed,$_.Exception.Message) -ForegroundColor Red
      if ($_.Exception){ Write-Host ("  • EXCEPTION: {0}" -f $_.Exception.ToString()) -ForegroundColor Red }
      if ($_.ScriptStackTrace){ Write-Host ("  • STACK: {0}" -f $_.ScriptStackTrace) -ForegroundColor DarkRed }
      throw
    }
  }

  # vSphere API helpers for robust SCSI/Disks on older PowerCLI --------------
  function Get-ScsiSnapshot {
    param([Parameter(Mandatory)]$VM)
    $v = Get-View -Id $VM.Id
    $list = @()
    foreach($d in $v.Config.Hardware.Device){
      if ($d -is [VMware.Vim.VirtualSCSIController]) {
        $list += [pscustomobject]@{ Bus=$d.BusNumber; Key=$d.Key; Type=$d.GetType().Name }
      }
    }
    $list | Sort-Object Bus
  }
  function Add-PvScsiControllerApi {
    param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)][int]$BusNumber)
    $vmView = Get-View -Id $VM.Id
    $exists = $vmView.Config.Hardware.Device |
              Where-Object { $_ -is [VMware.Vim.ParaVirtualSCSIController] -and $_.BusNumber -eq $BusNumber }
    if ($exists) { return $true }
    $spec  = New-Object VMware.Vim.VirtualMachineConfigSpec
    $ctrl  = New-Object VMware.Vim.ParaVirtualSCSIController
    $ctrl.Key           = -1000 - $BusNumber
    $ctrl.BusNumber     = $BusNumber
    $ctrl.SharedBus     = [VMware.Vim.VirtualSCSISharing]::noSharing
    $ctrl.ControllerKey = 100
    $devSpec = New-Object VMware.Vim.VirtualDeviceConfigSpec
    $devSpec.Operation  = [VMware.Vim.VirtualDeviceConfigSpecOperation]::add
    $devSpec.Device     = $ctrl
    $spec.DeviceChange  = @($devSpec)
    (Get-View -Id $VM.Id).ReconfigVM_Task($spec) | Out-Null
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
    foreach($u in 0..15){ if ($u -ne 7 -and $used -notcontains $u){ return $u } }
    throw "No free SCSI unit number left on controller key $ControllerKey."
  }
  function Move-HardDiskToBusApi {
    param([Parameter(Mandatory)]$VM,[Parameter(Mandatory)]$HardDisk,[Parameter(Mandatory)][int]$TargetBus)
    $vmView = Get-View -Id $VM.Id
    if ($TargetBus -ge 1) {
      $have = $vmView.Config.Hardware.Device |
              Where-Object { $_ -is [VMware.Vim.ParaVirtualSCSIController] -and $_.BusNumber -eq $TargetBus } |
              Select-Object -First 1
      if (-not $have) {
        Add-PvScsiControllerApi -VM $VM -BusNumber $TargetBus | Out-Null
        Start-Sleep -Milliseconds 700
        $vmView = Get-View -Id $VM.Id
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

  # AD helpers ---------------------------------------------------------------
  function Get-AdDcCandidates {
    param(
      [string]$Region,     # 'US'|'UK'|'IE'|'OTHER'
      [string]$UsSite,     # 'HAW'|'MAR'|$null
      [string]$Preferred   # explicit DC hostname preferred
    )
    $cands = @()
    if ($Preferred) { $cands += $Preferred }
    try {
      Import-Module ActiveDirectory -ErrorAction Stop | Out-Null
      $all = Get-ADDomainController -Filter * -ErrorAction Stop
      if ($Region -eq 'US' -and $UsSite) {
        $siteName = if ($UsSite -eq 'HAW') { 'Hawthorne' } elseif ($UsSite -eq 'MAR') { 'Marlborough' } else { $null }
        if ($siteName) {
          $cands += ($all | Where-Object { $_.Site -eq $siteName -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
        }
        $cands += ($all | Where-Object { $_.HostName -like 'USPRDC036*' -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
        $cands += ($all | Where-Object { $_.HostName -like 'USPRDC*'   -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
      } elseif ($Region -eq 'UK') {
        $cands += ($all | Where-Object { $_.HostName -like 'UKPRDC*' -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
      } elseif ($Region -eq 'IE') {
        $cands += ($all | Where-Object { $_.HostName -like 'IEPRDC*' -and -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
      }
      $cands += ($all | Where-Object { -not $_.IsReadOnly } | Select-Object -ExpandProperty HostName)
    } catch {
      Write-Warning ("Get-AdDcCandidates fallback only. Error: {0}" -f $_.Exception.Message)
    }
    $seen=@{}; $out=@(); foreach($x in $cands){ if(-not $seen.ContainsKey($x)){ $seen[$x]=$true; $out+=$x } }
    return $out
  }
  function Get-BaseDNFromTargetOU { param([Parameter(Mandatory)][string]$TargetOU)
    $dcParts = ($TargetOU -split ',') | Where-Object { $_ -match '^\s*DC=' }
    if (-not $dcParts) { throw "TargetOU does not appear to be a distinguished name (missing DC=... parts)." }
    ($dcParts -join ',').Trim()
  }

  # Self-contained AD move (no pre/post replication)
  function Invoke-AdMove {
    param(
      [Parameter(Mandatory)]$Config,
      [Parameter(Mandatory)]$CredPathRoot,
      [Parameter(Mandatory)]$SafeVcsaName,
      [Parameter(Mandatory)]$VcCred,
      [Parameter(Mandatory)]$Vm,
      [Parameter(Mandatory)]$RawConfig
    )
    # Returns: [pscustomobject]@{ Result='SUCCESS|FAILED|NOTFOUND|SKIPPED'; Reason=<string or $null> }

    Write-Stamp "AD BLOCK: starting (final step)"

    if (-not $Config.ADTargetOU) {
      $reason = 'No ADTargetOU in config'
      Write-Warning $reason
      return [pscustomobject]@{ Result='SKIPPED'; Reason=$reason }
    }
    Write-Stamp ("AD BLOCK: TargetOU = {0}" -f $Config.ADTargetOU)

    # Import AD module
    try {
      Import-Module ActiveDirectory -ErrorAction Stop
      Write-Stamp "AD BLOCK: ActiveDirectory module imported"
    } catch {
      $reason = "ActiveDirectory module not available: $($_.Exception.Message)"
      Write-Warning $reason
      return [pscustomobject]@{ Result='SKIPPED'; Reason=$reason }
    }

    # Infer region/site for DC ordering
    $usSite = if     ($Config.ADTargetOU -match 'OU=Hawthorne') { 'HAW' } elseif ($Config.ADTargetOU -match 'OU=Marlborough') { 'MAR' } else { $null }
    $region = if ($Config.ADTargetOU -match 'OU=Ireland') { 'IE' } elseif ($Config.ADTargetOU -match 'OU=London') { 'UK' } elseif ($usSite) { 'US' } else { 'OTHER' }
    $adServer = $Config.ADServer

    # Resolve credentials
    $DefaultAdCredFile = Join-Path $CredPathRoot ("{0}-cred.xml" -f $SafeVcsaName)
    try {
      $adCred = Get-AdCredential -UseVCenterCreds:$Config.ADUseVCenterCreds -VcCred:$VcCred -CredFile:$Config.ADCredFile -DefaultCredFilePath:$DefaultAdCredFile -PromptIfMissing
      Write-Stamp ("AD BLOCK: Using credential identity = {0}" -f ($adCred ? $adCred.UserName : '<null>'))
    } catch {
      $reason = "Could not resolve AD credentials: $($_.Exception.Message)"
      Write-Warning $reason
      return [pscustomobject]@{ Result='SKIPPED'; Reason=$reason }
    }

    # Search base: prefer Unmanaged OU if provided, else base DN from TargetOU
    $searchBase = if ($RawConfig.AD -and $RawConfig.AD.UnmanagedOU) { $RawConfig.AD.UnmanagedOU }
                  elseif ($Config.ADUnmanagedOU) { $Config.ADUnmanagedOU }
                  else { Get-BaseDNFromTargetOU -TargetOU $Config.ADTargetOU }

    Write-Stamp ("AD BLOCK: SearchBase = {0}" -f $searchBase)
    Write-Stamp ("AD BLOCK: Preferred DC = {0}" -f ($adServer ?? '<none>'))

    # DC candidates (local sites first)
    $dcList = Get-AdDcCandidates -Region $region -UsSite $usSite -Preferred $adServer
    if (-not $dcList -or $dcList.Count -eq 0) { if ($adServer) { $dcList = @($adServer) } else { $dcList = @() } }
    $dcList = @($dcList | Select-Object -Unique)
    Write-Stamp ("AD BLOCK: DC candidates = {0}" -f ($(if($dcList){$dcList -join ', '}else{'<empty>'})))

    # Validate target OU on preferred DC (best-effort; non-fatal)
    try {
      $p=@{ Identity=$Config.ADTargetOU; ErrorAction='Stop' }
      if($adServer){$p.Server=$adServer}; if($adCred){$p.Credential=$adCred}
      Get-ADOrganizationalUnit @p | Out-Null
      Write-Stamp "AD BLOCK: Target OU validated on preferred DC (or default)"
    } catch {
      Write-Warning ("AD BLOCK: Target OU validation on preferred DC failed: {0}" -f $_.Exception.Message)
    }

    # Timeouts
    $timeoutTotal = [int]($(if ($Config.ADWaitForSeconds) { $Config.ADWaitForSeconds } else { 180 }))
    $timeoutPerDc = [int]([Math]::Max(30, [Math]::Floor($timeoutTotal / [Math]::Max(1,$dcList.Count))))
    Write-Stamp ("AD BLOCK: Timeout total = {0}s (~{1}s per DC)" -f $timeoutTotal,$timeoutPerDc)

    # Search by VMName across DCs
    $adComp = $null
    foreach($dc in $dcList){
      Write-Stamp ("AD BLOCK: Searching '{0}$' on DC {1} (base '{2}')" -f $Config.VMName,$dc,$searchBase)
      try {
        $adComp = Wait-ForADComputer -Name $Config.VMName -BaseDN $searchBase -TimeoutSec $timeoutPerDc -Credential $adCred -Server $dc
      } catch {
        Write-Warning ("AD BLOCK: Search error on {0}: {1}" -f $dc,$_.Exception.Message)
      }
      if ($adComp) { Write-Stamp ("AD BLOCK: Found on {0}" -f $dc); break }
    }

    # Retry with guest hostname if not found
    if (-not $adComp) {
      try {
        $guest = Get-VMGuest -VM $Vm -ErrorAction SilentlyContinue
        $guestName = if ($guest -and $guest.HostName) { $guest.HostName.Split('.')[0] } else { $null }
        if ($guestName -and $guestName -ne $Config.VMName) {
          Write-Stamp ("AD BLOCK: Retry with guest hostname '{0}$'" -f $guestName)
          foreach($dc in $dcList){
            try {
              $adComp = Wait-ForADComputer -Name $guestName -BaseDN $searchBase -TimeoutSec ([Math]::Min(60,$timeoutPerDc)) -Credential $adCred -Server $dc
            } catch {
              Write-Warning ("AD BLOCK: Retry search error on {0}: {1}" -f $dc,$_.Exception.Message)
            }
            if ($adComp) { Write-Stamp ("AD BLOCK: Found (guest name) on {0}" -f $dc); break }
          }
        } else {
          Write-Stamp "AD BLOCK: No distinct guest hostname available for retry."
        }
      } catch {
        Write-Warning ("AD BLOCK: Guest lookup failed: {0}" -f $_.Exception.Message)
      }
    }

    if (-not $adComp) {
      $reason = "Computer not found within $timeoutTotal s under '$searchBase' across DCs: $($dcList -join ', ')"
      Write-Warning $reason
      return [pscustomobject]@{ Result='NOTFOUND'; Reason=$reason }
    }

    Write-Stamp ("AD BLOCK: Found DN = {0}" -f $adComp.DistinguishedName)

    # --- Perform move on preferred DC if set, else first DC
    $moveServer = if ($adServer) { $adServer } else { $dcList | Select-Object -First 1 }
    try {
      Invoke-Step -Name ("Move-ADObject to target OU on {0}" -f ($moveServer ?? '<auto>')) -Script {
        $p=@{ Identity=$adComp.DistinguishedName; TargetPath=$Config.ADTargetOU; ErrorAction='Stop' }
        if($adCred){$p.Credential=$adCred}; if($moveServer){$p.Server=$moveServer}
        Move-ADObject @p
      }
      Write-Stamp ("AD BLOCK: MOVE SUCCESS → '{0}' to '{1}'" -f $adComp.Name, $Config.ADTargetOU)
      return [pscustomobject]@{ Result='SUCCESS'; Reason=$null }
    } catch {
      $reason = "Move-ADObject error: $($_.Exception.Message)"
      Write-Warning $reason
      if ($_.Exception){ Write-Host ("  • EXCEPTION: {0}" -f $_.Exception.ToString()) -ForegroundColor DarkYellow }
      return [pscustomobject]@{ Result='FAILED'; Reason=$reason }
    }
  }

  $oldEap = $ErrorActionPreference
  $ErrorActionPreference = 'Stop'
  try {
    # --- Load & normalize config
    $rawConfig = Read-Config -Path $ConfigPath
    $config    = Convert-StructuredConfig -In $rawConfig
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
    $CredFile     = Join-Path $CredPathRoot ("{0}-cred.xml" -f $SafeVcsaName)
    $cred=$null
    if (Test-Path $CredFile) {
      try { $cred=Import-Clixml -Path $CredFile; if (-not ($cred -and $cred.UserName)) { $cred=$null } else { [void]$cred.GetNetworkCredential().Password } } catch { $cred=$null }
    }
    if (-not $cred) { Write-Host "Enter credentials for $($config.VCSA) (e.g. administrator@vsphere.local)"; $cred=Get-Credential; try { $cred | Export-Clixml -Path $CredFile -Force | Out-Null } catch {} }

    try { Set-PowerCLIConfiguration -InvalidCertificateAction Ignore -Confirm:$false | Out-Null } catch {}
    $null = Invoke-Step -Name ("Connect-VIServer {0}" -f $config.VCSA) -Script { Connect-VIServer -Server $config.VCSA -Credential $cred -ErrorAction Stop }

    # --- Placement
    $clusterObj     = Get-Cluster -Name $config.Cluster -ErrorAction Stop
    $dc             = Get-DatacenterForCluster -Cluster $clusterObj
    $tplFolderName  = if ($config.TemplatesFolderName) { $config.TemplatesFolderName } else { 'Templates' }
    $templatesFolder= Ensure-TemplatesFolderStrict -Datacenter $dc -FolderName $tplFolderName

    try { Get-Command Get-VDPortgroup -ErrorAction Stop | Out-Null } catch { Import-Module VMware.VimAutomation.Vds -ErrorAction SilentlyContinue | Out-Null }

    $hosts = Get-VMHost -Location $clusterObj | Sort-Object Name
    $vmHost = if ($config.VMHost) { $h=$hosts | Where-Object { $_.Name -eq $config.VMHost }; if (-not $h) { throw "VMHost '$($config.VMHost)' not found in cluster '$($clusterObj.Name)'." } $h } else { $hosts | Get-Random }

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

    # --- Network lookup
    $pgStd = Get-VirtualPortGroup -Standard -ErrorAction SilentlyContinue
    $pgDvs = Get-VDPortgroup -ErrorAction SilentlyContinue
    $allPG=@(); if($pgStd){$allPG+=$pgStd}; if($pgDvs){$allPG+=$pgDvs}
    $networkObj = $allPG | Where-Object { $_.Name -eq $config.Network }
    if (-not $networkObj) { throw "Portgroup '$($config.Network)' not found in vCenter '$($config.VCSA)'." }
    $usePortGroup=$false
    try { $usePortGroup = ($networkObj -is [VMware.VimAutomation.Vds.Types.V1.VDPortgroup]) -or ($networkObj.GetType().Name -eq 'VDPortgroup') } catch { $usePortGroup=$false }

    $vmFolder = Resolve-RootFolder -Datacenter $dc -Name $config.Folder

    # --- Content Library → local template
    $clItem = Get-ContentLibraryItem -ContentLibrary $config.ContentLibraryName -Name $config.TemplateName -ErrorAction SilentlyContinue
    if (-not $clItem) { throw "Content Library item '$($config.TemplateName)' not found in '$($config.ContentLibraryName)'." }
    $localTemplateName = if ($config.LocalTemplateName) { $config.LocalTemplateName } else { $config.TemplateName }

    if ($config.ForceReplace) {
      if ($t=Get-Template -Name $localTemplateName -ErrorAction SilentlyContinue){ Write-Host "Template '$localTemplateName' exists. Removing (force)..."; Remove-Template -Template $t -DeletePermanently -Confirm:$false -ErrorAction Stop }
      if ($v=Get-VM -Name $localTemplateName -ErrorAction SilentlyContinue){ Write-Host "VM '$localTemplateName' exists. Removing (force)..."; Remove-VM -VM $v -DeletePermanently -Confirm:$false -ErrorAction Stop }
    }

    Write-Host ("`nRefreshing local template from CL item '{0}' → '{1}'..." -f $clItem.Name,$localTemplateName)
    $seedParams=@{ Name=$localTemplateName; ContentLibraryItem=$clItem; VMHost=$vmHost; Datastore=$datastoreObj; Location=$templatesFolder; ErrorAction='Stop' }
    $ovfCfg=$null
    try { $ovfCfg = Get-OvfConfiguration -ContentLibraryItem $clItem -Target $clusterObj -ErrorAction Stop; Write-Host 'Detected OVF/OVA; mapping networks via OVF configuration...' }
    catch { Write-Host 'Detected VM template CL item; setting network parameter...' }
    if ($ovfCfg){
      if ($ovfCfg.NetworkMapping){ foreach($k in $ovfCfg.NetworkMapping.Keys){ $ovfCfg.NetworkMapping[$k].Value=$networkObj } }
      $seedParams['OvfConfiguration']=$ovfCfg
    } else {
      if ($usePortGroup){ $seedParams['PortGroup']=$networkObj } else { $seedParams['NetworkName']=$networkObj.Name }
    }
    $seedVM = New-VM_FilteringOvfWarning -Params $seedParams
    if ($config.RemoveExtraCDROMs){ Ensure-SingleCDHard -VM $seedVM }
    if ($config.EnsureNICConnected){ Set-VMNicStartConnected -VM $seedVM }
    if ($seedVM.PowerState -ne 'PoweredOff'){ Stop-VM -VM $seedVM -Confirm:$false | Out-Null }

    Write-Host ("Converting '{0}' to a local vSphere Template in '{1}'..." -f $localTemplateName,$tplFolderName)
    $localTemplate = Set-VM -VM $seedVM -ToTemplate -Name $localTemplateName -Confirm:$false -ErrorAction Stop
    Write-Host ("✅ Local template ready: '{0}'." -f $localTemplate.Name)

    # --- Deploy VM (powered off)
    $vmName=$config.VMName
    if (-not $vmName){ throw 'VMName is required in config for the VM deployment phase.' }
    if (Get-VM -Name $vmName -ErrorAction SilentlyContinue){ throw "A VM named '$vmName' already exists." }

    $oscSpec=$null
    if ($config.CustomizationSpec){ $oscSpec=Get-OSCustomizationSpec -Name $config.CustomizationSpec -ErrorAction SilentlyContinue; if (-not $oscSpec){ Write-Warning "OS Customization Spec '$($config.CustomizationSpec)' not found; continuing without it." } }

    Write-Host ("`nDeploying VM '{0}' from local template '{1}'..." -f $vmName,$localTemplate.Name)
    $createParams=@{ Name=$vmName; Template=$localTemplate; VMHost=$vmHost; Datastore=$datastoreObj; Location=$vmFolder; ErrorAction='Stop' }
    if ($oscSpec){ $createParams['OSCustomizationSpec']=$oscSpec }
    $newVM = New-VM @createParams

    # NIC mapping
    try{
      $firstNic=Get-NetworkAdapter -VM $newVM | Select-Object -First 1
      if($firstNic){
        if($usePortGroup){ Set-NetworkAdapter -NetworkAdapter $firstNic -PortGroup $networkObj -Confirm:$false | Out-Null }
        else { Set-NetworkAdapter -NetworkAdapter $firstNic -NetworkName $config.Network -Confirm:$false | Out-Null }
        Write-Stamp ("Ensured NIC 0 on '{0}'" -f $networkObj.Name)
      }
    } catch {
      Write-Warning "Post-clone network set failed: $($_.Exception.Message)"
      if ($_.Exception){ Write-Host ("  • EXCEPTION: {0}" -f $_.Exception.ToString()) -ForegroundColor DarkYellow }
    }

    if ($config.RemoveExtraCDROMs){ Ensure-SingleCDHard -VM $newVM }
    if ($config.EnsureNICConnected){ Set-VMNicStartConnected -VM $newVM }

    # CPU/Mem sizing and system disk grow while powered off
    if ($config.CPU -or $config.MemoryGB -or $config.CoresPerSocket){
      $setParams=@{ VM=$newVM; Confirm=$false; ErrorAction='SilentlyContinue' }
      if($config.CPU){$setParams['NumCPU']=[int]$config.CPU}
      if($config.MemoryGB){$setParams['MemoryGB']=[int]$config.MemoryGB}
      if($config.CoresPerSocket){$setParams['CoresPerSocket']=[int]$config.CoresPerSocket}
      Set-VM @setParams | Out-Null
    }
    if ($config.DiskGB){
      $sysDisk=Get-HardDisk -VM $newVM | Select-Object -First 1
      if($sysDisk -and $config.DiskGB -gt $sysDisk.CapacityGB){
        Set-HardDisk -HardDisk $sysDisk -CapacityGB $config.DiskGB -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
      }
    }

    # ---- ADDITIONAL DISKS: round-robin on PVSCSI 1..3 ONLY -----------------
    $additionalSizes = @()
    if ($rawConfig.VMware -and $rawConfig.VMware.Hardware -and $rawConfig.VMware.Hardware.AdditionalDisks) {
      $additionalSizes = @($rawConfig.VMware.Hardware.AdditionalDisks | ForEach-Object { [int]$_ })
    } elseif ($config.AdditionalDisks) {
      $additionalSizes = @($config.AdditionalDisks | ForEach-Object { [int]$_ })
    } elseif ($config.AdditionalDiskGB -and [int]$config.AdditionalDiskGB -gt 0) {
      $additionalSizes = @([int]$config.AdditionalDiskGB)
    }

    $buses = $null
    try {
      if     ($rawConfig.PvScsiBuses)  { $buses = @($rawConfig.PvScsiBuses | ForEach-Object { [int]$_ }) }
      elseif ($config.PvScsiBuses)     { $buses = @($config.PvScsiBuses  | ForEach-Object { [int]$_ }) }
    } catch {}
    if (-not $buses) {
      $n = [Math]::Max(0, $additionalSizes.Count)
      if ($n -gt 0) { $buses = 1..([Math]::Min(3,$n)) } else { $buses = @() }  # 1 → [1], 2 → [1,2], 3+ → [1,2,3]
    }
    $buses = @($buses | Where-Object { $_ -ge 1 -and $_ -le 3 })
    Write-Stamp ("PVSCSI round-robin buses to use: " + ($buses -join ', '))

    $ctrls = Get-ScsiSnapshot -VM $newVM
    Write-Stamp ("Found SCSI controllers: " + (( $ctrls | ForEach-Object { "Bus $($_.Bus)=$($_.Type)" } ) -join ', '))

    foreach($bus in $buses) {
      if (-not ($ctrls | Where-Object { $_.Bus -eq $bus -and $_.Type -eq 'ParaVirtualSCSIController' })) {
        Write-Stamp ("Adding PVSCSI controller on bus {0}…" -f $bus)
        Add-PvScsiControllerApi -VM $newVM -BusNumber $bus | Out-Null
        Start-Sleep -Milliseconds 700
        $ctrls = Get-ScsiSnapshot -VM $newVM
      }
    }

    if ($additionalSizes.Count -gt 0) {
      for ($i=0; $i -lt $additionalSizes.Count; $i++) {
        $size = [int]$additionalSizes[$i]
        $bus  = $buses[ $i % $buses.Count ]   # 1→2→3→1…
        $hd   = New-HardDisk -VM $newVM -CapacityGB $size -Datastore $datastoreObj -Confirm:$false -ErrorAction Stop
        try { Move-HardDiskToBusApi -VM $newVM -HardDisk $hd -TargetBus $bus }
        catch {
          Write-Host ("WARN: Move disk to SCSI{0} failed: {1}" -f $bus,$_.Exception.Message) -ForegroundColor Yellow
          if ($_.Exception){ Write-Host ("  • EXCEPTION: {0}" -f $_.Exception.ToString()) -ForegroundColor DarkYellow }
        }
        Write-Stamp ("Added data disk #{0} ({1} GB) on SCSI{2}" -f ($i+1), $size, $bus)
        Start-Sleep -Milliseconds 400
      }
    } else {
      Write-Stamp "No additional disk sizes provided."
    }

    # Notes (minimal: CR + timestamp only)
    if ($config.ChangeRequestNumber){
      $note = "CR: $($config.ChangeRequestNumber) — deployed $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
      try {
        Set-VM -VM $newVM -Notes $note -Confirm:$false | Out-Null
        Write-Stamp "Notes set: $note"
      } catch {
        Write-Warning "Failed to set Notes: $($_.Exception.Message)"
      }
    }


    # Tools policy (guard for older PowerCLI)
    try{
      $cmd = Get-Command Set-VM -ErrorAction SilentlyContinue
      if ($cmd -and $cmd.Parameters.ContainsKey('ToolsUpgradePolicy')) {
        Set-VM -VM $newVM -ToolsUpgradePolicy UpgradeAtPowerCycle -Confirm:$false | Out-Null
        $view=Get-View -Id $newVM.Id
        Write-Stamp ("Tools policy now: {0}" -f $view.Config.Tools.ToolsUpgradePolicy)
      } else { Write-Stamp "Skipping ToolsUpgradePolicy (parameter not supported in this PowerCLI)." }
    } catch {
      Write-Warning ("Set-VM ToolsUpgradePolicy failed: {0}" -f $_.Exception.Message)
      if ($_.Exception){ Write-Host ("  • EXCEPTION: {0}" -f $_.Exception.ToString()) -ForegroundColor DarkYellow }
    }

    # --- Power on -----------------------------------------------------------
    if ($config.PowerOn){
      Invoke-Step -Name "Power on VM" -Script { Start-VM -VM $newVM -Confirm:$false | Out-Null }
      if ($config.PostPowerOnDelay -gt 0) {
        Write-Stamp ("Sleeping {0}s post power-on…" -f $config.PostPowerOnDelay)
        Start-Sleep -Seconds ([int]$config.PostPowerOnDelay)
      }
    }

    # --- Tools wait / optional tools upgrade --------------------------------
    if ($config.PowerOn){
      $tw = if ($config.ToolsWaitSeconds) { [int]$config.ToolsWaitSeconds } else { 180 }
      $status = $null
      try { $status = Invoke-Step -Name ("Wait for VMware Tools healthy ({0}s)" -f $tw) -Script { Wait-ToolsHealthy -VM $newVM -TimeoutSec $tw } }
      catch {
        Write-Warning ("Wait-ToolsHealthy threw: {0}" -f $_.Exception.Message)
        if ($_.Exception){ Write-Host ("  • EXCEPTION: {0}" -f $_.Exception.ToString()) -ForegroundColor DarkYellow }
      }
      Write-Stamp ("Post-boot VMware Tools status: {0}" -f ($status ?? 'unknown'))

      try{
        $guest = Get-VMGuest -VM $newVM -ErrorAction SilentlyContinue
        $tools = if ($guest) { $guest.ToolsStatus } else { $null }
        if ($tools -in 'toolsOld','toolsSupportedOld','toolsOk') {
          try { Update-Tools -VM $newVM -NoReboot -ErrorAction SilentlyContinue | Out-Null; Write-Stamp 'Triggered VMware Tools upgrade.' } catch {}
          $status2 = Wait-ToolsHealthy -VM $newVM -TimeoutSec $tw
          Write-Stamp ("Post-upgrade VMware Tools status: {0}" -f ($status2 ?? 'unknown'))
        } else { Write-Stamp ("Skipping Update-Tools; Tools status is '{0}'" -f ($tools ?? 'unknown')) }
      } catch {
        Write-Warning ("Update-Tools check threw: {0}" -f $_.Exception.Message)
      }
    }

    # --- HW upgrade (best-effort) ------------------------------------------
    try {
      Upgrade-VMHardwareSafely -VM $newVM -RePowerOn:($config.PowerOn)
    } catch {
      Write-Warning ("Hardware upgrade step failed: {0}" -f $_.Exception.Message)
    }

    # ===================== AD MOVE (LAST STEP, NO REPLICATION) ===============
    $adOutcome = Invoke-AdMove -Config $config -CredPathRoot $CredPathRoot -SafeVcsaName $SafeVcsaName -VcCred $cred -Vm $newVM -RawConfig $rawConfig
    Write-Stamp ("Summary: AD move result = {0}{1}" -f $adOutcome.Result, ($(if ($adOutcome.Reason) { " — Reason: $($adOutcome.Reason)" } else { "" })))
    # =================== END AD MOVE =========================================

    # --- Final summary ------------------------------------------------------
    try{
      $v=Get-View -Id $newVM.Id; $v.UpdateViewData(); [void](Get-VM -Id $newVM.Id)
      $guest = Get-VMGuest -VM $newVM -ErrorAction SilentlyContinue
      $toolsState = if ($guest) { $guest.ToolsStatus } else { 'unknown' }
      $hwVer = (Get-VM -Id $newVM.Id).Version
      Write-Stamp ("Final status → Tools: {0}; HW: {1}; Power: {2}" -f $toolsState,$hwVer,(Get-VM -Id $newVM.Id).PowerState)
    } catch {
      Write-Warning "View/status refresh failed: $($_.Exception.Message)"
    }

    $powerMsg = if ($config.PowerOn){ 'powered on; ' } else { 'not powered on; ' }
    $busSummary = if     (-not $additionalSizes -or $additionalSizes.Count -eq 0) { '<none>' }
                  elseif ($additionalSizes.Count -eq 1) { '1' }
                  elseif ($additionalSizes.Count -eq 2) { '1,2' }
                  else { '1,2,3' }
    Write-Host ("✅ VM '{0}' deployed. Extra disks placed round-robin on PVSCSI bus(es): {1}; {2}AD move attempted if provided." -f $vmName, $busSummary, $powerMsg)

    try { Stop-Transcript | Out-Null } catch {}
  }
  finally { $ErrorActionPreference = $oldEap }
}


# --- Menu ------------------------------------------------------------------
$global:LastConfigPath = $null
function Show-Menu {
  Write-Host ""; Write-Host "==== VM Builder & Deployer ====" -ForegroundColor Cyan
  Write-Host "  [1]  Build new JSON (guided)"
  Write-Host "  [10] Deploy VM from JSON"
  Write-Host "  [99] Build then Deploy"
  Write-Host "  [0]  Exit"
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
