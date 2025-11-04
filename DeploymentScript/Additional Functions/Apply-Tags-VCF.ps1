<#
.SYNOPSIS
  Menu-driven tool with an "Apply Tags" option.
  Cohesity balancing + Zerto tagging via Region→vCenter map.
  Multi-vCenter SAFE (all inventory/tagging calls bound to the right VIServer).

  Now updated to prefer **VCF PowerCLI** (9.0+) while remaining backward-compatible
  with classic **VMware.PowerCLI**.

.NOTES
  - Requires VCF PowerCLI 9.0+ (module: VCF.PowerCLI), or VMware.PowerCLI 13.x.
  - Credential files must exist, named exactly:
      ukprim098.bfl.local-cred.xml
      usprim004.bfl.local-cred.xml
      ieprim018.bfl.local-cred.xml
#>

# =================== SETTINGS / DEFAULTS ===================

$RegionToVCSA = @{
  'UK' = 'ukprim098.bfl.local'
  'US' = 'usprim004.bfl.local'
  'IE' = 'ieprim018.bfl.local'
}

$CredRoot = "C:\Users\GrWay\OneDrive\OneDrive - Beazley Group\Documents\Scripts\DeploymentScript"

$DefaultTestVMs = @(
  'UKPRAP355','UKPRAP356','USPRAP180','USPRAP181','USPRAP182','USPRAP183','IEPRAP035','IEPRAP036'
)

$CohesityCategoryName = "Cohesity-VM-Group"
$CohesityTags         = @("VM-Group-1","VM-Group-2","VM-Group-3")

$ZertoCategoryName    = "ZertoAutoProtect"
$ZertoTag_DB4_to_LD9  = "DB4toLD9AutoProtectVPGDev:AutoProtect" # Ireland -> London
$ZertoTag_LD9_to_DB4  = "LD9toDB4AutoProtectVPGDev:AutoProtect" # London -> Ireland

# =================== MODULE LOADING (VCF PowerCLI preferred) ===================

function Import-VCFPowerCLI {
  # Prefer the new VCF.PowerCLI meta-module (PowerCLI renamed under VCF umbrella).
  $vcf = Get-Module -ListAvailable -Name 'VCF.PowerCLI'
  $vmw = Get-Module -ListAvailable -Name 'VMware.PowerCLI'

  if ($vcf) {
    Write-Host "Loading VCF.PowerCLI..." -ForegroundColor Cyan
    Import-Module 'VCF.PowerCLI' -ErrorAction Stop
  } elseif ($vmw) {
    Write-Host "VCF.PowerCLI not found; loading VMware.PowerCLI for backward compatibility..." -ForegroundColor Yellow
    Import-Module 'VMware.PowerCLI' -ErrorAction Stop
  } else {
    throw @"
Neither VCF.PowerCLI nor VMware.PowerCLI is installed.
Install one of the following (CurrentUser scope is fine):

  Install-Module VCF.PowerCLI -Scope CurrentUser
  # or
  Install-Module VMware.PowerCLI -Scope CurrentUser
"@
  }

  # Make sure Multiple server mode & relaxed certs are set (no prompts).
  $global:DefaultVIServerMode = 'Multiple'
  try {
    Set-PowerCLIConfiguration -DefaultVIServerMode Multiple -InvalidCertificateAction Ignore -ParticipateInCEIP:$false -Scope User -Confirm:$false | Out-Null
  } catch {}
}

# =================== CORE TAGGING FUNCTION ===================

function Add-VMTags {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][string[]]$VmNames,
    [Parameter(Mandatory)][hashtable]$RegionToVCSA,
    [Parameter(Mandatory)][string]$CredRoot,
    [string]$CohesityCategoryName = "Cohesity-VM-Group",
    [string[]]$CohesityTags = @("VM-Group-1","VM-Group-2","VM-Group-3"),
    [string]$ZertoCategoryName = "ZertoAutoProtect",
    [string]$ZertoTag_DB4_to_LD9 = "DB4toLD9AutoProtectVPGDev:AutoProtect",
    [string]$ZertoTag_LD9_to_DB4 = "LD9toDB4AutoProtectVPGDev:AutoProtect",
    [bool]$RebalanceCohesity = $true
  )

  function Get-RegionFromVMName ([string]$VmName) { if ($VmName.Length -ge 2) { $VmName.Substring(0,2).ToUpper() } else { $null } }
  function Get-CredFileForVCSA ([string]$VcsaFqdn) {
    $expected = Join-Path $CredRoot "$VcsaFqdn-cred.xml"
    if (Test-Path $expected) { return $expected }
    throw "Cred file not found: $expected"
  }
  function Ensure-TagCategory ([string]$Name,[string]$Cardinality='Single',[string[]]$EntityType=@('VirtualMachine'), [Parameter(Mandatory)][object]$Server) {
    $existing = Get-TagCategory -Server $Server -Name $Name -ErrorAction SilentlyContinue
    if (-not $existing) {
      Write-Host ("Creating tag category '{0}' (Cardinality={1}) on {2}..." -f $Name,$Cardinality,$Server) -ForegroundColor Cyan
      New-TagCategory -Server $Server -Name $Name -Cardinality $Cardinality -EntityType $EntityType | Out-Null
    }
  }
  function Ensure-Tags ([string]$Category,[string[]]$TagNames,[Parameter(Mandatory)][object]$Server) {
    foreach ($t in $TagNames) {
      if (-not (Get-Tag -Server $Server -Category $Category -Name $t -ErrorAction SilentlyContinue)) {
        Write-Host ("Creating tag '{0}' in category '{1}' on {2}..." -f $t,$Category,$Server) -ForegroundColor Cyan
        New-Tag -Server $Server -Name $t -Category $Category | Out-Null
      }
    }
  }
  function Get-TagCounts ([string]$Category,[string[]]$TagNames,[Parameter(Mandatory)][object]$Server) {
    $h=@{}; foreach ($t in $TagNames) {
      $tagObj = Get-Tag -Server $Server -Category $Category -Name $t -ErrorAction SilentlyContinue
      $h[$t] = if ($tagObj) { (Get-VM -Server $Server -Tag $tagObj -ErrorAction SilentlyContinue | Measure-Object).Count } else { 0 }
    }; return $h
  }
  function Choose-LeastLoadedTag ($Counts,[string[]]$TagOrder) {
    ($TagOrder | Sort-Object { $Counts[$_] }, { [array]::IndexOf($TagOrder, $_) })[0]
  }

  function Assign-Tag-Safely {
    param(
      [Parameter(Mandatory)][object]$VM,
      [Parameter(Mandatory)][string]$Category,
      [Parameter(Mandatory)][string]$TagName,
      [Parameter(Mandatory)][object]$Server,
      [bool]$Rebalance = $true
    )
    # Detect any existing assignment in this category using Category.Name for reliability
    $existingAny = Get-TagAssignment -Server $Server -Entity $VM -ErrorAction SilentlyContinue |
                   Where-Object { $_.Tag.Category.Name -eq $Category }

    if ($existingAny) {
      if ($existingAny.Tag.Name -eq $TagName) {
        Write-Host ("  - {0}: already has '{1}' [{2}] — no change" -f $VM.Name,$TagName,$Category) -ForegroundColor DarkGray
        return [pscustomobject]@{Succeeded=$false; Action='NoChange'; OldTag=$existingAny.Tag.Name }
      } elseif ($Rebalance) {
        Write-Host ("  * {0}: replacing '{1}' with '{2}' in '{3}'" -f $VM.Name,$existingAny.Tag.Name,$TagName,$Category) -ForegroundColor DarkYellow
        try {
          # Remove via the TagAssignment object (Remove-TagAssignment has no -Server parameter)
          $existingAny | ForEach-Object { Remove-TagAssignment -TagAssignment $_ -Confirm:$false -ErrorAction Stop }
        } catch {
          Write-Host ("    Could not remove existing tag(s) in '{0}' on {1}: {2} — leaving as-is" -f $Category,$VM.Name,$_.Exception.Message) -ForegroundColor DarkGray
          return [pscustomobject]@{Succeeded=$false; Action='RemoveFailed'; OldTag=$existingAny.Tag.Name }
        }
      } else {
        Write-Host ("  - {0}: has '{1}' in '{2}' — leaving as-is" -f $VM.Name,$existingAny.Tag.Name,$Category) -ForegroundColor DarkGray
        return [pscustomobject]@{Succeeded=$false; Action='SkipExisting'; OldTag=$existingAny.Tag.Name }
      }
    }

    try {
      $tagObj = Get-Tag -Server $Server -Category $Category -Name $TagName -ErrorAction Stop
      New-TagAssignment -Server $Server -Entity $VM -Tag $tagObj -ErrorAction Stop | Out-Null
      Write-Host ("  + {0}: assigned '{1}' [{2}]" -f $VM.Name,$TagName,$Category) -ForegroundColor Green
      $action = if ($existingAny) { 'Replaced' } else { 'Assigned' }
      return [pscustomobject]@{Succeeded=$true; Action=$action; OldTag=$existingAny?.Tag?.Name }
    } catch {
      $msg = $_.Exception.Message
      if ($msg -match '(?i)cardinality' -or $msg -match '(?i)ineligible to attach') {
        $postExisting = Get-TagAssignment -Server $Server -Entity $VM -ErrorAction SilentlyContinue |
                        Where-Object { $_.Tag.Category.Name -eq $Category }
        if ($postExisting) {
          Write-Host ("  - {0}: '{1}' in '{2}' already present — no change" -f $VM.Name,$postExisting.Tag.Name,$Category) -ForegroundColor DarkGray
          return [pscustomobject]@{Succeeded=$false; Action='NoChange'; OldTag=$postExisting.Tag.Name }
        }
      }
      if ($msg -match '(?i)must be managed by the same VC Server') {
        Write-Host ("  - {0}: tag '{1}' belongs to another vCenter — ensure tags exist on {2}" -f $VM.Name,$TagName,$Server) -ForegroundColor DarkGray
        return [pscustomobject]@{Succeeded=$false; Action='CrossVC'; OldTag=$null }
      }
      Write-Host ("  - {0}: could not assign '{1}' in '{2}' ({3})" -f $VM.Name,$TagName,$Category,$msg) -ForegroundColor DarkGray
      return [pscustomobject]@{Succeeded=$false; Action='Failed'; OldTag=$null }
    }
  }

  function Get-VM-SiteForZerto {
    param(
      [Parameter(Mandatory)][object]$VM,
      [Parameter(Mandatory)][string]$Region,
      [Parameter(Mandatory)][object]$Server
    )
    $cluster = Get-Cluster -Server $Server -VM $VM -ErrorAction SilentlyContinue
    $cn = $cluster?.Name
    if ($cn) {
      if ($cn -match '(?i)\bLD9\b|London|LD-?9|LDN9') { return 'LD9' }
      if ($cn -match '(?i)\bDB4\b|Dublin|DB-?4')      { return 'DB4' }
    }
    switch -Regex ($Region.ToUpper()) {
      '^UK$' { return 'LD9' }
      '^IE$' { return 'DB4' }
      '^US$' { return 'US' }
      default { return $null }
    }
  }

  function Ensure-Connected ([string]$Vcsa) {
    $credFile = Get-CredFileForVCSA $Vcsa
    $cred     = Import-Clixml -Path $credFile
    $conn = Connect-VIServer -Server $Vcsa -Credential $cred -ErrorAction Stop
    return $conn
  }

  # Prefer VCF.PowerCLI, fallback to VMware.PowerCLI
  Import-VCFPowerCLI

  # Be explicit about default param to suppress confirms globally in this function scope
  $PSDefaultParameterValues['*:Confirm'] = $false

  # Tidy up any prior sessions
  try { Disconnect-VIServer -Server * -Force -Confirm:$false -ErrorAction SilentlyContinue | Out-Null } catch {}

  $PerVcsaBootstrapped = @{}
  $ServerByVcsa = @{}
  $summary = [ordered]@{
    Cohesity_Assigned = 0; Cohesity_Skipped = 0;
    Zerto_Assigned = 0;    Zerto_Skipped = 0;
    NotFound = 0;          NoVCSAForRegion = 0
  }

  foreach ($name in $VmNames) {
    $region = Get-RegionFromVMName $name
    if (-not $RegionToVCSA.ContainsKey($region)) {
      Write-Host ("No VCSA mapping for region '{0}' (VM='{1}') — skipped." -f $region,$name) -ForegroundColor DarkGray
      $summary.NoVCSAForRegion++; continue
    }
    $vcsa = $RegionToVCSA[$region]

    if (-not $ServerByVcsa.ContainsKey($vcsa)) { $ServerByVcsa[$vcsa] = Ensure-Connected $vcsa }
    $server = $ServerByVcsa[$vcsa]

    if (-not $PerVcsaBootstrapped.ContainsKey($vcsa)) {
      Ensure-TagCategory $CohesityCategoryName 'Single' @('VirtualMachine') -Server $server
      Ensure-Tags $CohesityCategoryName $CohesityTags -Server $server
      Ensure-TagCategory $ZertoCategoryName 'Single' @('VirtualMachine') -Server $server
      Ensure-Tags $ZertoCategoryName @($ZertoTag_DB4_to_LD9,$ZertoTag_LD9_to_DB4) -Server $server
      $PerVcsaBootstrapped[$vcsa] = $true
    }

    $vm = Get-VM -Server $server -Name $name -ErrorAction SilentlyContinue
    if (-not $vm) {
      Write-Host ("VM not found in {0}: {1}" -f $vcsa,$name) -ForegroundColor DarkGray
      $summary.NotFound++; continue
    }

    Write-Host ("=== Processing {0} (vCenter={1}) ===" -f $vm.Name,$vcsa) -ForegroundColor White

    # ----- Cohesity (exclude names containing 'DV') -----
    if ($vm.Name -match 'DV') {
      Write-Host "  Cohesity: excluded (name contains 'DV')." -ForegroundColor DarkGray
      $summary.Cohesity_Skipped++
    } else {
      # fetch per-VC counts once then maintain during loop for balancing
      if (-not $script:CohesityCounts) { $script:CohesityCounts = @{} }
      if (-not $script:CohesityCounts.ContainsKey($vcsa)) {
        $script:CohesityCounts[$vcsa] = Get-TagCounts $CohesityCategoryName $CohesityTags -Server $server
      }
      $counts = $script:CohesityCounts[$vcsa]
      $chosen = Choose-LeastLoadedTag $counts $CohesityTags

      $status = Assign-Tag-Safely -VM $vm -Category $CohesityCategoryName -TagName $chosen -Server $server -Rebalance:$RebalanceCohesity

      if ($status.Succeeded) {
        # Update in-run counts
        if ($status.Action -eq 'Replaced' -and $status.OldTag) {
          if ($counts.ContainsKey($status.OldTag)) { $counts[$status.OldTag] = [math]::Max(0, $counts[$status.OldTag] - 1) }
        }
        if ($counts.ContainsKey($chosen)) { $counts[$chosen]++ }
        $script:CohesityCounts[$vcsa] = $counts
        $summary.Cohesity_Assigned++
      } else {
        $summary.Cohesity_Skipped++
      }
    }

    # ----- Zerto (any VM; skip US; infer LD9/DB4) -----
    $site = Get-VM-SiteForZerto -VM $vm -Region $region -Server $server
    switch ($site) {
      'LD9' {
        $status = Assign-Tag-Safely -VM $vm -Category $ZertoCategoryName -TagName $ZertoTag_LD9_to_DB4 -Server $server -Rebalance:$true
        if ($status.Succeeded) {
          Write-Host "  Zerto: assigned LD9→DB4" -ForegroundColor Green
          $summary.Zerto_Assigned++
        } else { $summary.Zerto_Skipped++ }
      }
      'DB4' {
        $status = Assign-Tag-Safely -VM $vm -Category $ZertoCategoryName -TagName $ZertoTag_DB4_to_LD9 -Server $server -Rebalance:$true
        if ($status.Succeeded) {
          Write-Host "  Zerto: assigned DB4→LD9" -ForegroundColor Green
          $summary.Zerto_Assigned++
        } else { $summary.Zerto_Skipped++ }
      }
      'US' {
        Write-Host "  Zerto: US site — skipping per requirement." -ForegroundColor DarkGray
        $summary.Zerto_Skipped++
      }
      default {
        Write-Host ("  Zerto: No site inferred from cluster or region for {0} — skipping." -f $vm.Name) -ForegroundColor DarkGray
        $summary.Zerto_Skipped++
      }
    }
  }

  [pscustomobject]$summary
}

# =================== MENU ===================

function Show-MainMenu {
  Write-Host ""
  Write-Host "========== Deployment Menu ==========" -ForegroundColor Cyan
  Write-Host "1) Apply Tags (Cohesity + Zerto) to a quick test list"
  Write-Host "2) Apply Tags to VM names you enter"
  Write-Host "3) Apply Tags from file (txt/csv: one name per line or comma/space-delimited)"
  Write-Host "X) Exit"
  Write-Host "====================================="
  Read-Host "Select an option"
}

function Read-VMs-From-User {
  $inp = Read-Host "Enter VM names (comma, space, or newline separated)"
  if (-not $inp) { return @() }
  return ($inp -split '[,\s]+' | ForEach-Object { $_.Trim() } | Where-Object { $_ } | Select-Object -Unique)
}

function Read-VMs-From-File {
  $path = Read-Host "Enter path to VM list file (txt/csv)"
  if (-not (Test-Path $path)) { Write-Host "File not found: $path" -ForegroundColor DarkGray; return @() }
  $raw = Get-Content -Path $path -ErrorAction SilentlyContinue
  if (-not $raw) { return @() }
  $names = @()
  foreach ($line in $raw) {
    $parts = $line -split '[,\s]+' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
    $names += $parts
  }
  return ($names | Select-Object -Unique)
}

$choice = Show-MainMenu
switch ($choice.ToUpper()) {
  '1' {
    Write-Host "`n[Apply Tags] Quick test list" -ForegroundColor Green
    $result = Add-VMTags -VmNames $DefaultTestVMs -RegionToVCSA $RegionToVCSA -CredRoot $CredRoot `
      -CohesityCategoryName $CohesityCategoryName -CohesityTags $CohesityTags `
      -ZertoCategoryName $ZertoCategoryName -ZertoTag_DB4_to_LD9 $ZertoTag_DB4_to_LD9 -ZertoTag_LD9_to_DB4 $ZertoTag_LD9_to_DB4 `
      -RebalanceCohesity:$true
    $result | Format-Table -AutoSize
  }
  '2' {
    $names = Read-VMs-From-User
    if ($names.Count -eq 0) { Write-Host "No VM names provided." -ForegroundColor DarkGray; break }
    Write-Host "`n[Apply Tags] Manual list: $($names -join ', ')" -ForegroundColor Green
    $result = Add-VMTags -VmNames $names -RegionToVCSA $RegionToVCSA -CredRoot $CredRoot `
      -CohesityCategoryName $CohesityCategoryName -CohesityTags $CohesityTags `
      -ZertoCategoryName $ZertoCategoryName -ZertoTag_DB4_to_LD9 $ZertoTag_DB4_to_LD9 -ZertoTag_LD9_to_DB4 $ZertoTag_LD9_to_DB4 `
      -RebalanceCohesity:$true
    $result | Format-Table -AutoSize
  }
  '3' {
    $names = Read-VMs-From-File
    if ($names.Count -eq 0) { Write-Host "No VM names loaded from file." -ForegroundColor DarkGray; break }
    Write-Host "`n[Apply Tags] File list: $($names -join ', ')" -ForegroundColor Green
    $result = Add-VMTags -VmNames $names -RegionToVCSA $RegionToVCSA -CredRoot $CredRoot `
      -CohesityCategoryName $CohesityCategoryName -CohesityTags $CohesityTags `
      -ZertoCategoryName $ZertoCategoryName -ZertoTag_DB4_to_LD9 $ZertoTag_DB4_to_LD9 -ZertoTag_LD9_to_DB4 $ZertoTag_LD9_to_DB4 `
      -RebalanceCohesity:$true
    $result | Format-Table -AutoSize
  }
  'X' { Write-Host "Bye!" -ForegroundColor Yellow; return }
  default { Write-Host "Invalid selection." -ForegroundColor DarkGray; return }
}
