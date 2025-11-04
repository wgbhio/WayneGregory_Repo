#Requires -Version 5.1
#Requires -Modules ActiveDirectory
<#
.SYNOPSIS
  Standalone tool to suggest the next available hostname (Region + Env + Role).

.DESCRIPTION
  Prompts for Region (UK/US/IE), Environment (PR/DV), and Role code from your taxonomy
  (AP, CA, CM, DB, DC, FP, IM, VH, WS, XA). Finds the lowest unused 3-digit number
  that passes these checks:
    - No AD Computer with that name
    - No "<Server>-LocalAdmins" group (in configured OU)
    - No DNS A/AAAA record
    - No ping response
  Prints & returns the chosen hostname and reminds you to update the Host Name Spreadsheet.
#>

#region configuration --------------------------------------------------------
$script:LocalAdminGroupsOU = 'OU=Local Admin Access,OU=Shared Groups,DC=bfl,DC=local'
$Global:RoleMap = [ordered]@{
  'AP' = 'Application'
  'CA' = 'Client Access'
  'CM' = 'Communications'
  'DB' = 'Database'
  'DC' = 'Domain Controller'
  'FP' = 'File/Print'
  'IM' = 'Infrastructure Management'
  'VH' = 'Virtual Host'
  'WS' = 'Web'
  'XA' = 'Citrix'
}
$Global:HostnameSpreadsheetLabel = 'Host Name Spreadsheet'
$Global:HostnameSpreadsheetUrl   = 'https://beazley.sharepoint.com/:x:/r/sites/Infrastructure474/_layouts/15/doc2.aspx?sourcedoc=%7B057AA142-61F6-45BA-839D-7DB1BE470A67%7D&file=Server%20Names.xlsx&action=default&mobileredirect=true&wdOrigin=TEAMS-MAGLEV.p2p_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1756201409527&web=1'
#endregion configuration -----------------------------------------------------

#region helpers --------------------------------------------------------------
function Write-Stamp([string]$Message, [ConsoleColor]$Color = [ConsoleColor]::Gray) {
  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  $prev = $Host.UI.RawUI.ForegroundColor
  try { $Host.UI.RawUI.ForegroundColor = $Color; Write-Host "[$ts] $Message" }
  finally { $Host.UI.RawUI.ForegroundColor = $prev }
}
function AskChoice {
  param([string]$Prompt,[object[]]$Choices,[int]$DefaultIndex=0,[scriptblock]$Render={ param($x) "$x" },[scriptblock]$Match=$null)
  Write-Host $Prompt -ForegroundColor Cyan
  for ($i=0; $i -lt $Choices.Count; $i++) {
    $star = if ($i -eq $DefaultIndex) { '*' } else { ' ' }
    Write-Host ("  [{0}] {1} {2}" -f $i, (& $Render $Choices[$i]), $star)
  }
  while ($true) {
    $ans = Read-Host "Enter number or value (default $DefaultIndex)"
    if ([string]::IsNullOrWhiteSpace($ans)) { return $Choices[$DefaultIndex] }
    if ($ans -as [int] -ne $null) {
      $idx = [int]$ans
      if ($idx -ge 0 -and $idx -lt $Choices.Count) { return $Choices[$idx] }
    } else {
      if ($Match) {
        $hit = $Choices | Where-Object { & $Match $_ $ans }
        if ($hit) { return $hit[0] }
      }
    }
  }
}
function Get-DomainDefaults {
  try {
    $dom = Get-ADDomain -ErrorAction Stop
    return [pscustomobject]@{ DnsSuffix=$dom.DNSRoot; SearchBase=$dom.DistinguishedName }
  } catch {
    $suffix = (Get-DnsClientGlobalSetting).SuffixSearchList | Select-Object -First 1
    return [pscustomobject]@{ DnsSuffix=$suffix; SearchBase=$null }
  }
}
function Test-ADComputerExists { param([string]$Name,[string]$SearchBase,[string]$Server)
  try { $p=@{LDAPFilter="(name=$Name)";ErrorAction='SilentlyContinue'}; if($SearchBase){$p.SearchBase=$SearchBase}; if($Server){$p.Server=$Server}; [bool](Get-ADComputer @p) } catch { $false } }
function Test-LocalAdminsGroupExists { param([string]$BaseName,[string]$Server)
  foreach($n in @("$BaseName-LocalAdmins","$BaseName LocalAdmins","$BaseName-Local-Admins")){
    try { $p=@{LDAPFilter="(name=$n)";ErrorAction='SilentlyContinue'}; if($script:LocalAdminGroupsOU){$p.SearchBase=$script:LocalAdminGroupsOU}; if($Server){$p.Server=$Server}; if(Get-ADGroup @p){ return $true } } catch {} }
  $false }
function Test-DnsExists { param([string]$Fqdn) try { Resolve-DnsName -Name $Fqdn -Type A,AAAA -ErrorAction Stop | Out-Null; $true } catch { $false } }
function Test-HostPings { param([string]$Target) try { Test-Connection -TargetName $Target -Count 1 -Quiet -ErrorAction SilentlyContinue } catch { $false } }
function Get-ExistingNumbers {
  param([string]$Prefix,[string]$SearchBase,[string]$Server)
  $regex = [regex]"^$([regex]::Escape($Prefix))(\d{3})(?:-LocalAdmins)?$"
  try { $pc=@{LDAPFilter="(name=$Prefix*)";Properties='name';ErrorAction='SilentlyContinue'}; if($SearchBase){$pc.SearchBase=$SearchBase}; if($Server){$pc.Server=$Server}; $comp=(Get-ADComputer @pc).Name } catch { $comp=@() }
  try { $gp=@{LDAPFilter="(name=$Prefix*)";Properties='name';ErrorAction='SilentlyContinue'}; if($script:LocalAdminGroupsOU){$gp.SearchBase=$script:LocalAdminGroupsOU}; if($Server){$gp.Server=$Server}; $grp=(Get-ADGroup @gp).Name } catch { $grp=@() }
  $nums=@(); foreach($n in ($comp+$grp)){ $m=$regex.Match($n); if($m.Success){ $nums+=[int]$m.Groups[1].Value } }; $nums | Sort-Object -Unique }
function Find-NextNumber {
  param([string]$Prefix,[string]$DnsSuffix,[string]$SearchBase,[string]$Server,[int[]]$Reserve=@())
  $existing = Get-ExistingNumbers -Prefix $Prefix -SearchBase $SearchBase -Server $Server
  $reserved = @($Reserve | Sort-Object -Unique)
  for($i=1;$i -le 999;$i++){
    if($existing -contains $i -or $reserved -contains $i){ continue }
    $candidate = ('{0}{1:000}' -f $Prefix,$i)
    $fqdn      = if($DnsSuffix){ "$candidate.$DnsSuffix" } else { $candidate }
    if(Test-ADComputerExists -Name $candidate -SearchBase $SearchBase -Server $Server){ continue }
    if(Test-LocalAdminsGroupExists -BaseName $candidate -Server $Server){ continue }
    if(Test-DnsExists -Fqdn $fqdn){ continue }
    if(Test-HostPings -Target $fqdn){ continue }
    return $candidate
  }
  $null
}
#endregion helpers -----------------------------------------------------------

#region main -----------------------------------------------------------------
try { Import-Module ActiveDirectory -ErrorAction Stop | Out-Null } catch {
  Write-Stamp "ActiveDirectory module not available. Please run on a domain-joined admin shell with RSAT." 'Red'; throw }

$region = AskChoice -Prompt 'Select Region' -Choices @('UK','US','IE') -DefaultIndex 0
$env    = AskChoice -Prompt 'Select Environment' -Choices @('PR','DV') -DefaultIndex 0

# Role selection
$roleChoices = ($Global:RoleMap.GetEnumerator() | ForEach-Object { [pscustomobject]@{ Code=$_.Key; Label=$_.Value } })
$rolePick = AskChoice -Prompt 'Select Role' -Choices $roleChoices -DefaultIndex 0 `
  -Render { param($x) "{0} — {1}" -f $x.Code,$x.Label } `
  -Match  { param($item,$ans) $item.Code.ToUpper() -eq $ans.ToUpper() -or $item.Label.ToUpper() -eq $ans.ToUpper() }
$roleCode = $rolePick.Code

# Optional reserved numbers
$resStr = Read-Host 'Reserved numbers, comma-separated (optional)'
[int[]]$reserve = @()
if ($resStr) { $reserve = @($resStr -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ } | ForEach-Object { [int]$_ }) }

# Prefix and lookup
$prefix = ('{0}{1}{2}' -f $region.ToUpper(), $env.ToUpper(), $roleCode.ToUpper())
$dom    = Get-DomainDefaults
Write-Stamp ("Searching for next available hostname for prefix '{0}'…" -f $prefix) 'Cyan'
Write-Stamp ("AD SearchBase: {0}" -f ($dom.SearchBase ?? '<domain default>'))
Write-Stamp ("DNS suffix:    {0}" -f ($dom.DnsSuffix  ?? '<none>'))

$name = Find-NextNumber -Prefix $prefix -DnsSuffix $dom.DnsSuffix -SearchBase $dom.SearchBase -Server $null -Reserve $reserve
if (-not $name) { Write-Stamp "No free hostnames in range 001..999 for prefix '$prefix'." 'Red'; exit 2 }

$fqdn = if ($dom.DnsSuffix) { "$name.$($dom.DnsSuffix)" } else { $name }

Write-Host ''
Write-Host '==========================================' -ForegroundColor Green
Write-Host ("Suggested hostname: {0}" -f $name) -ForegroundColor Green
Write-Host ("FQDN:               {0}" -f $fqdn) -ForegroundColor Green
Write-Host ("📝 Update the {0}: {1}" -f $Global:HostnameSpreadsheetLabel, $Global:HostnameSpreadsheetUrl) -ForegroundColor Red
Write-Host 'Checks passed: no AD computer, no LocalAdmins group, no DNS A/AAAA, no ping.' -ForegroundColor Green
Write-Host '==========================================' -ForegroundColor Green
Write-Host ''

# Also return it
$name
#endregion main --------------------------------------------------------------
