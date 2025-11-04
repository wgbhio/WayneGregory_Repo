# Clone System DSNs from ukdvap213 + ukdvap246 -> LOCAL (UKDVAP339)
# Shows in BOTH 64-bit and 32-bit ODBC GUIs

$sources = @('ukdvap213','ukdvap246')   # later host wins on duplicates

# --- helpers ---
function Assert-Admin {
  $id=[Security.Principal.WindowsIdentity]::GetCurrent()
  $p =New-Object Security.Principal.WindowsPrincipal($id)
  if(-not $p.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)){ throw "Run this PowerShell as Administrator." }
}

function Get-RemoteSystemDsns {
  param([string]$Computer)
  $out = @()

  foreach ($wow in $false,$true) {
    $plat = if($wow){'32-bit'} else {'64-bit'}
    $path = if($wow){'SOFTWARE\WOW6432Node\ODBC\ODBC.INI'} else {'SOFTWARE\ODBC\ODBC.INI'}
    try {
      $base = [Microsoft.Win32.RegistryKey]::OpenRemoteBaseKey([Microsoft.Win32.RegistryHive]::LocalMachine,$Computer)
      $root = $base.OpenSubKey($path)
      if(-not $root){ continue }

      $ods  = $root.OpenSubKey('ODBC Data Sources')  # may be $null if never created
      $names = $root.GetSubKeyNames() | Where-Object { $_ -ne 'ODBC Data Sources' }

      foreach($name in $names){
        $sub = $root.OpenSubKey($name)
        if(-not $sub){ continue }
        $vals = @{}
        foreach($v in $sub.GetValueNames()){ $vals[$v] = $sub.GetValue($v) }
        $driver = if($ods){ $ods.GetValue($name, $null) } else { $null }
        $out += [PSCustomObject]@{
          Source   = $Computer
          Name     = $name
          Platform = $plat
          Driver   = $driver
          Settings = $vals
        }
      }
    } catch {
      Write-Warning "[$Computer][$plat] Remote registry read failed: $($_.Exception.Message)"
    }
  }

  return $out
}

function Backup-LocalOdbc {
  $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
  $dir = Join-Path $env:TEMP "ODBC-LOCAL-BACKUP-$stamp"
  New-Item -ItemType Directory -Path $dir -Force | Out-Null
  & reg.exe export "HKLM\SOFTWARE\ODBC\ODBC.INI" "$dir\ODBC64.reg" /y | Out-Null
  & reg.exe export "HKLM\SOFTWARE\WOW6432Node\ODBC\ODBC.INI" "$dir\ODBC32.reg" /y | Out-Null
  Write-Host "Local ODBC backup: $dir" -ForegroundColor Yellow
}

function Ensure-LocalDsn {
  param(
    [string]$Platform,  # '64-bit' or '32-bit'
    [string]$Name,
    [hashtable]$Values,
    [string]$Driver
  )

  $base = if($Platform -eq '32-bit'){
    "HKLM:\SOFTWARE\WOW6432Node\ODBC\ODBC.INI\$Name"
  } else {
    "HKLM:\SOFTWARE\ODBC\ODBC.INI\$Name"
  }

  if(-not (Test-Path $base)){ New-Item -Path $base -Force | Out-Null }

  foreach($k in $Values.Keys){
    try { Set-ItemProperty -Path $base -Name $k -Value $Values[$k] -Force } catch { }
  }

  $list = Join-Path (Split-Path $base) 'ODBC Data Sources'
  if(-not (Test-Path $list)){ New-Item -Path $list -Force | Out-Null }
  if($Driver){ Set-ItemProperty -Path $list -Name $Name -Value $Driver -Force }
}

function Has-LocalDriver {
  param([string]$Driver,[string]$Platform)
  if([string]::IsNullOrWhiteSpace($Driver)){ return $false }
  $root = if($Platform -eq '32-bit'){
    'HKLM:\SOFTWARE\WOW6432Node\ODBC\ODBCINST.INI\ODBC Drivers'
  } else {
    'HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers'
  }
  if(-not (Test-Path $root)){ return $false }
  try { $o = Get-ItemProperty -Path $root -ErrorAction Stop; return ($o.$Driver -and $o.$Driver -ne 0) } catch { return $false }
}

# --- main ---
Assert-Admin

Write-Host "Reading System DSNs from sources..." -ForegroundColor Cyan
$all = @()
foreach($s in $sources){ $all += Get-RemoteSystemDsns -Computer $s }

# If sources only have 64-bit DSNs, we will still write to BOTH local hives
if(($all | Measure-Object).Count -eq 0){
  throw "No System DSNs could be read from sources. Check RemoteRegistry/firewall/permissions."
}

# De-dup by Name (prefer later sources) using 64-bit set as the canonical definition
$byName = @{}
foreach($d in ($all | Where-Object { $_.Platform -eq '64-bit' })){
  $byName[$d.Name] = $d
}
# If a 64-bit definition didn't exist, fall back to any platform’s definition
foreach($d in $all){
  if(-not $byName.ContainsKey($d.Name)){ $byName[$d.Name] = $d }
}

Backup-LocalOdbc

$missing = @()
foreach($name in $byName.Keys){
  $def = $byName[$name]

  # Write to LOCAL 64-bit
  Ensure-LocalDsn -Platform '64-bit' -Name $def.Name -Values $def.Settings -Driver $def.Driver
  # Write to LOCAL 32-bit (so it appears in 32-bit GUI too)
  Ensure-LocalDsn -Platform '32-bit' -Name $def.Name -Values $def.Settings -Driver $def.Driver

  if(-not (Has-LocalDriver -Driver $def.Driver -Platform '64-bit')){
    $missing += [PSCustomObject]@{ Name=$def.Name; Platform='64-bit'; Driver=$def.Driver }
  }
  if(-not (Has-LocalDriver -Driver $def.Driver -Platform '32-bit')){
    $missing += [PSCustomObject]@{ Name=$def.Name; Platform='32-bit'; Driver=$def.Driver }
  }

  Write-Host ("Wrote DSN '{0}' to local 64-bit and 32-bit" -f $def.Name)
}

Write-Host "`nOpen ODBC GUIs to confirm:" -ForegroundColor Green
Write-Host "  64-bit: C:\Windows\System32\odbcad32.exe"
Write-Host "  32-bit: C:\Windows\SysWOW64\odbcad32.exe"

if($missing.Count -gt 0){
  Write-Warning "`nDrivers missing locally for these DSNs (they will list, but tests may fail):"
  $missing | Sort-Object Platform,Name | Format-Table -AutoSize
}
