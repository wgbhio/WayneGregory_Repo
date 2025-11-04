<# 
.SYNOPSIS
  Query NS servers for a zone and check a host's DNS record on each, with optional PTR (reverse) checks, then summarize drift.

.EXAMPLE
  .\Check-DnsOnNs.ps1 -Domain bfl.local -Host EULONVDIDV072 -RecordTypes A,CNAME,PTR -Verbose
#>

[CmdletBinding()]
param(
  [string]$Domain = 'bfl.local',
  [Parameter(Mandatory=$true)]
  [Alias('Host')]
  [string]$RecordName,
  [ValidateSet('A','AAAA','CNAME','TXT','MX','SRV','NS','PTR')]
  [string[]]$RecordTypes = @('A','CNAME'),
  [string]$ExportCsv
)

function Resolve-HostOrIp {
  param([string]$NameOrIp)
  if ([System.Net.IPAddress]::TryParse($NameOrIp, [ref]([ipaddress]$null))) { return ,$NameOrIp }

  $ips = @()
  try { $a = Resolve-DnsName -Name $NameOrIp -Type A -ErrorAction Stop; $ips += ($a | ? {$_.Type -eq 'A'}).IPAddress } catch {}
  try { $aaaa = Resolve-DnsName -Name $NameOrIp -Type AAAA -ErrorAction Stop; $ips += ($aaaa | ? {$_.Type -eq 'AAAA'}).IPAddress } catch {}

  if (-not $ips) { return ,$NameOrIp }
  return $ips
}

function ConvertTo-PtrName {
  param([Parameter(Mandatory=$true)][string]$Ip)
  $ipObj = $null
  if (-not [System.Net.IPAddress]::TryParse($Ip, [ref]$ipObj)) { return $null }
  if ($ipObj.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork) {
    $octs = $Ip.Split('.'); if ($octs.Count -ne 4) { return $null }
    return "$($octs[3]).$($octs[2]).$($octs[1]).$($octs[0]).in-addr.arpa"
  } else {
    $bytes = $ipObj.GetAddressBytes()
    $hex = ($bytes | % { '{0:x2}' -f $_ }) -join ''
    $nibbles = $hex.ToCharArray(); [array]::Reverse($nibbles)
    return (($nibbles -join '.') + '.ip6.arpa')
  }
}

# Build target FQDN
if ($RecordName -like "*.*") { $targetFqdn = $RecordName } else { $targetFqdn = "$RecordName.$Domain" }

Write-Verbose "Looking up NS records for zone: $Domain"
try {
  $nsHosts = (Resolve-DnsName -Name $Domain -Type NS -ErrorAction Stop |
              Where-Object { $_.Type -eq 'NS' } |
              Select-Object -ExpandProperty NameHost -Unique)
} catch {
  Write-Error "Failed to resolve NS records for '$Domain'. $_"; exit 1
}
if (-not $nsHosts) { Write-Error "No NS records found for '$Domain'."; exit 1 }

Write-Verbose ("Found NS hosts: " + ($nsHosts -join ', '))

# Expand NS hostnames to endpoints
$nsServers = @()
foreach ($ns in $nsHosts) {
  $endpoints = Resolve-HostOrIp -NameOrIp $ns
  if (-not $endpoints -or $endpoints.Count -eq 0) { $endpoints = @($ns) }
  foreach ($ep in $endpoints) {
    if ([string]::IsNullOrWhiteSpace($ep)) { continue }
    $nsServers += [pscustomobject]@{ NsHost = $ns; Server = $ep }
  }
}
$nsServers = $nsServers | Sort-Object NsHost, Server -Unique
Write-Verbose ("Querying servers: " + ($nsServers.Server -join ', '))

$results = New-Object System.Collections.Generic.List[Object]

# Ensure A/AAAA happen before PTR so we can reverse IPs
$nonPtrTypes = $RecordTypes | Where-Object { $_ -ne 'PTR' }
$includePtr  = $RecordTypes -contains 'PTR'
$ipAnswers = @{}  # per (NsHost|Server) -> [IPs]

# -------- Pass 1: non-PTR types --------
foreach ($srv in $nsServers) {
  foreach ($rtype in $nonPtrTypes) {
    $status = 'NotFound'; $answerText = $null; $ttl = $null; $errorMsg = $null
    try {
      $resp = Resolve-DnsName -Name $targetFqdn -Type $rtype -Server $srv.Server -ErrorAction Stop
      $answers = @()
      foreach ($r in $resp) {
        switch ($r.Type) {
          'A'     { $answers += $r.IPAddress }
          'AAAA'  { $answers += $r.IPAddress }
          'CNAME' { $answers += $r.NameHost }
          'TXT'   { $answers += ($r.strings -join ' ') }
          'MX'    { $answers += "$($r.MailExchange) (Pref $($r.Preference))" }
          'SRV'   { $answers += "$($r.Target) $($r.Port)/$($r.Priority)/$($r.Weight)" }
          default { if ($r.NameHost) { $answers += $r.NameHost } }
        }
        if ($r.TTL -and -not $ttl) { $ttl = $r.TTL }
      }
      if ($answers.Count -gt 0) { $status = 'Exists'; $answerText = ($answers | Select-Object -Unique) -join '; ' }
      else { $status = 'NoAnswer' }
    } catch { $status = 'Error'; $errorMsg = $_.Exception.Message }

    # Capture IPs for PTR
    if ($status -eq 'Exists' -and ($rtype -eq 'A' -or $rtype -eq 'AAAA')) {
      $key = "$($srv.NsHost)|$($srv.Server)"
      if (-not $ipAnswers.ContainsKey($key)) { $ipAnswers[$key] = @() }
      $parts = $answerText -split '; ' | Where-Object { $_ -and $_ -match '^\S+$' }
      foreach ($ip in $parts) { if (-not ($ipAnswers[$key] -contains $ip)) { $ipAnswers[$key] += $ip } }
    }

    $answerOut = $null; if ($errorMsg) { $answerOut = $errorMsg } elseif ($answerText) { $answerOut = $answerText }
    $results.Add([pscustomobject]@{
      Zone=$Domain; QueriedName=$targetFqdn; RecordType=$rtype; NsHost=$srv.NsHost; Server=$srv.Server;
      Status=$status; Answer=$answerOut; TTL=$ttl; Timestamp=(Get-Date)
    })
  }
}

# -------- Pass 2: PTR (reverse) --------
if ($includePtr) {
  foreach ($srv in $nsServers) {
    $status='NoAnswer'; $answerText=$null; $ttl=$null; $errorMsg=$null
    $key = "$($srv.NsHost)|$($srv.Server)"
    $ipsForThisServer = @()
    if ($ipAnswers.ContainsKey($key)) { $ipsForThisServer = $ipAnswers[$key] }
    if (-not $ipsForThisServer -or $ipsForThisServer.Count -eq 0) {
      try { $aResp = Resolve-DnsName -Name $targetFqdn -Type A -Server $srv.Server -ErrorAction Stop
            $ipsForThisServer = ($aResp | ? {$_.Type -eq 'A'} | Select-Object -ExpandProperty IPAddress -Unique) } catch {}
    }

    if (-not $ipsForThisServer -or $ipsForThisServer.Count -eq 0) {
      $answerText = "No A/AAAA to reverse"
    } else {
      $ptrPairs = @()
      foreach ($ip in $ipsForThisServer) {
        $ptrName = ConvertTo-PtrName -Ip $ip
        if (-not $ptrName) { $ptrPairs += "$ip -> (invalid IP)"; continue }
        try {
          $ptrResp = Resolve-DnsName -Name $ptrName -Type PTR -Server $srv.Server -ErrorAction Stop
          $ptrTargets = ($ptrResp | ? {$_.Type -eq 'PTR'} | Select-Object -ExpandProperty NameHost -Unique)
          if ($ptrTargets -and $ptrTargets.Count -gt 0) {
            if (-not $ttl) { $first = $ptrResp | Select-Object -First 1; if ($first -and $first.TTL) { $ttl = $first.TTL } }
            $status = 'Exists'
            $ptrPairs += ($ip + " -> " + ($ptrTargets -join ','))
          } else {
            if ($status -ne 'Exists') { $status = 'NoAnswer' }
            $ptrPairs += ($ip + " -> (NoAnswer)")
          }
        } catch { $status='Error'; $errorMsg=$_.Exception.Message; $ptrPairs += ($ip + " -> (Error)") }
      }
      if ($ptrPairs.Count -gt 0 -and -not $errorMsg) { $answerText = ($ptrPairs -join ' ; ') }
    }

    $answerOut=$null; if ($errorMsg) {$answerOut=$errorMsg} elseif ($answerText) {$answerOut=$answerText}
    $results.Add([pscustomobject]@{
      Zone=$Domain; QueriedName=$targetFqdn; RecordType='PTR'; NsHost=$srv.NsHost; Server=$srv.Server;
      Status=$status; Answer=$answerOut; TTL=$ttl; Timestamp=(Get-Date)
    })
  }
}

# ---------- Output: Per-server detail ----------
Write-Verbose ("Collected results: " + ($results.Count))
$results |
  Sort-Object RecordType, NsHost, Server |
  Format-Table RecordType, NsHost, Server, Status, Answer, TTL, Timestamp -AutoSize

if ($ExportCsv) {
  try { $results | Export-Csv -NoTypeInformation -Path $ExportCsv -Encoding UTF8; Write-Host "Results exported to $ExportCsv" }
  catch { Write-Warning "Failed to export CSV: $_" }
}

# ---------- Summary / Drift detection (bullet-proof) ----------
Write-Host ""
Write-Host "===== SUMMARY (by RecordType) ====="

if (-not $results -or $results.Count -eq 0) {
  Write-Host "No results collected — check earlier errors/verbose output."
  return
}

$groups = $results | Group-Object RecordType
$summary = foreach ($g in $groups) {
  $subset = $g.Group; $rtype = if ($g.Name) { $g.Name } else { '(unknown)' }

  $exists   = $subset | Where-Object {$_.Status -eq 'Exists'}
  $noanswer = $subset | Where-Object {$_.Status -eq 'NoAnswer'}
  $notfound = $subset | Where-Object {$_.Status -eq 'NotFound'}
  $errors   = $subset | Where-Object {$_.Status -eq 'Error'}

  $distinctAnswers = $exists | Where-Object { $_.Answer } | Select-Object -ExpandProperty Answer -Unique

  $answerCounts = @{}; $majorityAnswer = $null; $majorityCount = 0
  foreach ($row in $exists) {
    $ans = $row.Answer; if (-not $ans) { continue }
    if (-not $answerCounts.ContainsKey($ans)) { $answerCounts[$ans] = 0 }
    $answerCounts[$ans]++
  }
  foreach ($k in $answerCounts.Keys) { if ($answerCounts[$k] -gt $majorityCount) { $majorityCount=$answerCounts[$k]; $majorityAnswer=$k } }

  $allNoAnswer = (($subset | Where-Object {$_.Status -ne 'NoAnswer'}) | Measure-Object).Count -eq 0
  $hasError    = ($errors   | Measure-Object).Count -gt 0
  $hasNotFound = ($notfound | Measure-Object).Count -gt 0
  $hasExists   = ($exists   | Measure-Object).Count -gt 0
  $hasNoAnswer = ($noanswer | Measure-Object).Count -gt 0
  $distinctAnswerCount = ($distinctAnswers | Measure-Object).Count

  $drift = $false
  if     ($hasError -or $hasNotFound) { $drift = $true }
  elseif ($allNoAnswer)               { $drift = $false }
  elseif ($distinctAnswerCount -gt 1) { $drift = $true }
  elseif ($hasExists -and $hasNoAnswer) { $drift = $true }

  [pscustomobject]@{
    RecordType     = $rtype
    Exists         = ($exists   | Measure-Object).Count
    NoAnswer       = ($noanswer | Measure-Object).Count
    NotFound       = ($notfound | Measure-Object).Count
    Error          = ($errors   | Measure-Object).Count
    UniqueAnswers  = $distinctAnswerCount
    DriftDetected  = $drift
    MajorityAnswer = $majorityAnswer
    Answers        = ($distinctAnswers -join ' | ')
  }
}

$summary | Sort-Object RecordType |
  Format-Table RecordType, Exists, NoAnswer, NotFound, Error, UniqueAnswers, DriftDetected, MajorityAnswer, Answers -AutoSize
