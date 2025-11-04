Import-Module ActiveDirectory

# ---------- Settings ----------
$searchBase           = "DC=bfl,DC=local"
$wuPattern            = "*app.WindowsUpdate*"
$exceptionGroupName   = "app.WindowsUpdate.GBL.ExceptionServers"
$reportDir            = "C:\Reports\AD"
$reportName           = "Servers_WU_ThreeTables.html"
$reportPath           = Join-Path $reportDir $reportName
New-Item -Path $reportDir -ItemType Directory -Force | Out-Null

# ---------- Helpers ----------
function Get-OSBucket {
    param([string]$os)
    if ([string]::IsNullOrWhiteSpace($os)) { return "Other" }
    switch -Regex ($os) {
        'Windows Server 2025'      { '2025'; break }
        'Windows Server 2022'      { '2022'; break }
        'Windows Server 2019'      { '2019'; break }
        'Windows Server 2016'      { '2016'; break }
        'Windows Server 2012 R2'   { '2012 R2'; break }
        'Windows Server 2012'      { '2012'; break }
        'Windows Server 2008 R2'   { '2008 R2'; break }
        default                    { 'Other' }
    }
}

function New-OSCountsHtml {
    param($rows)
    $groups = $rows | Group-Object OSBucket | Sort-Object Name
    if (-not $groups -or $groups.Count -eq 0) { return "<em>No servers</em>" }
    $lis = $groups | ForEach-Object { "<li><strong>$($_.Name)</strong>: $($_.Count)</li>" }
    return "<ul>" + ($lis -join "`n") + "</ul>"
}

function Count-OKlt30 {
    param($rows)
    # OK if DaysSinceLogon < 30 and not null
    return ($rows | Where-Object { $_.DaysSinceLogon -lt 30 -and $_.DaysSinceLogon -ne $null }).Count
}

# ---------- Get groups ----------
$wuGroups = Get-ADGroup -Filter "Name -like '$wuPattern'" -SearchBase $searchBase -Properties DistinguishedName, Name
$excGroup = Get-ADGroup -Filter "Name -eq '$exceptionGroupName'" -SearchBase $searchBase -Properties DistinguishedName, Name

# Maps and sets
$dnToWUGroupNames = @{}   # DN -> [list of *app.WindowsUpdate* group names]
$isWUCore         = @{}   # DN -> True if in any *app.WindowsUpdate* group EXCEPT the Exceptions group
$isException      = @{}   # DN -> True if in Exceptions group

# --- Expand members of ALL *app.WindowsUpdate* groups (recursive) ---
$excDN = if ($excGroup) { $excGroup.DistinguishedName } else { $null }

foreach ($g in $wuGroups) {
    $gName = $g.Name
    $gDN   = $g.DistinguishedName
    try {
        Get-ADGroupMember -Identity $gDN -Recursive -ErrorAction Stop |
            Where-Object { $_.objectClass -eq 'computer' } |
            ForEach-Object {
                $dn = $_.distinguishedName
                if (-not $dnToWUGroupNames.ContainsKey($dn)) {
                    $dnToWUGroupNames[$dn] = New-Object System.Collections.Generic.List[string]
                }
                $dnToWUGroupNames[$dn].Add($gName)

                if ($excDN -and $gDN -eq $excDN) {
                    $isException[$dn] = $true
                } else {
                    $isWUCore[$dn] = $true
                }
            }
    } catch {
        Write-Warning "Failed expanding members of group '$gName': $_"
    }
}

# ---------- Get all enabled Windows Servers (exclude names containing 'VDI') ----------
$allServers = Get-ADComputer -Filter "Enabled -eq 'True' -and OperatingSystem -like 'Windows Server*' -and Name -notlike '*VDI*'" `
               -SearchBase $searchBase -SearchScope Subtree `
               -Properties Name, DNSHostName, OperatingSystem, LastLogonDate, DistinguishedName

# ---------- Shape data ----------
function New-Row {
    param($c)
    $ll  = $c.LastLogonDate
    $age = if ($ll) { (New-TimeSpan -Start $ll -End (Get-Date)).Days } else { $null }

    $wuNames = if ($dnToWUGroupNames.ContainsKey($c.DistinguishedName)) {
        ($dnToWUGroupNames[$c.DistinguishedName] | Sort-Object -Unique) -join '; '
    } else { '' }

    $inExc   = [bool]$isException[$c.DistinguishedName]
    $inCore  = [bool]$isWUCore[$c.DistinguishedName]
    $inAnyWU = $inExc -or $inCore
    $flags   = if ($inExc -and $inCore) { 'BOTH' } else { '' }

    [pscustomobject]@{
        Name            = $c.Name
        DNSHostName     = $c.DNSHostName
        OperatingSystem = $c.OperatingSystem
        OSBucket        = Get-OSBucket $c.OperatingSystem
        LastLogonDT     = $ll
        LastLogon       = if ($ll) { $ll.ToString("yyyy-MM-dd HH:mm") } else { "" }
        DaysSinceLogon  = $age
        Status          = if (-not $ll) { "Never" }
                          elseif ($age -ge 180) { "Stale-180+" }
                          elseif ($age -ge 90)  { "Stale-90+" }
                          elseif ($age -ge 30)  { "Stale-30+" }
                          else { "OK" }
        'WUGroup(s)'    = $wuNames
        InExceptions    = $inExc
        InPatchingWU    = $inCore
        InAnyWU         = $inAnyWU
        Flags           = $flags   # 'BOTH' if in Exceptions AND another *app.WindowsUpdate* group
    }
}

$rows = $allServers | ForEach-Object { New-Row $_ }

# ---------- Split into the three requested tables ----------
$tbl_NotInAny   = $rows | Where-Object { -not $_.InAnyWU }                       | Sort-Object LastLogonDT -Descending
$tbl_Exceptions = $rows | Where-Object { $_.InExceptions }                        | Sort-Object LastLogonDT -Descending
$tbl_Both       = $rows | Where-Object { $_.InExceptions -and $_.InPatchingWU }   | Sort-Object LastLogonDT -Descending

# ---------- Summaries ----------
$domain   = (Get-ADDomain).DNSRoot
$now      = Get-Date

$c1 = $tbl_NotInAny.Count
$c2 = $tbl_Exceptions.Count
$c3 = $tbl_Both.Count

# OK (<30d) and Requires Attention (>=30d or Never)
$c1_ok   = Count-OKlt30 $tbl_NotInAny
$c2_ok   = Count-OKlt30 $tbl_Exceptions
$c3_ok   = Count-OKlt30 $tbl_Both

$c1_attn = $c1 - $c1_ok
$c2_attn = $c2 - $c2_ok
$c3_attn = $c3 - $c3_ok

# OS counts per table (HTML lists)
$os1 = New-OSCountsHtml -rows $tbl_NotInAny
$os2 = New-OSCountsHtml -rows $tbl_Exceptions
$os3 = New-OSCountsHtml -rows $tbl_Both

# ---------- HTML ----------
$head = @"
<style>
  body { font-family: Segoe UI, Arial, sans-serif; margin: 24px; }
  h1 { margin-bottom: 6px; }
  .meta { color:#444; margin: 0 0 18px 0; }
  h2 { margin-top: 28px; }
  table { border-collapse: collapse; width: 100%; margin-top: 8px; }
  th, td { border: 1px solid #ddd; padding: 8px; }
  th { background:#f5f5f5; text-align:left; position: sticky; top:0; }
  tr.ok { }
  tr.stale30 { background:#fff8e6; }
  tr.stale90 { background:#ffe9e9; }
  tr.stale180 { background:#ffd6d6; font-weight: 600; }
  tr.never { background:#f0f0f0; font-style: italic; }
  tr.conflict { outline: 3px solid #d4aa00; }  /* BOTH */
  .legend { margin: 12px 0 20px 0; }
  .legend span { display:inline-block; padding:4px 8px; margin-right:8px; border:1px solid #ddd; }
  .pill { display:inline-block; padding: 2px 6px; border-radius: 8px; border:1px solid #bbb; }
  .pill-both { background:#fff4cc; border-color:#d4aa00; font-weight:600; }
  .counts { margin: 6px 0 4px 0; }
</style>
<script>
  // Color rows by "Status" and highlight conflicts by "Flags"
  window.addEventListener("DOMContentLoaded", () => {
    document.querySelectorAll("table").forEach(tbl => {
      const rows = tbl.querySelectorAll("tbody tr");
      rows.forEach(r => {
        const cells = r.cells;
        const status = (cells[5]?.innerText || "").toLowerCase(); // Status column
        const flags  = (cells[7]?.innerText || "").toUpperCase(); // Flags column

        if (status.includes("never")) r.classList.add("never");
        else if (status.includes("180")) r.classList.add("stale180");
        else if (status.includes("90"))  r.classList.add("stale90");
        else if (status.includes("30"))  r.classList.add("stale30");
        else r.classList.add("ok");

        if (flags.includes("BOTH")) r.classList.add("conflict");
      });
    });
  });
</script>
"@

$pre = @"
<h1>Enabled Windows Servers – Patching Group Overview</h1>
<p class="meta">
  Domain: <strong>$domain</strong> &nbsp;|&nbsp;
  Generated: <strong>$($now.ToString("yyyy-MM-dd HH:mm"))</strong>
</p>
<div class="legend">
  <span>OK</span>
  <span style="background:#fff8e6">Stale-30+</span>
  <span style="background:#ffe9e9">Stale-90+</span>
  <span style="background:#ffd6d6">Stale-180+</span>
  <span style="background:#f0f0f0">Never</span>
  <span class="pill pill-both">BOTH: In Exceptions & another *app.WindowsUpdate* group</span>
</div>
"@

# Build the three HTML tables
$commonCols = @('Name','DNSHostName','OperatingSystem','LastLogon','DaysSinceLogon','Status','WUGroup(s)','Flags')

$h1 = $tbl_NotInAny   | Select-Object $commonCols | ConvertTo-Html -Fragment
$h2 = $tbl_Exceptions | Select-Object $commonCols | ConvertTo-Html -Fragment
$h3 = $tbl_Both       | Select-Object $commonCols | ConvertTo-Html -Fragment

$body = @"
$pre

<h2>1) Not in any Patching Groups (<code>$wuPattern</code>)</h2>
<p class="counts">
  <strong>Total:</strong> $c1 &nbsp;|&nbsp;
  <strong>OK (&lt;30d):</strong> $c1_ok &nbsp;|&nbsp;
  <strong>Requires attention (≥30d or Never):</strong> $c1_attn
</p>
<p><strong>By OS version:</strong></p>
$os1
$h1

<h2>2) In Exceptions Group (<code>$exceptionGroupName</code>)</h2>
<p class="counts">
  <strong>Total:</strong> $c2 &nbsp;|&nbsp;
  <strong>OK (&lt;30d):</strong> $c2_ok &nbsp;|&nbsp;
  <strong>Requires attention (≥30d or Never):</strong> $c2_attn
</p>
<p><strong>By OS version:</strong></p>
$os2
$h2

<h2>3) Exists in BOTH (Exceptions & another <code>$wuPattern</code> group)</h2>
<p class="counts">
  <strong>Total:</strong> $c3 &nbsp;|&nbsp;
  <strong>OK (&lt;30d):</strong> $c3_ok &nbsp;|&nbsp;
  <strong>Requires attention (≥30d or Never):</strong> $c3_attn
</p>
<p><strong>By OS version:</strong></p>
$os3
$h3
"@

# Compose & write full HTML
$full = "<html><head><meta charset='utf-8'><title>Servers – Patching Group Overview</title>$head</head><body>$body</body></html>"
$full | Out-File -FilePath $reportPath -Encoding UTF8

Start-Process $reportPath
Write-Host "Report written to $reportPath" -ForegroundColor Green
