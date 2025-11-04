# FSMO Best Practices Report with Rationale and Timestamped HTML Output
# Author: Wayne Gregory

# Output path with timestamp
$timestamp = Get-Date -Format "yyyy-MM-dd_HHmm"
$basePath = "C:\Users\GrWay\OneDrive\OneDrive - Beazley Group\Documents\Scripts\Active Directory\CheckFSMO"
$htmlReport = Join-Path $basePath "FSMO_Report_$timestamp.html"

function Get-FsmoRoles {
    $roles = netdom query fsmo | Out-String
    $fsmoRoles = @{}

    foreach ($line in $roles -split "`n") {
        if ($line -match '^(Schema Master|Domain Naming Master|PDC|RID Pool Manager|Infrastructure Master)\s+(\S+)') {
            $fsmoRoles[$matches[1]] = $matches[2]
        }
    }

    return $fsmoRoles
}

function Check-BestPractices {
    param ([hashtable]$FsmoRoles)
    $results = @()

    $rid = $FsmoRoles["RID Pool Manager"]
    $pdc = $FsmoRoles["PDC"]
    $infra = $FsmoRoles["Infrastructure Master"]
    $sameCore = ($rid -eq $pdc -and $pdc -eq $infra)

    $results += [pscustomobject]@{
        Check    = "RID, PDC, and Infrastructure on same DC"
        Result   = $sameCore
        Details  = if ($sameCore) { "All on $rid" } else { "RID=$rid, PDC=$pdc, Infra=$infra" }
        Rationale = if ($sameCore) {
            "✅ Keeping RID, PDC, and Infrastructure together improves performance and avoids replication delays during account provisioning."
        } else {
            "❌ Separation can cause latency and token inconsistencies. Keeping them together avoids inter-DC dependency."
        }
    }

    $schema = $FsmoRoles["Schema Master"]
    $dnm = $FsmoRoles["Domain Naming Master"]
    $separated = ($schema -ne $rid -and $dnm -ne $rid)

    $results += [pscustomobject]@{
        Check    = "Schema & Domain Naming separated from core FSMO roles"
        Result   = $separated
        Details  = "Schema=$schema, DomainNaming=$dnm, CoreRoles on $rid"
        Rationale = if ($separated) {
            "✅ These roles are rarely used. Separating them off the main DC reduces exposure and risk during schema extensions."
        } else {
            "❌ Keeping all roles on one DC increases risk. Schema/Naming should be isolated due to their critical but infrequent use."
        }
    }

    return $results
}

function Get-ReplicationHealth {
    $summary = repadmin /replsummary
    $failures = $summary | Where-Object { $_ -match "\s[1-9]+\s+failures" }

    return [pscustomobject]@{
        Check    = "Replication Health"
        Result   = if (-not $failures) { $true } else { $false }
        Details  = if ($failures) { ($failures -join "<br>") } else { "No replication failures detected." }
        Rationale = if (-not $failures) {
            "✅ Replication is fully healthy. AD changes are syncing correctly across domain controllers."
        } else {
            "❌ Replication issues can lead to login failures, policy delays, or FSMO role confusion."
        }
    }
}

function Generate-HtmlReport {
    param (
        $FsmoRoles, 
        $BestPracticeResults, 
        $ReplicationResult
    )

    $html = @"
<html>
<head>
<style>
body { font-family: Calibri, sans-serif; font-size: 14px; }
table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
th, td { border: 1px solid #ccc; padding: 8px; vertical-align: top; }
th { background-color: #f2f2f2; }
.pass { color: green; font-weight: bold; }
.fail { color: red; font-weight: bold; }
</style>
<title>FSMO Best Practices Report</title>
</head>
<body>
<h2>FSMO Role Holders</h2>
<table>
<tr><th>FSMO Role</th><th>Holder</th></tr>
"@

    foreach ($key in $FsmoRoles.Keys) {
        $html += "<tr><td>$key</td><td>$($FsmoRoles[$key])</td></tr>`n"
    }

    $html += @"
</table>
<h2>Best Practice Evaluation</h2>
<table>
<tr><th>Check</th><th>Result</th><th>Details</th><th>Rationale</th></tr>
"@

    foreach ($item in $BestPracticeResults) {
        $cls = if ($item.Result) { "pass" } else { "fail" }
        $html += "<tr><td>$($item.Check)</td><td class='$cls'>$($item.Result)</td><td>$($item.Details)</td><td>$($item.Rationale)</td></tr>`n"
    }

    $cls = if ($ReplicationResult.Result) { "pass" } else { "fail" }
    $html += "<tr><td>$($ReplicationResult.Check)</td><td class='$cls'>$($ReplicationResult.Result)</td><td>$($ReplicationResult.Details)</td><td>$($ReplicationResult.Rationale)</td></tr>"

    $html += @"
</table>
</body></html>
"@

    $html | Set-Content -Path $htmlReport -Encoding UTF8
    Write-Host "✔ FSMO HTML report saved to: $htmlReport" -ForegroundColor Green
}

function Main {
    $fsmo = Get-FsmoRoles
    $bpResults = Check-BestPractices -FsmoRoles $fsmo
    $replResult = Get-ReplicationHealth
    Generate-HtmlReport -FsmoRoles $fsmo -BestPracticeResults $bpResults -ReplicationResult $replResult
}

Main
