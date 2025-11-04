<# 
.SYNOPSIS
  Report Zerto tags across UK & IE vCenters.
  Treat these categories as Zerto: "Zerto – Protection Automation" (en-dash) and "ZertoAutoProtect".
  Exclude infra/support VMs (Z-VRA*, vCLS*, VPG_*, *dt-vsensor*, *DC*, *K8*).
  Separate tables for: 
    • VMs With No Zerto Tags 
    • VMs Explicitly Tagged NoZerto 
    • VMs With Matching Zerto Tags
  HTML opens in Edge.
#>

[CmdletBinding()]
param(
    [string[]]$vCenters = @('ukprim098.bfl.local','ieprim018.bfl.local'),

    [string[]]$ZertoCategories = @('Zerto – Protection Automation','ZertoAutoProtect'),
    [string[]]$ExcludeTagNames = @('NoZerto'),

    [pscredential]$Credential,
    [string]$OutputPath = (Join-Path -Path $PWD -ChildPath ("ZertoTagsReport_{0}.html" -f (Get-Date -Format "yyyyMMdd_HHmm")))
)

begin {
    try { Set-PowerCLIConfiguration -InvalidCertificateAction Ignore -Confirm:$false | Out-Null } catch {}
    if (-not $Credential) { $Credential = Get-Credential -Message "Enter vCenter credentials" }

    $detailRows    = New-Object System.Collections.Generic.List[object] # qualifying Zerto tags
    $noZertoRows   = New-Object System.Collections.Generic.List[object] # explicitly tagged NoZerto
    $noTagRows     = New-Object System.Collections.Generic.List[object] # no Zerto tags at all
    $coverageRows  = New-Object System.Collections.Generic.List[object]

    function Normalize-Dash([string]$s) { if ($null -eq $s) { return '' } return ($s -replace '[\u2013\u2014]','-') } # en/em dash -> hyphen
    $allowedCats = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($c in $ZertoCategories) { [void]$allowedCats.Add((Normalize-Dash $c)) }

    $excludeTagSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($t in $ExcludeTagNames) { [void]$excludeTagSet.Add($t) }
}

process {
    foreach ($vc in $vCenters) {
        Write-Host "Connecting to ${vc} ..." -ForegroundColor Cyan
        $svr = $null
        try { $svr = Connect-VIServer -Server $vc -Credential $Credential -WarningAction SilentlyContinue -ErrorAction Stop }
        catch { Write-Warning "Failed to connect to ${vc}: $($_.Exception.Message)"; continue }

        try {
            $vms = Get-VM -Server $svr -ErrorAction Stop |
                   Where-Object {
                       $_.Name -notmatch '^(Z-VRA|vCLS)' -and
                       $_.Name -notmatch '^(?i)VPG_' -and
                       $_.Name -notmatch '(?i)dt-vsensor' -and
                       $_.Name -notmatch '(?i)DC' -and
                       $_.Name -notmatch '(?i)K8'
                   }

            Write-Host ("${vc}: {0} VMs after exclusions" -f $vms.Count) -ForegroundColor DarkGray

            $withCount = 0; $withoutCount = 0; $noZertoCount = 0

            foreach ($vm in $vms) {
                $assignments = @()
                try { $assignments = Get-TagAssignment -Entity $vm -Server $svr -ErrorAction Stop }
                catch { Write-Warning "Tag lookup failed for VM '$($vm.Name)' on ${vc}: $($_.Exception.Message)"; continue }

                # First: detect NoZerto tags by NAME (regardless of category)
                $noZertoHits = $assignments | Where-Object { $_.Tag -and $excludeTagSet.Contains($_.Tag.Name) }

                if ($noZertoHits.Count -gt 0) {
                    foreach ($h in $noZertoHits) {
                        $noZertoRows.Add([pscustomobject]@{
                            vCenter     = $vc
                            VMName      = $vm.Name
                            PowerState  = $vm.PowerState
                            TagCategory = (Normalize-Dash $h.Tag.Category.Name)
                            TagName     = $h.Tag.Name
                        })
                    }
                    $noZertoCount++
                    continue
                }

                # Then: detect qualifying Zerto tags
                $hits = $assignments | Where-Object {
                    $_.Tag -and
                    $allowedCats.Contains( (Normalize-Dash $_.Tag.Category.Name) ) -and
                    -not $excludeTagSet.Contains( $_.Tag.Name )
                }

                if ($hits.Count -gt 0) {
                    foreach ($h in $hits) {
                        $detailRows.Add([pscustomobject]@{
                            vCenter     = $vc
                            VMName      = $vm.Name
                            PowerState  = $vm.PowerState
                            TagCategory = (Normalize-Dash $h.Tag.Category.Name)
                            TagName     = $h.Tag.Name
                        })
                    }
                    $withCount++
                } else {
                    $noTagRows.Add([pscustomobject]@{
                        vCenter    = $vc
                        VMName     = $vm.Name
                        PowerState = $vm.PowerState
                    })
                    $withoutCount++
                }
            }

            $coverageRows.Add([pscustomobject]@{
                vCenter          = $vc
                VMCount          = $vms.Count
                WithZertoTags    = $withCount
                NoZertoCount     = $noZertoCount
                WithoutZertoTags = $withoutCount
            })
        }
        finally {
            if ($svr) { Disconnect-VIServer -Server $svr -Confirm:$false | Out-Null }
        }
    }
}

end {
    $style = @"
<style>
body { font-family: Segoe UI, Roboto, Arial, sans-serif; margin: 20px; }
h1 { margin-bottom: 0; }
small { color: #666; }
table { border-collapse: collapse; width: 100%; margin-top: 16px; }
th, td { border: 1px solid #ddd; padding: 8px; }
th { background: #f5f5f5; text-align: left; }
tr:nth-child(even) { background: #fafafa; }
code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
</style>
"@

    $reportTime = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $title = "Zerto Tags Report (UK & IE Only)"
    $intro = @"
<h1>$title</h1>
<small>Generated: $reportTime</small>
<p><b>vCenters:</b> $(($vCenters -join ', '))</p>
<p><b>Zerto Categories:</b> <code>$(($ZertoCategories -join '</code>, <code>'))</code></p>
<p><b>Excluded by Tag Name (treated as physically excluded):</b> <code>$(($ExcludeTagNames -join '</code>, <code>'))</code></p>
<p><b>Excluded VM name patterns:</b> <code>Z-VRA*</code>, <code>vCLS*</code>, <code>VPG_*</code>, <code>*dt-vsensor*</code>, <code>*DC*</code>, <code>*K8*</code></p>
"@

    $coverageTable = $coverageRows | Sort-Object vCenter |
        ConvertTo-Html -Fragment -PreContent "<h2>Coverage Summary</h2>" `
        -Property vCenter, VMCount, WithZertoTags, NoZertoCount, WithoutZertoTags

    # --- New Display Order ---
    $noTagTable = if ($noTagRows.Count -gt 0) {
        ($noTagRows | Sort-Object vCenter, VMName |
            ConvertTo-Html -Fragment -PreContent "<h2>VMs With <u>No</u> Zerto Tags</h2>" `
            -Property vCenter, VMName, PowerState)
    } else { "<p><i>No VMs missing Zerto tags.</i></p>" }

    $noZertoTable = if ($noZertoRows.Count -gt 0) {
        ($noZertoRows | Sort-Object vCenter, VMName, TagName |
            ConvertTo-Html -Fragment -PreContent "<h2>VMs Explicitly Tagged <code>NoZerto</code></h2>" `
            -Property vCenter, VMName, PowerState, TagCategory, TagName)
    } else { "<p><i>No VMs are explicitly tagged <code>NoZerto</code>.</i></p>" }

    $detailTable = if ($detailRows.Count -gt 0) {
        ($detailRows | Sort-Object vCenter, VMName, TagCategory, TagName |
            ConvertTo-Html -Fragment -PreContent "<h2>Details (Matching Zerto Tags)</h2>" `
            -Property vCenter, VMName, PowerState, TagCategory, TagName)
    } else { "<p><i>No VMs have qualifying Zerto tags.</i></p>" }

    # Assemble report in the new order
    $html = ConvertTo-Html -Title $title -Head $style -Body ($intro + $coverageTable + $noTagTable + $noZertoTable + $detailTable)

    $folder = Split-Path -Path $OutputPath -Parent
    if (-not (Test-Path $folder)) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }
    $html | Out-File -FilePath $OutputPath -Encoding UTF8

    Write-Host "✅ Report written to: $OutputPath" -ForegroundColor Green

    try { Start-Process "msedge" -ArgumentList "`"$OutputPath`"" } catch {
        $edgePaths = @("$env:ProgramFiles\Microsoft\Edge\Application\msedge.exe",
                       "$env:ProgramFiles(x86)\Microsoft\Edge\Application\msedge.exe")
        foreach ($p in $edgePaths) { if (Test-Path $p) { Start-Process $p -ArgumentList "`"$OutputPath`""; break } }
    }
}
