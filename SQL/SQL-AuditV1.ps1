<#
.SYNOPSIS
  AD "DB" servers → vCenter match (partial), NUMA risk + Memory Reservation checks,
  guest power plan via Invoke-Command, and Excel export with row highlighting + Summary.

.REQUIREMENTS
  - RSAT / ActiveDirectory module
  - VMware PowerCLI (Install-Module VMware.PowerCLI)
  - ImportExcel module (auto-installs if missing; falls back to CSV)
  - WinRM enabled on Windows guests you want to query; valid credentials

.SHEETS
  - AllResults
  - NUMA_Risk
  - NotFound
  - Summary (RiskByVCCluster & RiskByVC)

.ROW COLOUR RULES
  - Salmon: NumaRisk=Yes OR CpuHotAdd=True OR MemoryHotAdd=True
  - Yellow: MemoryReservationLockedToMax=False
  - LightOrange: GuestPowerPlan != "High performance"
#>

[CmdletBinding()]
param(
    [string]$SearchBase,
    [string]$NameContains = 'DB',
    [string[]]$vCenters = @('ukprim098.bfl.local','usprim004.bfl.local'),

    # NUMA heuristic
    [int]$NumaVcpuThreshold = 8,

    # Output controls
    [string]$OutputDir = 'C:\Users\GrWay\OneDrive\OneDrive - Beazley Group\Documents\Scripts\SQL',
    [string]$WorkbookPrefix = 'DBServers_vs_vCenter',
    [switch]$AlsoWriteCsv,

    # Guest query
    [pscredential]$GuestCredential,
    [int]$PowerPlanTimeoutSec = 15
)

begin {
    $ErrorActionPreference = 'Stop'

    if ($OutputDir -and -not (Test-Path $OutputDir)) {
        New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
    }

    $timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
    $xlsxPath  = Join-Path $OutputDir ("{0}_{1}.xlsx" -f $WorkbookPrefix, $timestamp)
    $csvAll    = Join-Path $OutputDir ("{0}_All_{1}.csv" -f $WorkbookPrefix, $timestamp)
    $csvRisk   = Join-Path $OutputDir ("{0}_NUMA_Risk_{1}.csv" -f $WorkbookPrefix, $timestamp)
    $csvNF     = Join-Path $OutputDir ("{0}_NotFound_{1}.csv" -f $WorkbookPrefix, $timestamp)

    if (-not (Get-Module -ListAvailable -Name ActiveDirectory)) {
        throw "ActiveDirectory module not found. Install RSAT."
    }
    Import-Module ActiveDirectory -ErrorAction Stop

    if (-not (Get-Module -ListAvailable -Name VMware.PowerCLI)) {
        throw "VMware PowerCLI not found. Install-Module VMware.PowerCLI"
    }
    Import-Module VMware.VimAutomation.Core -ErrorAction Stop

    # Excel
    $script:HasImportExcel = $false
    if (Get-Module -ListAvailable -Name ImportExcel) {
        Import-Module ImportExcel -ErrorAction Stop
        $script:HasImportExcel = $true
    } else {
        try {
            Install-Module ImportExcel -Scope CurrentUser -Force -AllowClobber -ErrorAction Stop
            Import-Module ImportExcel -ErrorAction Stop
            $script:HasImportExcel = $true
        } catch {
            Write-Warning ("ImportExcel module not available: {0}. Will fall back to CSV exports only." -f $_.Exception.Message)
        }
    }

    # Optional: ignore VCSA cert prompts
    # Set-PowerCLIConfiguration -Scope User -InvalidCertificateAction Ignore -Confirm:$false | Out-Null
}

process {
    # 1) AD list
    $namePattern = "*$NameContains*"
    $adParams = @{
        Filter     = "Name -like '$namePattern' -and OperatingSystem -like '*Server*'"
        Properties = @('DNSHostName','OperatingSystem','OperatingSystemVersion','Enabled','WhenCreated')
    }
    if ($SearchBase) { $adParams['SearchBase'] = $SearchBase }

    $adServers = Get-ADComputer @adParams | Select-Object `
        @{N='ADName';E={$_.Name}},
        @{N='ADShortName';E={$_.Name.Split('.')[0]}},
        DNSHostName, OperatingSystem, OperatingSystemVersion, Enabled, WhenCreated

    if (-not $adServers) {
        Write-Host "No matching AD servers found for pattern '$NameContains'." -ForegroundColor Yellow
        return
    }

    # 2) vCenter connect + VM inventory
    $connections = @()
    $allVMs = @()

    foreach ($vc in $vCenters) {
        Write-Host "Connecting to $vc..." -ForegroundColor Cyan
        try {
            $conn = Connect-VIServer -Server $vc -WarningAction SilentlyContinue -ErrorAction Stop
            $connections += $conn
        }
        catch {
            Write-Warning ("Failed to connect to {0}: {1}" -f $vc, $_.Exception.Message)
            continue
        }

        try {
            $vms = Get-VM -Server $conn | Select-Object `
                @{N='VMName';E={$_.Name}},
                @{N='vCenter';E={$conn.Name}},
                PowerState,
                VMHost,
                @{N='Cluster';E={ $_.VMHost.Parent.Name }},
                NumCpu,
                CoresPerSocket,
                MemoryGB,
                @{N='CpuHotAddEnabled';E={$_.ExtensionData.Config.CpuHotAddEnabled}},
                @{N='MemoryHotAddEnabled';E={$_.ExtensionData.Config.MemoryHotAddEnabled}},
                @{N='MemoryReservationLockedToMax';E={$_.ExtensionData.Config.MemoryReservationLockedToMax}},
                @{N='GuestHostName';E={$_.Guest.HostName}},
                @{N='ToolsStatus';E={$_.Guest.ToolsStatus}}
            $allVMs += $vms
        }
        catch {
            Write-Warning ("Could not enumerate VMs on {0}: {1}" -f $conn.Name, $_.Exception.Message)
        }
    }

    if (-not $allVMs) {
        Write-Host "No VMs retrieved from the specified vCenters." -ForegroundColor Yellow
        return
    }

    # 3) Build VM index w/ NUMA + reservation
    $vmIndex = [System.Collections.Generic.List[object]]::new()
    foreach ($vm in $allVMs) {
        $vmShort     = $vm.VMName.Split('.')[0]
        $guestShort  = if ($vm.GuestHostName) { $vm.GuestHostName.Split('.')[0] } else { $null }
        $cpuHot      = [bool]$vm.CpuHotAddEnabled
        $memHot      = [bool]$vm.MemoryHotAddEnabled
        $memLocked   = [bool]$vm.MemoryReservationLockedToMax
        $numCpu      = [int]$vm.NumCpu
        $coresPer    = [int]$vm.CoresPerSocket
        $memGB       = [double]$vm.MemoryGB

        $mayBreakNuma = ($cpuHot -or $memHot) -and ($numCpu -ge $NumaVcpuThreshold)
        $reasonParts = @()
        if ($cpuHot) { $reasonParts += "CPU Hot Add=On" }
        if ($memHot) { $reasonParts += "Memory Hot Add=On" }
        $reasonParts += "vCPU=$numCpu (threshold=$NumaVcpuThreshold)"
        $reason = ($reasonParts -join '; ')

        $vmIndex.Add([PSCustomObject]@{
            vCenter                      = $vm.vCenter
            VMName                       = $vm.VMName
            VMShort                      = $vmShort
            GuestHostName                = $vm.GuestHostName
            GuestShort                   = $guestShort
            PowerState                   = $vm.PowerState
            VMHost                       = $vm.VMHost
            Cluster                      = $vm.Cluster
            NumCpu                       = $numCpu
            CoresPerSocket               = $coresPer
            MemoryGB                     = $memGB
            CpuHotAddEnabled             = $cpuHot
            MemoryHotAddEnabled          = $memHot
            MemoryReservationLockedToMax = $memLocked
            ToolsStatus                  = $vm.ToolsStatus
            NumaRisk                     = if ($mayBreakNuma) { 'Yes' } else { 'No' }
            NumaRiskReason               = $reason
        })
    }

    # 4) Match logic + guest power plan
    function Get-GuestPowerPlan {
        param(
            [string]$ComputerName,
            [pscredential]$Cred,
            [int]$TimeoutSec = 15
        )
        if (-not $Cred) { return "N/A" }
        if (-not $ComputerName) { return "N/A" }
        try {
            $sess = New-PSSession -ComputerName $ComputerName -Credential $Cred -UseSSL:$false -ErrorAction Stop
        } catch {
            return ("Unreachable")
        }
        try {
            $job = Invoke-Command -Session $sess -ScriptBlock {
                try {
                    $o = powercfg /GETACTIVESCHEME
                    if ($o -match '\((.+)\)') { $matches[1] } else { ($o -join ' ') }
                } catch {
                    "Error"
                }
            } -AsJob -ErrorAction Stop

            $done = $job | Wait-Job -Timeout $TimeoutSec
            if (-not $done) {
                Stop-Job $job -ErrorAction SilentlyContinue | Out-Null
                Remove-PSSession $sess
                return "Timeout"
            }
            $res = Receive-Job $job -ErrorAction SilentlyContinue
            Remove-PSSession $sess
            if ($null -eq $res -or $res -eq '') { return "Unknown" }
            return "$res"
        } catch {
            Remove-PSSession -Session $sess -ErrorAction SilentlyContinue
            return "Error"
        }
    }

    $report = foreach ($srv in $adServers) {
        $adShort = $srv.ADShortName

        # Exact short matches
        $matches = $vmIndex | Where-Object {
            ($_.'VMShort' -ieq $adShort) -or
            ($_.GuestShort -and ($_.GuestShort -ieq $adShort))
        }

        # Partial if no exact
        if (-not $matches) {
            $matches = $vmIndex | Where-Object {
                ($_.VMShort    -like "$adShort*") -or
                ($_.GuestShort -and ($_.GuestShort -like "$adShort*")) -or
                ($_.VMShort    -like "*$adShort*") -or
                ($_.GuestShort -and ($_.GuestShort -like "*$adShort*"))
            }
        }

        if ($matches) {
            foreach ($m in $matches) {
                # Choose best target for remoting: prefer AD DNS, then GuestHostName, then AD short
                $guestTarget = $srv.DNSHostName
                if (-not $guestTarget -and $m.GuestHostName) { $guestTarget = $m.GuestHostName }
                if (-not $guestTarget) { $guestTarget = $adShort }

                # Only try on Windows-y AD objects if creds supplied
                $powerPlan = "N/A"
                if ($GuestCredential -and ($srv.OperatingSystem -match 'Windows')) {
                    $powerPlan = Get-GuestPowerPlan -ComputerName $guestTarget -Cred $GuestCredential -TimeoutSec $PowerPlanTimeoutSec
                }

                [PSCustomObject]@{
                    AD_Name                         = $srv.ADName
                    AD_ShortName                    = $adShort
                    AD_DNSHostName                  = $srv.DNSHostName
                    AD_OperatingSystem              = $srv.OperatingSystem
                    AD_OperatingSystemVer           = $srv.OperatingSystemVersion
                    vCenter                         = $m.vCenter
                    VM_Name                         = $m.VMName
                    VM_Short                        = $m.VMShort
                    Guest_HostName                  = $m.GuestHostName
                    Guest_Short                     = $m.GuestShort
                    VM_PowerState                   = $m.PowerState
                    ESXi_Host                       = $m.VMHost
                    Cluster                         = $m.Cluster
                    NumCpu                          = $m.NumCpu
                    CoresPerSocket                  = $m.CoresPerSocket
                    MemoryGB                        = [math]::Round($m.MemoryGB,2)
                    CpuHotAdd                       = $m.CpuHotAddEnabled
                    MemoryHotAdd                    = $m.MemoryHotAddEnabled
                    MemoryReservationLockedToMax    = $m.MemoryReservationLockedToMax
                    NumaRisk                        = $m.NumaRisk
                    NumaRiskReason                  = $m.NumaRiskReason
                    ToolsStatus                     = $m.ToolsStatus
                    GuestPowerPlan                  = $powerPlan
                    MatchType                       = if ($m.VMShort -ieq $adShort -or ($m.GuestShort -and $m.GuestShort -ieq $adShort)) { 'ExactShort' } else { 'Partial' }
                }
            }
        }
        else {
            [PSCustomObject]@{
                AD_Name                         = $srv.ADName
                AD_ShortName                    = $adShort
                AD_DNSHostName                  = $srv.DNSHostName
                AD_OperatingSystem              = $srv.OperatingSystem
                AD_OperatingSystemVer           = $srv.OperatingSystemVersion
                vCenter                         = 'Not Found'
                VM_Name                         = $null
                VM_Short                        = $null
                Guest_HostName                  = $null
                Guest_Short                     = $null
                VM_PowerState                   = $null
                ESXi_Host                       = $null
                Cluster                         = $null
                NumCpu                          = $null
                CoresPerSocket                  = $null
                MemoryGB                        = $null
                CpuHotAdd                       = $null
                MemoryHotAdd                    = $null
                MemoryReservationLockedToMax    = $null
                NumaRisk                        = 'Unknown'
                NumaRiskReason                  = 'VM not found in vCenter scope'
                ToolsStatus                     = $null
                GuestPowerPlan                  = 'N/A'
                MatchType                       = 'NoMatch'
            }
        }
    }

    # 5) Split and summaries
    $allResults = $report | Sort-Object vCenter, AD_ShortName, MatchType
    $riskRows   = $allResults | Where-Object { $_.NumaRisk -eq 'Yes' }
    $notFound   = $allResults | Where-Object { $_.MatchType -eq 'NoMatch' }

    function Convert-IdxToColLetter([int]$n) { $s=""; while ($n -gt 0){$n--; $s=[char](65+($n%26))+$s; $n=[math]::Floor($n/26)}; $s }

    $riskByVCCluster = $riskRows |
        Group-Object vCenter, Cluster | ForEach-Object {
            [PSCustomObject]@{
                vCenter = $_.Group[0].vCenter
                Cluster = $_.Group[0].Cluster
                RiskCount = $_.Count
            }
        } | Sort-Object vCenter, Cluster

    $riskByVC = $riskRows |
        Group-Object vCenter | ForEach-Object {
            [PSCustomObject]@{
                vCenter = $_.Group[0].vCenter
                RiskCount = $_.Count
            }
        } | Sort-Object vCenter

    # 6) Excel export (whole-row colour rules), CSV fallback
    if ($script:HasImportExcel) {
        $pkg = $allResults | Export-Excel -Path $xlsxPath -WorksheetName 'AllResults' -TableName 'AllTbl' -AutoSize -FreezeTopRow -AutoFilter -BoldTopRow -ClearSheet -PassThru
        if ($riskRows) {
            $pkg = $riskRows   | Export-Excel -ExcelPackage $pkg -WorksheetName 'NUMA_Risk' -TableName 'RiskTbl' -AutoSize -FreezeTopRow -AutoFilter -BoldTopRow -ClearSheet -PassThru
        } else {
            ,@() | Export-Excel -ExcelPackage $pkg -WorksheetName 'NUMA_Risk' -TableName 'RiskTbl' -AutoSize -FreezeTopRow -AutoFilter -BoldTopRow -ClearSheet -PassThru | Out-Null
        }
        if ($notFound) {
            $pkg = $notFound   | Export-Excel -ExcelPackage $pkg -WorksheetName 'NotFound'  -TableName 'NFTbl'  -AutoSize -FreezeTopRow -AutoFilter -BoldTopRow -ClearSheet -PassThru
        } else {
            ,@() | Export-Excel -ExcelPackage $pkg -WorksheetName 'NotFound'  -TableName 'NFTbl'  -AutoSize -FreezeTopRow -AutoFilter -BoldTopRow -ClearSheet -PassThru | Out-Null
        }

        # Summary sheet
        $startRow = 1
        $pkg = $riskByVCCluster | Export-Excel -ExcelPackage $pkg -WorksheetName 'Summary' -TableName 'RiskByVCCluster' -StartRow $startRow -AutoSize -BoldTopRow -FreezeTopRow -ClearSheet -PassThru
        $startRow += ($riskByVCCluster.Count + 3); if ($startRow -lt 4) { $startRow = 4 }
        $pkg = $riskByVC      | Export-Excel -ExcelPackage $pkg -WorksheetName 'Summary' -TableName 'RiskByVC'       -StartRow $startRow -AutoSize -BoldTopRow -PassThru

        # Whole-row conditional formatting for AllResults & NUMA_Risk
        foreach ($wsName in @('AllResults','NUMA_Risk')) {
            $ws = $pkg.Workbook.Worksheets[$wsName]
            if (-not $ws -or -not $ws.Dimension) { continue }

            $lastRow = $ws.Dimension.End.Row
            $lastCol = $ws.Dimension.End.Column

            $headers = @{}
            for ($c=1; $c -le $lastCol; $c++) {
                $h = $ws.Cells[1, $c].Text
                if ($h) { $headers[$h] = Convert-IdxToColLetter $c }
            }

            if ($headers.ContainsKey('NumaRisk') -and $headers.ContainsKey('CpuHotAdd') -and
                $headers.ContainsKey('MemoryHotAdd') -and $headers.ContainsKey('MemoryReservationLockedToMax') -and
                $headers.ContainsKey('GuestPowerPlan')) {

                $rng = ("A2:{0}{1}" -f (Convert-IdxToColLetter $lastCol), $lastRow)

                $colNuma   = $headers['NumaRisk']
                $colCpu    = $headers['CpuHotAdd']
                $colMemAdd = $headers['MemoryHotAdd']
                $colMemRes = $headers['MemoryReservationLockedToMax']
                $colPlan   = $headers['GuestPowerPlan']

                # Yellow if memory reservation not locked
                $formulaYellow = ("${0}2=FALSE" -f $colMemRes)
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue $formulaYellow -BackgroundColor 'LightYellow'

                # Salmon if NUMA risk or hot add flags
                $formulaSalmon = ("OR(${0}2=""Yes"", ${1}2=TRUE, ${2}2=TRUE)" -f $colNuma,$colCpu,$colMemAdd)
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue $formulaSalmon -BackgroundColor 'LightSalmon'

                # Light Orange if power plan isn't High performance
                $formulaOrange = ("NOT(${0}2=""High performance"")" -f $colPlan)
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue $formulaOrange -BackgroundColor 'LightOrange'
            }
        }

        Close-ExcelPackage $pkg
        Write-Host ("Excel workbook written: {0}" -f $xlsxPath) -ForegroundColor Green
    }
    else {
        Write-Warning "ImportExcel unavailable. Writing CSVs instead."
        $allResults | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvAll
        $riskRows   | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvRisk
        $notFound   | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvNF
        Write-Host ("CSV files written:`n - {0}`n - {1}`n - {2}" -f $csvAll,$csvRisk,$csvNF) -ForegroundColor Yellow
    }

    if ($AlsoWriteCsv -and $script:HasImportExcel) {
        $allResults | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvAll
        $riskRows   | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvRisk
        $notFound   | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvNF
        Write-Host ("(Also wrote CSVs)`n - {0}`n - {1}`n - {2}" -f $csvAll,$csvRisk,$csvNF) -ForegroundColor DarkYellow
    }

    if ($connections.Count -gt 0) {
        Disconnect-VIServer -Server $connections -Confirm:$false | Out-Null
    }
}
