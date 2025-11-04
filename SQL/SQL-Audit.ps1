<#
.SYNOPSIS
  AD "DB" servers → vCenter match (partial), NUMA risk + Memory Reservation checks,
  guest power plan via Invoke-Command, SCSI controller checks, SQL Edition discovery,
  and Excel export with row highlighting + Summary.

.REQUIREMENTS
  - RSAT / ActiveDirectory module
  - VMware PowerCLI (Install-Module VMware.PowerCLI)
  - ImportExcel module (auto-installs if missing; falls back to CSV)
  - WinRM enabled on Windows guests you want to query; valid credentials

.SHEETS
  - AllResults
  - NUMA_Risk
  - NotFound
  - Summary (RiskByVCCluster, RiskByVC,
             PvScsiNonCompliantByVCCluster, PvScsiNonCompliantByVC,
             SqlEnterpriseByVCCluster, SqlEnterpriseByVC)

.ROW COLOUR RULES
  - Salmon: NumaRisk=Yes OR CpuHotAdd=True OR MemoryHotAdd=True
  - Yellow: MemoryReservationLockedToMax=False
  - LightOrange: GuestPowerPlan != "High performance"
  - LightPink: ToolsStatus != "toolsOk"
  - LightBlue: PvScsiCompliant = "No" (not exactly 4 PVSCSI controllers)
  - Orange: SqlEnterprisePresent = "Yes"
#>

[CmdletBinding()]
param(
    [string]$SearchBase,
    [string]$NameContains = 'DB',
    [string[]]$vCenters = @('ukprim098.bfl.local','usprim004.bfl.local','ieprim018.bfl.local'),

    # NUMA heuristic
    [int]$NumaVcpuThreshold = 8,

    # Output controls
    [string]$OutputDir = 'C:\Users\GrWay\OneDrive\OneDrive - Beazley Group\Documents\Scripts\SQL',
    [string]$WorkbookPrefix = 'DBServers_vs_vCenter',
    [switch]$AlsoWriteCsv,

    # Guest query
    [pscredential]$GuestCredential,
    [int]$PowerPlanTimeoutSec = 15,
    [int]$SqlTimeoutSec = 20
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

    # Prompt ONCE for guest creds if not supplied
    if (-not $GuestCredential) {
        try {
            $GuestCredential = Get-Credential -Message "Enter guest OS credentials (used for all VMs to run powercfg & read SQL edition)"
        } catch {
            Write-Warning "No credentials provided, guest checks will be skipped."
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
                @{N='ToolsStatus';E={ $_.ExtensionData.Guest.ToolsStatus }},
                @{N='ToolsVersion';E={ $_.ExtensionData.Guest.ToolsVersionStatus2 }},
                # --- SCSI controller fields ---
                @{N='TotalScsiControllers';E={ (Get-ScsiController -VM $_ -Server $conn).Count }},
                @{N='PvScsiCount';E={ (Get-ScsiController -VM $_ -Server $conn | Where-Object Type -eq 'ParaVirtual').Count }},
                @{N='ScsiTypes';E={ (Get-ScsiController -VM $_ -Server $conn | Select-Object -ExpandProperty Type | Sort-Object | Get-Unique) -join ',' }}
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

    # 3) Build VM index w/ NUMA + reservation + SCSI
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

        $pvCount = [int]$vm.PvScsiCount
        $pvOk    = if ($pvCount -eq 4) { 'Yes' } else { 'No' }

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
            ToolsVersion                 = $vm.ToolsVersion
            # SCSI
            TotalScsiControllers         = $vm.TotalScsiControllers
            PvScsiCount                  = $pvCount
            ScsiTypes                    = $vm.ScsiTypes
            PvScsiCompliant              = $pvOk
            # NUMA
            NumaRisk                     = if ($mayBreakNuma) { 'Yes' } else { 'No' }
            NumaRiskReason               = $reason
        })
    }

    # 4) Guest checks: power plan + SQL editions
    function Get-GuestPowerPlan {
        param(
            [string]$ComputerName,
            [pscredential]$Cred,
            [int]$TimeoutSec = 15
        )
        if (-not $Cred -or -not $ComputerName) { return "N/A" }
        if (-not (Test-WSMan -ComputerName $ComputerName -ErrorAction SilentlyContinue)) { return "Unreachable" }

        try {
            $job = Invoke-Command -ComputerName $ComputerName -Credential $Cred -ScriptBlock {
                try {
                    $o = powercfg /GETACTIVESCHEME
                    if ($o -match '\((.+)\)') { $matches[1] } else { ($o -join ' ') }
                } catch { "Error" }
            } -AsJob -ErrorAction Stop

            if (-not ($job | Wait-Job -Timeout $TimeoutSec)) { Stop-Job $job -ErrorAction SilentlyContinue | Out-Null; return "Timeout" }
            $res = Receive-Job $job -ErrorAction SilentlyContinue
            if ($null -eq $res -or $res -eq '') { return "Unknown" }
            return "$res"
        } catch { return "Error" }
    }

    function Get-GuestSqlInfo {
        param(
            [string]$ComputerName,
            [pscredential]$Cred,
            [int]$TimeoutSec = 20
        )
        if (-not $Cred -or -not $ComputerName) { return @() }
        if (-not (Test-WSMan -ComputerName $ComputerName -ErrorAction SilentlyContinue)) { return "Unreachable" }

        $sb = {
            try {
                $instancesKey = 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\Instance Names\SQL'
                if (-not (Test-Path $instancesKey)) { return @() }   # no SQL
                $names = Get-ItemProperty -Path $instancesKey | Select-Object -ExpandProperty PSObject -ErrorAction SilentlyContinue
                # convert property bag to name->instanceID
                $props = (Get-ItemProperty -Path $instancesKey).psobject.Properties |
                         Where-Object { $_.MemberType -eq 'NoteProperty' }

                $out = @()
                foreach ($p in $props) {
                    $instName = $p.Name                   # e.g. MSSQLSERVER or SQL2019
                    $instId   = $p.Value                  # e.g. MSSQL15.MSSQLSERVER
                    $setupKey = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\${instId}\Setup"
                    $ed = $null; $ver = $null
                    if (Test-Path $setupKey) {
                        $ip = Get-ItemProperty -Path $setupKey -ErrorAction SilentlyContinue
                        $ed = $ip.Edition
                        $ver = $ip.Version
                    }
                    $out += [PSCustomObject]@{
                        InstanceName = $instName
                        Edition      = $ed
                        Version      = $ver
                    }
                }
                return $out
            } catch {
                return "Error"
            }
        }

        try {
            $job = Invoke-Command -ComputerName $ComputerName -Credential $Cred -ScriptBlock $sb -AsJob -ErrorAction Stop
            if (-not ($job | Wait-Job -Timeout $TimeoutSec)) { Stop-Job $job -ErrorAction SilentlyContinue | Out-Null; return "Timeout" }
            $res = Receive-Job $job -ErrorAction SilentlyContinue
            return $res
        } catch { return "Error" }
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

                # Guest power plan
                $powerPlan = "N/A"
                if ($GuestCredential -and ($srv.OperatingSystem -match 'Windows')) {
                    $powerPlan = Get-GuestPowerPlan -ComputerName $guestTarget -Cred $GuestCredential -TimeoutSec $PowerPlanTimeoutSec
                }

                # Guest SQL editions
                $sqlInstanceCount = $null
                $sqlInstances     = $null
                $sqlEditions      = $null
                $sqlEnterprise    = 'Unknown'

                if ($GuestCredential -and ($srv.OperatingSystem -match 'Windows')) {
                    $sqlInfo = Get-GuestSqlInfo -ComputerName $guestTarget -Cred $GuestCredential -TimeoutSec $SqlTimeoutSec

                    if ($sqlInfo -is [string]) {
                        # "Unreachable"/"Timeout"/"Error"
                        $sqlInstances  = $sqlInfo
                        $sqlEditions   = $sqlInfo
                        $sqlEnterprise = 'Unknown'
                    } elseif ($sqlInfo -is [array] -and $sqlInfo.Count -gt 0) {
                        $sqlInstanceCount = $sqlInfo.Count
                        $sqlInstances     = ($sqlInfo | ForEach-Object { $_.InstanceName }) -join ','
                        $sqlEditions      = ($sqlInfo | ForEach-Object { "$($_.InstanceName): $($_.Edition)" }) -join '; '
                        $sqlEnterprise    = if ($sqlInfo | Where-Object { $_.Edition -match 'Enterprise' }) { 'Yes' } else { 'No' }
                    } else {
                        # No SQL found
                        $sqlInstanceCount = 0
                        $sqlInstances     = 'None'
                        $sqlEditions      = 'None'
                        $sqlEnterprise    = 'No'
                    }
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
                    ToolsVersion                    = $m.ToolsVersion
                    GuestPowerPlan                  = $powerPlan
                    # SCSI
                    TotalScsiControllers            = $m.TotalScsiControllers
                    PvScsiCount                     = $m.PvScsiCount
                    ScsiTypes                       = $m.ScsiTypes
                    PvScsiCompliant                 = $m.PvScsiCompliant
                    # SQL
                    SqlInstanceCount                = $sqlInstanceCount
                    SqlInstances                    = $sqlInstances
                    SqlEditions                     = $sqlEditions
                    SqlEnterprisePresent            = $sqlEnterprise
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
                ToolsVersion                    = $null
                GuestPowerPlan                  = 'N/A'
                TotalScsiControllers            = $null
                PvScsiCount                     = $null
                ScsiTypes                       = $null
                PvScsiCompliant                 = 'Unknown'
                SqlInstanceCount                = $null
                SqlInstances                    = 'N/A'
                SqlEditions                     = 'N/A'
                SqlEnterprisePresent            = 'Unknown'
                MatchType                       = 'NoMatch'
            }
        }
    }

    # 5) Split and summaries
    $allResults = $report | Sort-Object vCenter, AD_ShortName, MatchType
    $riskRows   = $allResults | Where-Object { $_.NumaRisk -eq 'Yes' }
    $notFound   = $allResults | Where-Object { $_.MatchType -eq 'NoMatch' }

    function Convert-IdxToColLetter([int]$n) { $s=""; while ($n -gt 0){$n--; $s=[char](65+($n%26))+$s; $n=[math]::Floor($n/26)}; $s }

    # Existing summaries
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

    $pvBad = $allResults | Where-Object { $_.PvScsiCompliant -eq 'No' }
    $pvBadByVCCluster = $pvBad |
        Group-Object vCenter, Cluster | ForEach-Object {
            [PSCustomObject]@{
                vCenter = $_.Group[0].vCenter
                Cluster = $_.Group[0].Cluster
                PvScsiNonCompliant = $_.Count
            }
        } | Sort-Object vCenter, Cluster
    $pvBadByVC = $pvBad |
        Group-Object vCenter | ForEach-Object {
            [PSCustomObject]@{
                vCenter = $_.Group[0].vCenter
                PvScsiNonCompliant = $_.Count
            }
        } | Sort-Object vCenter

    # NEW: SQL Enterprise-present summaries
    $sqlEnt = $allResults | Where-Object { $_.SqlEnterprisePresent -eq 'Yes' }
    $sqlEntByVCCluster = $sqlEnt |
        Group-Object vCenter, Cluster | ForEach-Object {
            [PSCustomObject]@{
                vCenter = $_.Group[0].vCenter
                Cluster = $_.Group[0].Cluster
                SqlEnterpriseVMs = $_.Count
            }
        } | Sort-Object vCenter, Cluster
    $sqlEntByVC = $sqlEnt |
        Group-Object vCenter | ForEach-Object {
            [PSCustomObject]@{
                vCenter = $_.Group[0].vCenter
                SqlEnterpriseVMs = $_.Count
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

        # Summary sheet (existing sections + new SQL sections)
        $startRow = 1
        $pkg = $riskByVCCluster | Export-Excel -ExcelPackage $pkg -WorksheetName 'Summary' -TableName 'RiskByVCCluster' -StartRow $startRow -AutoSize -BoldTopRow -FreezeTopRow -ClearSheet -PassThru
        $startRow += ($riskByVCCluster.Count + 3); if ($startRow -lt 4) { $startRow = 4 }
        $pkg = $riskByVC        | Export-Excel -ExcelPackage $pkg -WorksheetName 'Summary' -TableName 'RiskByVC' -StartRow $startRow -AutoSize -BoldTopRow -PassThru
        $startRow += ($riskByVC.Count + 3); if ($startRow -lt 8) { $startRow = 8 }

        $pkg = $pvBadByVCCluster | Export-Excel -ExcelPackage $pkg -WorksheetName 'Summary' -TableName 'PvScsiNonCompliantByVCCluster' -StartRow $startRow -AutoSize -BoldTopRow -PassThru
        $startRow += ($pvBadByVCCluster.Count + 3)
        $pkg = $pvBadByVC        | Export-Excel -ExcelPackage $pkg -WorksheetName 'Summary' -TableName 'PvScsiNonCompliantByVC'       -StartRow $startRow -AutoSize -BoldTopRow -PassThru
        $startRow += ($pvBadByVC.Count + 3)

        # New SQL Enterprise summaries appended
        $pkg = $sqlEntByVCCluster | Export-Excel -ExcelPackage $pkg -WorksheetName 'Summary' -TableName 'SqlEnterpriseByVCCluster' -StartRow $startRow -AutoSize -BoldTopRow -PassThru
        $startRow += ($sqlEntByVCCluster.Count + 3)
        $pkg = $sqlEntByVC        | Export-Excel -ExcelPackage $pkg -WorksheetName 'Summary' -TableName 'SqlEnterpriseByVC'       -StartRow $startRow -AutoSize -BoldTopRow -PassThru

        # Whole-row conditional formatting for AllResults & NUMA_Risk
        foreach ($wsName in @('AllResults','NUMA_Risk')) {
            $ws = $pkg.Workbook.Worksheets[$wsName]
            if (-not $ws -or -not $ws.Dimension) { continue }

            $lastRow = $ws.Dimension.End.Row
            $lastCol = $ws.Dimension.End.Column

            $headers = @{}
            for ($c=1; $c -le $lastCol; $c++) { $h = $ws.Cells[1, $c].Text; if ($h) { $headers[$h] = Convert-IdxToColLetter $c } }

            if ($headers.ContainsKey('NumaRisk') -and $headers.ContainsKey('CpuHotAdd') -and
                $headers.ContainsKey('MemoryHotAdd') -and $headers.ContainsKey('MemoryReservationLockedToMax') -and
                $headers.ContainsKey('GuestPowerPlan') -and $headers.ContainsKey('ToolsStatus') -and
                $headers.ContainsKey('PvScsiCompliant') -and $headers.ContainsKey('SqlEnterprisePresent')) {

                $rng = ("A2:{0}{1}" -f (Convert-IdxToColLetter $lastCol), $lastRow)

                $colNuma   = $headers['NumaRisk']
                $colCpu    = $headers['CpuHotAdd']
                $colMemAdd = $headers['MemoryHotAdd']
                $colMemRes = $headers['MemoryReservationLockedToMax']
                $colPlan   = $headers['GuestPowerPlan']
                $colTools  = $headers['ToolsStatus']
                $colPvOk   = $headers['PvScsiCompliant']
                $colSqlEnt = $headers['SqlEnterprisePresent']

                # Yellow if memory reservation not locked
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue ("${0}2=FALSE" -f $colMemRes) -BackgroundColor 'LightYellow'

                # Salmon if NUMA risk or hot add flags
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue ("OR(${0}2=""Yes"", ${1}2=TRUE, ${2}2=TRUE)" -f $colNuma,$colCpu,$colMemAdd) -BackgroundColor 'LightSalmon'

                # Light Orange if power plan isn't High performance
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue ("NOT(${0}2=""High performance"")" -f $colPlan) -BackgroundColor 'LightOrange'

                # Light Pink if VMware Tools not OK
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue ("NOT(${0}2=""toolsOk"")" -f $colTools) -BackgroundColor 'LightPink'

                # Light Blue if PVSCSI not exactly 4
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue ("${0}2=""No""" -f $colPvOk) -BackgroundColor 'LightBlue'

                # Orange if any SQL Enterprise instance present
                Add-ConditionalFormatting -WorkSheet $ws -Range $rng -RuleType Expression -ConditionValue ("${0}2=""Yes""" -f $colSqlEnt) -BackgroundColor 'Orange'
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
