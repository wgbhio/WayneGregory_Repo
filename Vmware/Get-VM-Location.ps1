# ===========================
# VM placement + contention (last 7 days) with alarms, per-disk report, and HTML summary
# vCenter: ieprim018.bfl.local
# Output: C:\Users\GrWay\OneDrive\OneDrive - Beazley Group\Documents\Scripts\Reports
# ===========================

# -------- Optional vCenter connect --------
# Connect-VIServer -Server ieprim018.bfl.local

# -------- Settings --------
$vmNames = @(
    "USPRDB050.BFL.LOCAL",
    "USPRDB051.BFL.LOCAL",
    "USPRAP112.BFL.LOCAL",
    "USPRAP113.BFL.LOCAL",
    "USPRAP114.BFL.LOCAL",
    "USPRAP115.BFL.LOCAL",
    "USPRAP116.BFL.LOCAL",
    "USPRAP117.BFL.LOCAL",
    "USPRAP118.BFL.LOCAL",
    "USPRAP119.BFL.LOCAL",
    "USPRAP120.BFL.LOCAL",
    "USPRAP121.BFL.LOCAL"
)

$reportDir     = "C:\Users\GrWay\OneDrive\OneDrive - Beazley Group\Documents\Scripts\Reports"
$timestamp     = Get-Date -Format "yyyy-MM-dd_HHmm"
$lookbackMins  = 10080   # 7 days

# Thresholds (tune for your estate)
$th = [pscustomobject]@{
    CpuRdyWarn  = 5      # %
    CpuRdyCrit  = 10     # %
    CoStopWarn  = 3      # %
    DiskLatWarn = 25     # ms
    DiskLatCrit = 50     # ms
    BalloonWarn = 1      # KB
    SwapWarn    = 1      # KB/s
}

# Ensure output directory exists
$null = New-Item -ItemType Directory -Force -Path $reportDir

# -------- Helpers --------

# Robust VM lookup: FQDN -> short -> wildcard -> Guest.HostName
function Find-VMByInput {
    param([Parameter(Mandatory)] [string] $InputName)

    $fqdn  = $InputName
    $short = ($InputName -split '\.')[0]

    $method = $null
    $vm = Get-VM -Name $fqdn -ErrorAction SilentlyContinue
    if ($vm) { return @{ Vm=$vm; Method="Exact Name (FQDN)" } }

    $vm = Get-VM -Name $short -ErrorAction SilentlyContinue
    if ($vm) { return @{ Vm=$vm; Method="Exact Name (short)" } }

    $vm = Get-VM -Name "$short*" -ErrorAction SilentlyContinue
    if ($vm) {
        if ($vm.Count -gt 1) { $vm = $vm | Sort-Object Name | Select-Object -First 1; $method = " (first of multiple)" }
        return @{ Vm=$vm; Method="Wildcard Name (short*)$method" }
    }

    # Guest.HostName fallback (can be slower)
    $vm = Get-VM -ErrorAction SilentlyContinue | Where-Object {
        $_.ExtensionData.Guest -and (
            $_.ExtensionData.Guest.HostName -ieq $fqdn -or
            $_.ExtensionData.Guest.HostName -ilike "$short.*"
        )
    }
    if ($vm) {
        if ($vm.Count -gt 1) { $vm = $vm | Sort-Object Name | Select-Object -First 1; $method = " (first of multiple)" }
        return @{ Vm=$vm; Method="Guest.HostName match$method" }
    }

    return @{ Vm=$null; Method="No match" }
}

# Reset any previous versions to avoid parameter mismatch
Remove-Item Function:\Get-VmPerfSummary -ErrorAction SilentlyContinue

# Perf summary: supports -Minutes (default 7 days) OR -StartTime
function Get-VmPerfSummary {
    [CmdletBinding(DefaultParameterSetName='ByMinutes')]
    param(
        [Parameter(Mandatory=$true)]
        $Vm,

        [Parameter(ParameterSetName='ByMinutes')]
        [int] $Minutes = 10080,   # 7 days

        [Parameter(ParameterSetName='ByStart')]
        [datetime] $StartTime
    )

    if ($PSCmdlet.ParameterSetName -eq 'ByStart') {
        $start    = $StartTime
        $spanMins = [int]([datetime]::Now - $StartTime).TotalMinutes
    } else {
        $start    = (Get-Date).AddMinutes(-$Minutes)
        $spanMins = $Minutes
    }

    # Use historical rollups for >1h, realtime for short windows
    $interval = if ($spanMins -gt 60) { 300 } else { 20 }

    # ----- Pull stats -----
    # CPU summation counters (ms per interval)
    $rdy     = Get-Stat -Entity $Vm -Stat cpu.ready.summation    -Start $start -IntervalSecs $interval -ErrorAction SilentlyContinue
    $costop  = Get-Stat -Entity $Vm -Stat cpu.costop.summation    -Start $start -IntervalSecs $interval -ErrorAction SilentlyContinue

    # Memory averages
    $balloon = Get-Stat -Entity $Vm -Stat mem.vmmemctl.average    -Start $start -IntervalSecs $interval -ErrorAction SilentlyContinue
    $swapin  = Get-Stat -Entity $Vm -Stat mem.swapinRate.average  -Start $start -IntervalSecs $interval -ErrorAction SilentlyContinue
    $swapout = Get-Stat -Entity $Vm -Stat mem.swapoutRate.average -Start $start -IntervalSecs $interval -ErrorAction SilentlyContinue

    # Disk latency is per-virtual-disk instance → aggregate across all instances
    $rdSeries = Get-Stat -Entity $Vm -Stat virtualDisk.totalReadLatency.average  -Start $start -IntervalSecs $interval -Instance "*" -ErrorAction SilentlyContinue
    $wrSeries = Get-Stat -Entity $Vm -Stat virtualDisk.totalWriteLatency.average -Start $start -IntervalSecs $interval -Instance "*" -ErrorAction SilentlyContinue

    function Avg-PercentFromSummation {
        param($series, [int]$vcpus, [int]$intervalSeconds)
        if (-not $series -or $series.Count -eq 0 -or $vcpus -lt 1) { return $null }
        $intervalMs = $intervalSeconds * 1000
        $avgMs = ($series | Measure-Object -Property Value -Average | Select-Object -ExpandProperty Average)
        if (-not $avgMs) { return $null }
        [math]::Round(($avgMs / ($vcpus * $intervalMs)) * 100, 2)
    }

    function Avg-OfInstances {
        param($series)
        if (-not $series -or $series.Count -eq 0) { return $null }
        [math]::Round(($series | Measure-Object -Property Value -Average | Select-Object -ExpandProperty Average), 2)
    }

    $numCpu     = $Vm.NumCpu
    $cpuRdyPct  = Avg-PercentFromSummation $rdy    $numCpu $interval
    $coStopPct  = Avg-PercentFromSummation $costop $numCpu $interval
    $balloonKb  = if ($balloon) { [math]::Round(($balloon | Measure-Object -Property Value -Average).Average,0) } else { $null }
    $swapInKb   = if ($swapin)  { [math]::Round(($swapin  | Measure-Object -Property Value -Average).Average,2) } else { $null }
    $swapOutKb  = if ($swapout) { [math]::Round(($swapout | Measure-Object -Property Value -Average).Average,2) } else { $null }

    $rdLatMs    = Avg-OfInstances $rdSeries
    $wrLatMs    = Avg-OfInstances $wrSeries

    [pscustomobject]@{
        WindowStart  = $start
        IntervalSec  = $interval
        CpuRdyPct    = $cpuRdyPct
        CoStopPct    = $coStopPct
        BalloonKB    = $balloonKb
        SwapInKBs    = $swapInKb
        SwapOutKBs   = $swapOutKb
        ReadLatMs    = $rdLatMs
        WriteLatMs   = $wrLatMs
        DiskCounters = if ($rdSeries -or $wrSeries) { "OK" } else { "MISSING" }
    }
}

function Get-ActiveAlarmsText {
    param([Parameter(Mandatory)] $Vm)
    try {
        $view = Get-View -Id $Vm.Id -ErrorAction Stop
        $states = $view.TriggeredAlarmState
        if (-not $states -or $states.Count -eq 0) { return "" }
        $names = foreach ($s in $states) {
            try { (Get-View -Id $s.Alarm).Info.Name } catch { $null }
        }
        ($names | Where-Object { $_ }) -join ", "
    } catch { "" }
}

# -------- Main --------

$mainRows = @()
$perfRows = @()

foreach ($input in $vmNames) {
    $found = Find-VMByInput -InputName $input
    $vm     = $found.Vm
    $method = $found.Method

    if ($vm) {
        # Datastores where the VM lives
        $dsNames = try {
            ($vm | Get-Datastore -ErrorAction Stop | Select-Object -ExpandProperty Name | Sort-Object -Unique) -join ", "
        } catch { "UNKNOWN" }

        # Perf summary (7 days by default via -Minutes)
        $perf   = Get-VmPerfSummary -Vm $vm -Minutes $lookbackMins
        $alarms = Get-ActiveAlarmsText -Vm $vm

        # Build issues string based on thresholds
        $issues = @()
        if ($perf.CpuRdyPct -ne $null) {
            if ($perf.CpuRdyPct -ge $th.CpuRdyCrit) { $issues += "CPU RDY ${($perf.CpuRdyPct)}% (CRIT ≥ $($th.CpuRdyCrit)%)" }
            elseif ($perf.CpuRdyPct -ge $th.CpuRdyWarn) { $issues += "CPU RDY ${($perf.CpuRdyPct)}% (WARN ≥ $($th.CpuRdyWarn)%)" }
        }
        if ($perf.CoStopPct -ne $null -and $perf.CoStopPct -ge $th.CoStopWarn) {
            $issues += "Co-Stop ${($perf.CoStopPct)}% (WARN ≥ $($th.CoStopWarn)%)"
        }
        if ($perf.BalloonKB -ne $null -and $perf.BalloonKB -ge $th.BalloonWarn) {
            $issues += "Ballooning ${($perf.BalloonKB)}KB"
        }
        if (($perf.SwapInKBs -ne $null -and $perf.SwapInKBs -ge $th.SwapWarn) -or
            ($perf.SwapOutKBs -ne $null -and $perf.SwapOutKBs -ge $th.SwapWarn)) {
            $issues += "Swapping (in ${($perf.SwapInKBs)} KB/s, out ${($perf.SwapOutKBs)} KB/s)"
        }
        foreach ($lat in @(@{n="Read";v=$perf.ReadLatMs}, @{n="Write";v=$perf.WriteLatMs})) {
            if ($lat.v -ne $null) {
                if ($lat.v -ge $th.DiskLatCrit) { $issues += "Disk ${lat.n} Lat ${lat.v}ms (CRIT ≥ $($th.DiskLatCrit)ms)" }
                elseif ($lat.v -ge $th.DiskLatWarn) { $issues += "Disk ${lat.n} Lat ${lat.v}ms (WARN ≥ $($th.DiskLatWarn)ms)" }
            }
        }
        if ($perf.DiskCounters -eq "MISSING") {
            $issues += "Disk latency counters missing (check vCenter perf level ≥ 2 / stats retention)"
        }
        if ($alarms) {
            $issues += "Active Alarms: $alarms"
        }

        $mainRows += [pscustomobject]@{
            Input       = $input
            vCenterName = $vm.Name
            Host        = $vm.VMHost.Name
            Cluster     = $vm.VMHost.Parent.Name
            Datastores  = $dsNames
            PowerState  = $vm.PowerState
            Lookup      = $method
        }

        $perfRows += [pscustomobject]@{
            Input        = $input
            vCenterName  = $vm.Name
            WindowStart  = $perf.WindowStart
            IntervalSec  = $perf.IntervalSec
            CpuRdyPct    = $perf.CpuRdyPct
            CoStopPct    = $perf.CoStopPct
            BalloonKB    = $perf.BalloonKB
            SwapInKBs    = $perf.SwapInKBs
            SwapOutKBs   = $perf.SwapOutKBs
            ReadLatMs    = $perf.ReadLatMs
            WriteLatMs   = $perf.WriteLatMs
            ActiveAlarms = $alarms
            Issues       = ($issues -join "; ")
        }
    }
    else {
        $mainRows += [pscustomobject]@{
            Input       = $input
            vCenterName = $null
            Host        = "NOT FOUND"
            Cluster     = $null
            Datastores  = $null
            PowerState  = $null
            Lookup      = $method
        }
        $perfRows += [pscustomobject]@{
            Input        = $input
            vCenterName  = $null
            WindowStart  = (Get-Date).AddMinutes(-$lookbackMins)
            IntervalSec  = 300
            CpuRdyPct    = $null
            CoStopPct    = $null
            BalloonKB    = $null
            SwapInKBs    = $null
            SwapOutKBs   = $null
            ReadLatMs    = $null
            WriteLatMs   = $null
            ActiveAlarms = $null
            Issues       = "VM not found"
        }
    }
}

# -------- Export (inventory + contention) --------
$inventoryCsv  = Join-Path $reportDir ("vm-hosts_" + $timestamp + ".csv")
$contentionCsv = Join-Path $reportDir ("vm-hosts-contention_" + $timestamp + ".csv")

$mainRows | Export-Csv -NoTypeInformation -Path $inventoryCsv
$perfRows | Export-Csv -NoTypeInformation -Path $contentionCsv

Write-Host "Inventory report : $inventoryCsv"
Write-Host "Contention report: $contentionCsv"

# -------- Per-disk detail --------
$diskRows = foreach ($input in $vmNames) {
    $found = Find-VMByInput -InputName $input
    $vm = $found.Vm
    if ($vm) {
        foreach ($hd in Get-HardDisk -VM $vm -ErrorAction SilentlyContinue) {
            [pscustomobject]@{
                Input       = $input
                vCenterName = $vm.Name
                DiskName    = $hd.Name
                CapacityGB  = [math]::Round($hd.CapacityGB,2)
                Persistence = $hd.Persistence
                Thin        = $hd.StorageFormat
                FileName    = $hd.Filename
                Datastore   = $hd.Filename.Split(']')[0].TrimStart('[').Trim()
                Controller  = $hd.ExtensionData.ControllerKey
            }
        }
    }
}
$diskCsv = Join-Path $reportDir ("vm-hosts-disks_" + $timestamp + ".csv")
$diskRows | Export-Csv -NoTypeInformation -Path $diskCsv
Write-Host "Disk detail report : $diskCsv"

# -------- HTML summary (issues only) --------
$issuesOnly = $perfRows | Where-Object { $_.Issues }

if ($issuesOnly) {
    $htmlPath = Join-Path $reportDir ("vm-hosts-summary_" + $timestamp + ".html")
    $pre = @"
<h2 style='font-family:Segoe UI,Arial,sans-serif'>VMs with Issues (last 7 days)</h2>
<p style='font-family:Segoe UI,Arial,sans-serif'>Generated: $(Get-Date)</p>
"@
    $html = $issuesOnly |
        Select-Object Input,vCenterName,CpuRdyPct,CoStopPct,BalloonKB,SwapInKBs,SwapOutKBs,ReadLatMs,WriteLatMs,ActiveAlarms,Issues |
        ConvertTo-Html -Title "VM Contention/Alarm Summary" -PreContent $pre |
        Out-String
    Set-Content -Path $htmlPath -Value $html -Encoding UTF8
    Write-Host "HTML summary report: $htmlPath"

    # Auto-open in default browser
    try {
        Start-Process -FilePath $htmlPath -ErrorAction Stop
    } catch {
        Write-Warning "Couldn't auto-open HTML. Open manually: $htmlPath"
    }
} else {
    Write-Host "No issues detected across the last 7 days — no HTML summary created."
}
