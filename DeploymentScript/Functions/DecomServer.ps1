param(
    [Parameter(Mandatory=$false)]
    [switch]$Test = $true  # Default to TEST mode for safety
)

# ========== CONFIG ==========
$vCenterServer = @("ukprim098.bfl.local","usprim004.bfl.local","ieprim018.bfl.local")

$SCOMSvr   = "UKPRAP184"   # SCOM management server
$DnsServer = "ukprdc014"   # Hop for AD/DNS remoting
$DnsZone   = "bfl.local"
$InfraHop  = $DnsServer    # single remote hop for AD/DNS (same $Cred)

$Path = "C:\Users\GrWay\OneDrive - Beazley Group\Documents\Scripts\DeploymentScript\Logs\DecomLogs"

$DECOMServers = @"
usdvap094
UKDVAP238
UKDVAP237
UKPRAP202
UKPRAP304
UKPRAP215
UKDVAP128
UKPRDB095
UKDVAP081
UKPRAP216
"@ -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ }

# ========== PATHS & HELPERS ==========
function Ensure-Directory {
    param([Parameter(Mandatory=$true)][string]$PathToCreate)
    if (-not (Test-Path -LiteralPath $PathToCreate)) {
        New-Item -ItemType Directory -Path $PathToCreate -Force | Out-Null
    }
}

$timestamp  = Get-Date -Format "yyyyMMdd_HHmmss"
$reportsDir = Join-Path $Path "Reports"
Ensure-Directory $Path
Ensure-Directory $reportsDir

$outputPath = Join-Path $reportsDir "ServerStatus_Report_$timestamp-ALL.csv"
$logFile    = Join-Path $reportsDir ("log_{0}.log" -f (Get-Date -Format 'yyyyMMddHHmmss'))

function Log-Message {
    param ([string]$Message, [string]$Level = "INFO")
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $modePrefix = if ($Test) { "[TEST]" } else { "[ACTION]" }
    $entry = "$ts $modePrefix [$Level] $Message"
    Ensure-Directory (Split-Path -Parent $logFile)
    Add-Content -Path $logFile -Value $entry
    Write-Host $entry
}

function Get-ReversePTRInfo {
    param([Parameter(Mandatory=$true)][string]$IPv4)
    try {
        if (-not ($IPv4 -match '^\d{1,3}(\.\d{1,3}){3}$')) { return $null }
        $oct = $IPv4.Split('.')
        if ($oct.Count -ne 4) { return $null }
        # /24 reverse zone
        $zone = "{0}.{1}.{2}.in-addr.arpa" -f $oct[2], $oct[1], $oct[0]
        $name = $oct[3]
        return [PSCustomObject]@{ Zone=$zone; Name=$name }
    } catch { return $null }
}

function Get-ShortHost {
    param([string]$FQDN)
    if ([string]::IsNullOrWhiteSpace($FQDN)) { return $null }
    return $FQDN.Split('.')[0]
}

function Invoke-Remote {
    param(
        [Parameter(Mandatory=$true)][string]$ComputerName,
        [Parameter(Mandatory=$true)][ScriptBlock]$ScriptBlock,
        [Parameter()][object[]]$ArgumentList = @(),
        [Parameter()][switch]$StopOnError
    )
    try {
        return Invoke-Command -ComputerName $ComputerName -Credential $Cred -ScriptBlock $ScriptBlock -ArgumentList $ArgumentList -ErrorAction Stop
    } catch {
        if ($StopOnError) { throw } else { return $null }
    }
}

# ========== CONNECT ==========
$Cred = Get-Credential
try {
    Connect-VIServer -Server $vCenterServer -Credential $Cred -WarningAction SilentlyContinue | Out-Null
    Log-Message "Connected to vCenter servers: $($vCenterServer -join ', ')"
} catch {
    Log-Message "Failed to connect to vCenter: $_" "ERROR"
    throw
}

if ($Test) {
    Write-Host "=== RUNNING IN TEST MODE - NO CHANGES WILL BE MADE ===" -ForegroundColor Yellow -BackgroundColor Red
    Log-Message "Script started in TEST MODE - no changes will be made"
} else {
    Write-Host "=== RUNNING IN ACTION MODE - CHANGES WILL BE MADE ===" -ForegroundColor White -BackgroundColor Red
    $confirmation = Read-Host "Are you sure you want to proceed with making changes? (Type 'YES' to continue)"
    if ($confirmation -ne 'YES') {
        Write-Host "Operation cancelled by user" -ForegroundColor Yellow
        Log-Message "User cancelled ACTION mode start"
        Disconnect-VIServer -Server $vCenterServer -Confirm:$false | Out-Null
        return
    }
    Log-Message "Script started in ACTION MODE - changes will be made" "WARNING"
}

# ========== VCENTER: SHUTDOWN HELPER ==========
function Invoke-VMShutdownIfNeeded {
    param(
        [Parameter(Mandatory=$true)][string]$VMName,
        [Parameter(Mandatory=$true)][switch]$IsActionMode,
        [Parameter()][int]$GraceMinutes = 8
    )
    try {
        $vmObj = Get-VM -Name $VMName -ErrorAction Stop
    } catch {
        Log-Message "VM ${VMName} not found while attempting shutdown: $_" "ERROR"
        return $false
    }

    if ($vmObj.PowerState -eq "PoweredOff") {
        Log-Message "VM ${VMName} already PoweredOff; no shutdown needed."
        return $true
    }

    if (-not $IsActionMode) {
        Log-Message "TEST: Would initiate shutdown for ${VMName}"
        return $false
    }

    # Try graceful guest shutdown first
    $toolsOk = ($vmObj.ExtensionData.Guest.ToolsStatus -match "toolsOk|toolsOld|toolsSupported")
    if ($toolsOk) {
        try {
            Log-Message "Attempting graceful guest shutdown for ${VMName}"
            Shutdown-VMGuest -VM $vmObj -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
        } catch {
            Log-Message "Graceful shutdown call failed for ${VMName}: $_" "WARN"
        }
    } else {
        Log-Message "VMware Tools not healthy on ${VMName}; skipping guest shutdown"
    }

    # Wait for graceful shutdown
    $deadline = (Get-Date).AddMinutes($GraceMinutes)
    while ((Get-VM -Id $vmObj.Id).PowerState -ne "PoweredOff" -and (Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 10
    }

    $vmObj = Get-VM -Id $vmObj.Id
    if ($vmObj.PowerState -ne "PoweredOff") {
        Log-Message "Graceful shutdown timed out; forcing Stop-VM on ${VMName}"
        Stop-VM -VM $vmObj -Confirm:$false | Out-Null
        $deadline = (Get-Date).AddMinutes(2)
        while ((Get-VM -Id $vmObj.Id).PowerState -ne "PoweredOff" -and (Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 5
        }
        $vmObj = Get-VM -Id $vmObj.Id
    }

    if ($vmObj.PowerState -eq "PoweredOff") {
        try {
            $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
            $who = $Cred.UserName
            $newNoteLine = "Decom Shutdown: $ts by $who"
            $existing = $vmObj.Notes
            $updatedNotes = if ([string]::IsNullOrWhiteSpace($existing)) { $newNoteLine } else { "$existing`r`n$newNoteLine" }
            Set-VM -VM $vmObj -Notes $updatedNotes -Confirm:$false | Out-Null
            Log-Message "Appended shutdown timestamp to VM Notes for ${VMName}"
        } catch {
            Log-Message "Failed to update VM Notes for ${VMName}: $_" "WARN"
        }
        return $true
    } else {
        Log-Message "Failed to power off VM ${VMName}" "ERROR"
        return $false
    }
}


# ========== AD / DNS / SCOM (single-credential, remote on hop) ==========
function Invoke-ADQuery {
    param([Parameter(Mandatory=$true)][string]$Hostname)
    $sb = {
        param($HostToFind)
        Import-Module ActiveDirectory -ErrorAction Stop
        try { Get-ADComputer -Identity $HostToFind -Properties OperatingSystem,DNSHostName -ErrorAction Stop } catch { $null }
    }
    return Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($Hostname)
}
function Invoke-ADRemoveComputer {
    param([Parameter(Mandatory=$true)][object]$AdComputer)
    $sb = {
        param($comp)
        Import-Module ActiveDirectory -ErrorAction Stop
        Remove-ADComputer -Identity $comp -Confirm:$false
        "OK"
    }
    return Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($AdComputer)
}
function Invoke-ADGetWUGroups {
    param([Parameter(Mandatory=$true)][object]$AdComputer)
    $sb = {
        param($comp)
        Import-Module ActiveDirectory -ErrorAction Stop
        Get-ADPrincipalGroupMembership -Identity $comp | Where-Object { $_.Name -like "app.WindowsUpdate.*" }
    }
    return Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($AdComputer)
}
function Invoke-ADRemoveFromGroup {
    param([Parameter(Mandatory=$true)][object]$AdComputer,[Parameter(Mandatory=$true)][object]$Group,[switch]$WhatIf)
    $sb = {
        param($comp,$grp,$doWhatIf)
        Import-Module ActiveDirectory -ErrorAction Stop
        if ($doWhatIf) {
            Remove-ADGroupMember -Identity $grp -Members $comp -WhatIf
            "WHATIF"
        } else {
            Remove-ADGroupMember -Identity $grp -Members $comp -Confirm:$false
            "OK"
        }
    }
    return Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($AdComputer,$Group,[bool]$WhatIf)
}
function Invoke-DNSQuery {
    param([Parameter(Mandatory=$true)][string]$Zone,[Parameter(Mandatory=$true)][string]$Name,[Parameter(Mandatory=$true)][string]$RRType)
    $sb = {
        param($zone,$name,$rr)
        Import-Module DNSServer -ErrorAction Stop
        Get-DnsServerResourceRecord -ZoneName $zone -Name $name -RRType $rr -ErrorAction SilentlyContinue
    }
    return Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($Zone,$Name,$RRType)
}
function Invoke-DNSRemove {
    param([Parameter(Mandatory=$true)][string]$Zone,[Parameter(Mandatory=$true)][string]$Name,[Parameter(Mandatory=$true)][string]$RRType)
    $sb = {
        param($zone,$name,$rr)
        Import-Module DNSServer -ErrorAction Stop
        Remove-DnsServerResourceRecord -ZoneName $zone -Name $name -RRType $rr -Force
        "OK"
    }
    return Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($Zone,$Name,$RRType)
}
function Test-ServerDNSRecords {
    param(
        [Parameter(Mandatory=$true)][string]$ShortHost,
        [Parameter(Mandatory=$true)][string]$FQDN,
        [Parameter(Mandatory=$true)][string]$IPAddress,
        [Parameter(Mandatory=$true)][string]$DnsZone,
        [switch]$Test
    )
    $result = @{
        DNSStatus = ""
        ActionsNeeded = [System.Collections.ArrayList]@()
        Notes = ""
    }
    try {
        if ([string]::IsNullOrWhiteSpace($ShortHost)) { $ShortHost = $FQDN.Split('.')[0] }

        $aRecord   = Invoke-DNSQuery -Zone $DnsZone -Name $ShortHost -RRType A
        $ptrRecord = $null
        $ptrInfo   = $null
        if ($IPAddress -and $IPAddress -ne "Not Resolvable") {
            $ptrInfo = Get-ReversePTRInfo -IPv4 $IPAddress
            if ($ptrInfo) { $ptrRecord = Invoke-DNSQuery -Zone $ptrInfo.Zone -Name $ptrInfo.Name -RRType PTR }
        }

        if ($aRecord -or $ptrRecord) {
            Log-Message "DNS records found for $FQDN (A: $([bool]$aRecord), PTR: $([bool]$ptrRecord))"
            if ($Test) {
                $result.DNSStatus = "TEST: DNS records present - would remove"
                $result.ActionsNeeded.Add("TEST: Would remove DNS A/PTR for $FQDN")
            } else {
                try {
                    if ($aRecord) {
                        Invoke-DNSRemove -Zone $DnsZone -Name $ShortHost -RRType A | Out-Null
                        $result.ActionsNeeded.Add("ACTION: Removed DNS A for $FQDN")
                        Log-Message "Removed DNS A for $FQDN"
                    }
                    if ($ptrRecord) {
                        Invoke-DNSRemove -Zone $ptrInfo.Zone -Name $ptrInfo.Name -RRType PTR | Out-Null
                        $result.ActionsNeeded.Add("ACTION: Removed DNS PTR for $IPAddress")
                        Log-Message "Removed DNS PTR $($ptrInfo.Name).$($ptrInfo.Zone)"
                    }
                    $result.DNSStatus = "ACTION: DNS records removed"
                } catch {
                    $err = "ERROR: Failed to remove DNS records: $_"
                    Log-Message $err "ERROR"
                    $result.ActionsNeeded.Add($err)
                    $result.Notes += "`nDNS Removal Error: $_"
                }
            }
        } else {
            Log-Message "No DNS records found for $FQDN"
            $result.DNSStatus = "No DNS records found"
        }
    } catch {
        $err = "Error checking DNS records: $_"
        Log-Message $err "ERROR"
        $result.ActionsNeeded.Add($err)
        $result.Notes += "`nDNS Check Error: $_"
    }
    return $result
}
function Remove-WindowsUpdateGroups {
    param(
        [Parameter(Mandatory=$true)][object]$AdComputer,
        [switch]$Test
    )
    $result = @{
        IvantiStatus = ""
        ActionsNeeded = [System.Collections.ArrayList]@()
        Notes = ""
    }
    try {
        $groups = Invoke-ADGetWUGroups -AdComputer $AdComputer
        if ($groups) {
            $processed = @()
            foreach ($g in $groups) {
                Log-Message "$($AdComputer.Name) is member of $($g.Name)"
                try {
                    if ($Test) {
                        Invoke-ADRemoveFromGroup -AdComputer $AdComputer -Group $g -WhatIf | Out-Null
                        $processed += "TEST: Would remove from $($g.Name)"
                    } else {
                        Invoke-ADRemoveFromGroup -AdComputer $AdComputer -Group $g | Out-Null
                        $processed += "ACTION: Removed from $($g.Name)"
                    }
                } catch {
                    $err = "ERROR: Failed to remove from $($g.Name): $_"
                    Log-Message $err "ERROR"
                    $processed += $err
                    $result.Notes += "`nUpdate Group Removal Error: $_"
                }
            }
            $result.IvantiStatus = if ($Test) { "TEST: Would remove from groups" } else { "ACTION: Removed from groups" }
            $result.ActionsNeeded.Add(($processed -join ' | '))
        } else {
            Log-Message "No Windows Update groups found for $($AdComputer.Name)"
            $result.IvantiStatus = "No groups found"
        }
    } catch {
        $err = "Error checking Update Group membership: $_"
        Log-Message $err "ERROR"
        $result.IvantiStatus = "Check error"
        $result.ActionsNeeded.Add($err)
        $result.Notes += "`nUpdate Group Check Error: $_"
    }
    return $result
}
function Test-SCOMRegistration {
    param(
        [Parameter(Mandatory=$true)][string]$HostnameFQDN,
        [Parameter(Mandatory=$true)][string]$SCOMServer,
        [switch]$Test
    )
    $result = @{
        SCOMStatus = ""
        ActionsNeeded = [System.Collections.ArrayList]@()
        Notes = ""
    }
    if ([string]::IsNullOrWhiteSpace($HostnameFQDN)) {
        $result.SCOMStatus = "No FQDN available"
        Log-Message "No FQDN available for SCOM check"
        return $result
    }
    try {
        Log-Message "Checking SCOM registration for $HostnameFQDN on $SCOMServer"
        $scomStatus = Invoke-Remote -ComputerName $SCOMServer -ScriptBlock {
            param($serverFQDN, $doRemove)
            Import-Module OperationsManager -ErrorAction Stop
            $agent = Get-SCOMAgent -DNSHostName $serverFQDN -ErrorAction SilentlyContinue
            if ($agent) {
                if ($doRemove) {
                    try {
                        Remove-SCOMAgent -Agent $agent -Confirm:$false
                        return @{ IsRegistered = $true; Removed = $true; HealthState = $agent.HealthState }
                    } catch {
                        return @{ IsRegistered = $true; Removed = $false; Error = $_.Exception.Message }
                    }
                } else {
                    return @{ IsRegistered = $true; Removed = $false; HealthState = $agent.HealthState }
                }
            } else {
                return @{ IsRegistered = $false }
            }
        } -ArgumentList @($HostnameFQDN, (-not $Test)) -StopOnError

        if ($scomStatus -and $scomStatus.IsRegistered) {
            if ($scomStatus.Removed) {
                $result.SCOMStatus = "Removed from SCOM"
                $result.ActionsNeeded.Add("ACTION: Removed SCOM agent for $HostnameFQDN")
                Log-Message "ACTION: Removed SCOM agent for $HostnameFQDN"
            } elseif ($Test) {
                $result.SCOMStatus = "Registered - Would remove"
                $result.ActionsNeeded.Add("TEST: Would remove SCOM agent for $HostnameFQDN")
                Log-Message "TEST: Would remove SCOM agent for $HostnameFQDN"
            } elseif ($scomStatus.Error) {
                $result.SCOMStatus = "Removal failed"
                $result.Notes += "SCOM removal error: $($scomStatus.Error)"
                Log-Message "ERROR: Failed to remove SCOM agent for $HostnameFQDN : $($scomStatus.Error)" "ERROR"
            } else {
                $result.SCOMStatus = "Registered - Not removed"
            }
        } else {
            $result.SCOMStatus = "Not registered"
            Log-Message "SCOM agent not found for $HostnameFQDN"
        }
    } catch {
        $result.SCOMStatus = "Error checking SCOM"
        $result.Notes += "SCOM Check Error: $_"
        Log-Message "Error checking SCOM status for $HostnameFQDN : $_" "ERROR"
    }
    return $result
}

function Test-DecommissionStatus {
    param(
        [Parameter(Mandatory=$true)][string]$Hostname,
        [Parameter(Mandatory=$true)]$ConnectivityResult,
        [Parameter(Mandatory=$true)][string]$DnsZone,
        [Parameter(Mandatory=$true)][string]$vCenterPowerState,
        [switch]$Test
    )
    $result = @{
        DecommissionStatus = ""
        ActionsNeeded = [System.Collections.ArrayList]@()
        ADExists = $null
        OS = ""
        IPAddress = ""
        ADStatus = ""
        Notes = ""
        FQDN = ""
        DNSStatus = ""
    }
    try {
        $adObj = Invoke-ADQuery -Hostname $Hostname
        if ($adObj) {
            $result.ADExists  = $adObj
            $result.OS       = $adObj.OperatingSystem
            $result.ADStatus = "Exists in AD"
            $result.FQDN     = $adObj.DNSHostName
            $shortHost = if ($result.FQDN) { Get-ShortHost $result.FQDN } else { $Hostname }

            try {
                $nameForDns = if ($result.FQDN) { $result.FQDN } else { $Hostname }
                $ipObj = [System.Net.Dns]::GetHostAddresses($nameForDns) | Where-Object { $_.AddressFamily -eq 'InterNetwork' } | Select-Object -First 1
                $result.IPAddress = if ($ipObj) { $ipObj.IPAddressToString } else { "Not Resolvable" }
            } catch { $result.IPAddress = "Not Resolvable" }

            # Safety gate for destructive AD/DNS ops
            $pingSaysShutdown = ($ConnectivityResult.Status -eq "Shutdown")
            $vcIsPoweredOff   = ($vCenterPowerState -eq "PoweredOff")
            $safeToDecom      = ($pingSaysShutdown -and $vcIsPoweredOff)

            if ($safeToDecom) {
                $result.DecommissionStatus = "Ready for decommission"

                if ($Test) {
                    $result.ActionsNeeded.Add("TEST: Would remove AD computer account")
                } else {
                    try {
                        Invoke-ADRemoveComputer -AdComputer $adObj | Out-Null
                        $result.ADStatus = "Removed from AD"
                        $result.ActionsNeeded.Add("ACTION: AD computer account removed")
                    } catch {
                        $err = "ERROR: Failed to remove AD computer account: $_"
                        Log-Message $err "ERROR"
                        $result.ActionsNeeded.Add($err)
                        $result.Notes += "`nAD Removal Error: $_"
                    }
                }

                $dnsResult = Test-ServerDNSRecords -ShortHost $shortHost -FQDN $result.FQDN -IPAddress $result.IPAddress -DnsZone $DnsZone -Test:$Test
                $result.DNSStatus = $dnsResult.DNSStatus
                $result.ActionsNeeded.AddRange($dnsResult.ActionsNeeded)
                $result.Notes += $dnsResult.Notes
            } else {
                $why = @()
                if (-not $pingSaysShutdown) { $why += "responds to ping" }
                if (-not $vcIsPoweredOff)   { $why += "vCenter shows PoweredOn" }
                $reason = if ($why) { $why -join " & " } else { "unknown condition" }
                $result.DecommissionStatus = "Not safe to decommission ($reason)"
                $msg = if ($Test) { "TEST: Would wait until: no ping AND PoweredOff" } else { "ACTION: Must shut down VM and stop ping before removal" }
                $result.ActionsNeeded.Add($msg)
            }
        } else {
            $result.ADStatus = "Not found in AD"
            Log-Message "AD lookup failed for $Hostname" "WARN"
        }
    } catch {
        $result.ADStatus = "Error during AD step"
        Log-Message "AD step error for $Hostname : $_" "ERROR"
    }
    return $result
}

# ========== INVENTORY ==========
Log-Message "Getting VM inventory data"
$vmResults = @()

foreach ($token in $DECOMServers) {
    $matches = Get-VM | Where-Object { $_.Name -like "*$token*" }
    foreach ($m in $matches) {
        $vmResults += [PSCustomObject]@{
            Hostname  = $m.Name
            Datastore = [string]::Join(",", (Get-Datastore -Id $m.DatastoreIdList | Select-Object -ExpandProperty Name))
            PowerState= $m.PowerState.ToString()
            Notes     = $m.Notes
            OS        = try {
                $ad = Invoke-ADQuery -Hostname $m.Name
                if ($ad) { $ad.OperatingSystem } else { "Unable to retrieve OS" }
            } catch { "Unable to retrieve OS" }
        }
    }
}

$decomListPath = Join-Path $reportsDir ("DECOMList_{0}.csv" -f (Get-Date -Format "ddMMyyyy_hhmm"))
$vmResults | Export-Csv -Path $decomListPath -NoTypeInformation
Log-Message "Exported VM discovery list to $decomListPath"

Log-Message "Starting decommission process for $($vmResults.Count) servers"
$finalResults = @()

foreach ($vm in $vmResults) {
    $serverResult = [PSCustomObject]@{
        Hostname            = $vm.Hostname
        Datastore           = $vm.Datastore
        PowerState          = $vm.PowerState
        OS                  = $vm.OS
        Status              = ""
        IPAddress           = ""
        ADStatus            = ""
        DNSStatus           = ""
        IvantiStatus        = ""
        SCOMStatus          = ""
        DecommissionStatus  = ""
        ActionsNeeded       = ""
        Notes               = ""
        ProcessingErrors    = ""
        Mode                = if ($Test) { "TEST" } else { "ACTION" }
        ShutdownDate        = ""
        DaysSinceShutdown   = ""
        CanDeleteVMDisk     = ""
        FQDN                = ""
        VMNotes             = $vm.Notes
    }

    try {
        Log-Message "Processing server: $($vm.Hostname)"

        # === 1) SCOM FIRST ===
        # Query AD (non-destructive) to get FQDN for SCOM removal
        $adForFqdn = Invoke-ADQuery -Hostname $vm.Hostname
        if ($adForFqdn -and $adForFqdn.DNSHostName) {
            $scomResult = Test-SCOMRegistration -HostnameFQDN $adForFqdn.DNSHostName -SCOMServer $SCOMSvr -Test:$Test
            $serverResult.SCOMStatus = $scomResult.SCOMStatus
            if ($scomResult.Notes) { $serverResult.Notes += $scomResult.Notes + "`n" }
            if ($scomResult.ActionsNeeded) {
                $serverResult.ActionsNeeded += (($scomResult.ActionsNeeded | Where-Object {$_}) -join " | ") + " | "
            }
        } else {
            $serverResult.SCOMStatus = "No FQDN in AD"
            Log-Message "No FQDN found in AD for $($vm.Hostname) - skipping SCOM check"
        }

        # === 2) SHUTDOWN VM SECOND ===
        if (-not $Test -and $vm.PowerState -ne "PoweredOff") {
            $shutdownOk = Invoke-VMShutdownIfNeeded -VMName $vm.Hostname -IsActionMode
            # refresh power state
            $refreshed = Get-VM -Name $vm.Hostname -ErrorAction SilentlyContinue
            if ($refreshed) { $vm.PowerState = $refreshed.PowerState.ToString() }
        } elseif ($Test -and $vm.PowerState -ne "PoweredOff") {
            Log-Message "TEST: Would shut down VM $($vm.Hostname) and append timestamp to Notes"
        }

        # === 3) PING + SHUTDOWN AGE ===
        $connectivityResult = Test-ServerConnectivity -Hostname $vm.Hostname
        $serverResult.Status = $connectivityResult.Status

        if ($vm.PowerState -eq "PoweredOff") {
            $shutdownInfo = Get-VMShutdownAge -VMName $vm.Hostname
            $serverResult.ShutdownDate = if ($shutdownInfo.ShutdownDate) { $shutdownInfo.ShutdownDate.ToString("yyyy-MM-dd HH:mm:ss") } else { "Unknown" }
            $serverResult.DaysSinceShutdown = if ($shutdownInfo.DaysSinceShutdown) { $shutdownInfo.DaysSinceShutdown } else { "Unknown" }
            if ($shutdownInfo.DaysSinceShutdown -ne $null -and $shutdownInfo.DaysSinceShutdown -gt 30) {
                $serverResult.CanDeleteVMDisk = "Yes"
                if ($Test) { $serverResult.ActionsNeeded += "TEST: VM disk can be deleted (shutdown > 30 days ago) | " }
                else       { $serverResult.ActionsNeeded += "ACTION: VM disk can be deleted (shutdown > 30 days ago) | " }
            } else {
                $serverResult.CanDeleteVMDisk = "No"
            }
            if ($shutdownInfo.Error) { $serverResult.Notes += "VM Shutdown Check Error: $($shutdownInfo.Error)`n" }
        } else {
            $serverResult.CanDeleteVMDisk = "No - VM Still Running"
            $serverResult.ShutdownDate = "N/A - VM Running"
            $serverResult.DaysSinceShutdown = "N/A"
        }

        # === 4) AD/DNS DECOM (safety gate: PoweredOff + no ping) ===
        $decomResult = Test-DecommissionStatus -Hostname $vm.Hostname -ConnectivityResult $connectivityResult -DnsZone $DnsZone -vCenterPowerState $vm.PowerState -Test:$Test
        $serverResult.ADStatus = $decomResult.ADStatus
        $serverResult.DecommissionStatus = $decomResult.DecommissionStatus
        $serverResult.IPAddress = $decomResult.IPAddress
        $serverResult.DNSStatus = $decomResult.DNSStatus
        $serverResult.FQDN = $decomResult.FQDN

        if ($decomResult.ADExists) {
            # === 5) WINDOWS UPDATE GROUPS ===
            $updateGroupResult = Remove-WindowsUpdateGroups -AdComputer $decomResult.ADExists -Test:$Test
            $serverResult.IvantiStatus = $updateGroupResult.IvantiStatus

            # Aggregate actions and notes
            $allActions = @()
            $allActions += $decomResult.ActionsNeeded
            $allActions += $updateGroupResult.ActionsNeeded
            $serverResult.ActionsNeeded += ($allActions | Where-Object { $_ }) -join " | "

            $allNotes = @()
            if ($decomResult.Notes) { $allNotes += $decomResult.Notes }
            if ($updateGroupResult.Notes) { $allNotes += $updateGroupResult.Notes }
            $serverResult.Notes += ($allNotes | Where-Object { $_ }) -join "`n"
        } else {
            $serverResult.ActionsNeeded += "Server not found in AD | "
            if (-not $serverResult.SCOMStatus) { $serverResult.SCOMStatus = "Cannot check - not in AD" }
        }

        $serverResult.ActionsNeeded = $serverResult.ActionsNeeded.TrimEnd(" | ")
    } catch {
        $serverResult.ProcessingErrors = "Error processing server: $_"
        Log-Message "Error processing $($vm.Hostname) : $_" "ERROR"
    }

    $finalResults += $serverResult
    Log-Message "Completed processing: $($vm.Hostname)"
}

# ========== EXPORT & SUMMARY ==========
$finalResults | Export-Csv -Path $outputPath -NoTypeInformation
Log-Message "Results exported to: $outputPath"

$modeText = if ($Test) { "TEST MODE" } else { "ACTION MODE" }
Write-Host "`n=== DECOMMISSION SUMMARY ($modeText) ===" -ForegroundColor Cyan
Write-Host "Total servers processed: $($finalResults.Count)" -ForegroundColor Green
Write-Host "Servers shutdown (no ping): $(($finalResults | Where-Object {$_.Status -eq 'Shutdown'}).Count)" -ForegroundColor Yellow
Write-Host "Servers still running (ping OK): $(($finalResults | Where-Object {$_.Status -eq 'Running'}).Count)" -ForegroundColor Red
Write-Host "Servers in AD: $(($finalResults | Where-Object {$_.ADStatus -eq 'Exists in AD'}).Count)" -ForegroundColor Magenta
Write-Host "VMs eligible for disk deletion: $(($finalResults | Where-Object {$_.CanDeleteVMDisk -eq 'Yes'}).Count)" -ForegroundColor Cyan
Write-Host "SCOM agents removed: $(($finalResults | Where-Object {$_.SCOMStatus -eq 'Removed from SCOM'}).Count)" -ForegroundColor Green

if ($Test) {
    Write-Host "`nTo run in ACTION mode, use: -Test:`$false" -ForegroundColor Yellow
}

Log-Message "Decommission process completed in $modeText"
Disconnect-VIServer -Server $vCenterServer -Confirm:$false | Out-Null
Log-Message "Disconnected from vCenter servers"
