param(
    [Parameter(Mandatory = $false)]
    [bool]$Test = $true  # default to TEST
)

# ========== CONFIG ==========
$vCenterServer = @('ukprim098.bfl.local','usprim004.bfl.local','ieprim018.bfl.local')
$SCOMSvr   = 'UKPRAP184'        # SCOM management server
$DnsServer = 'ukprdc014'        # Hop for AD/DNS remoting
$DnsZone   = 'bfl.local'
$InfraHop  = $DnsServer
# Windows Update (Ivanti) group handling
$WUExceptionGroupName = 'app.WindowsUpdate.GBL.ExceptionServers'

$RootPath  = 'C:\Users\GrWay\OneDrive - Beazley Group\Documents\Scripts\DeploymentScript\Logs\DecomLogs'

# Change list mapping (deduplicated)
$ChangeItems = @(
    @{ Change='CHG0091029'; Host='UKDVAP081' }
    @{ Change='CHG0090713'; Host='UKPRAP216' }
    @{ Change='CHG0091964'; Host='USDVAP119' }
    @{ Change='CHG0091018'; Host='UKDVAP128' }
    @{ Change='CHG0091312'; Host='UKPRAP080' }
    @{ Change='CHG0091958'; Host='USDVAP118' }
    @{ Change='CHG0091949'; Host='USDVAP111' }
    @{ Change='CHG0091955'; Host='USDVAP112' }
    @{ Change='CHG0091962'; Host='USDVAP116' }
    @{ Change='CHG0090714'; Host='UKPRDB095' }
    @{ Change='CHG0091959'; Host='USDVAP114' }
    @{ Change='CHG0091960'; Host='USDVAP115' }
    @{ Change='CHG0090710'; Host='UKPRAP215' }
    @{ Change='CHG0091953'; Host='USDVAP110' }
    @{ Change='CHG0091963'; Host='USDVAP117' }
    @{ Change='CHG0091957'; Host='USDVAP113' }
    @{ Change='CHG0092022'; Host='USDVAP084' }
    @{ Change='CHG0092021'; Host='USDVAP083' }
    @{ Change='CHG0092019'; Host='USDVAP086' }
    @{ Change='CHG0092018'; Host='USDVAP085' }
    @{ Change='CHG0092017'; Host='USDVCM002' }
    @{ Change='CHG0092016'; Host='USDVCM001' }
)

# ========== PATHS ==========
function Ensure-Directory([string]$PathToCreate) {
    if (-not (Test-Path -LiteralPath $PathToCreate)) {
        New-Item -ItemType Directory -Path $PathToCreate -Force | Out-Null
    }
}
$timestamp  = Get-Date -Format 'yyyyMMdd_HHmmss'
$ReportsDir = Join-Path $RootPath 'Reports'
Ensure-Directory $RootPath
Ensure-Directory $ReportsDir

$CsvDiscovery   = Join-Path $ReportsDir ("DECOMList_{0}.csv" -f (Get-Date -Format 'ddMMyyyy_HHmm'))
$CsvResults     = Join-Path $ReportsDir ("ServerStatus_Report_{0}-ALL.csv" -f $timestamp)
$LogFile        = Join-Path $ReportsDir ("log_{0}.log" -f (Get-Date -Format 'yyyyMMddHHmmss'))
$TranscriptFile = Join-Path $ReportsDir ("transcript_{0}.txt" -f (Get-Date -Format 'yyyyMMddHHmmss'))

# ========== LOGGING ==========
function Log-Message {
    param([string]$Message,[string]$Level='INFO')
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $modePrefix = if ($Test) { '[TEST]' } else { '[ACTION]' }
    $entry = "$ts $modePrefix [$Level] $Message"
    Ensure-Directory (Split-Path -Parent $LogFile)

    # Robust append with retry to avoid file-locks (e.g., OneDrive sync) and Transcript locks.
    $maxAttempts = 4; $delayMs = 150
    for ($i=1; $i -le $maxAttempts; $i++) {
        try {
            $fs = [System.IO.File]::Open($LogFile, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::ReadWrite)
            try {
                $fs.Seek(0, [System.IO.SeekOrigin]::End) | Out-Null
                $bytes = [System.Text.Encoding]::UTF8.GetBytes($entry + [Environment]::NewLine)
                $fs.Write($bytes, 0, $bytes.Length)
                $fs.Flush()
            } finally { $fs.Dispose() }
            break
        } catch {
            if ($i -eq $maxAttempts) { Write-Warning "Log file locked; writing to console only: $($_.Exception.Message)" }
            Start-Sleep -Milliseconds $delayMs
        }
    }
    Write-Host $entry
}

# Start a transcript to capture console output as well
try { Start-Transcript -Path $TranscriptFile -Append -ErrorAction SilentlyContinue } catch {}

# ========== HELPERS ==========
function Get-ADLookupName {
    param([Parameter(Mandatory=$true)][string]$VMName)
    $name = $VMName -replace '\s+-\s+.*$', ''   # drop " - description"
    $name = $name -replace '\s+', ''             # drop spaces but keep hyphens/underscores/digits
    return $name.Trim()
}
function Get-ReversePTRInfo([string]$IPv4) {
    try {
        if (-not ($IPv4 -match '^\d{1,3}(\.\d{1,3}){3}$')) { return $null }
        $oct = $IPv4.Split('.')
        if ($oct.Count -ne 4) { return $null }
        # Assumes /24 reverse zones; if your infra uses other delegations, extend this logic.
        [PSCustomObject]@{ Zone = "{0}.{1}.{2}.in-addr.arpa" -f $oct[2],$oct[1],$oct[0]; Name=$oct[3] }
    } catch { $null }
}
function Invoke-Remote {
    param(
        [Parameter(Mandatory=$true)][string]$ComputerName,
        [Parameter(Mandatory=$true)][ScriptBlock]$ScriptBlock,
        [Parameter()][object[]]$ArgumentList=@(),
        [Parameter()][switch]$StopOnError
    )
    try {
        Invoke-Command -ComputerName $ComputerName -Credential $Cred -ScriptBlock $ScriptBlock -ArgumentList $ArgumentList -ErrorAction Stop
    } catch {
        if ($StopOnError) { throw } else { $null }
    }
}

# ========== AD / DNS / SCOM WRAPPERS ==========
function Invoke-ADQuery {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Hostname
    )

    Import-Module ActiveDirectory -ErrorAction Stop

    $c = $null

    # 1) Try direct identity (covers DN, GUID, sAMAccountName if exact)
    try {
        $c = Get-ADComputer -Identity $Hostname `
                            -Properties OperatingSystem, DNSHostName, DistinguishedName `
                            -ErrorAction Stop
    } catch {
        # 2) Fallback – same pattern as your other AD logic:
        #    - sAMAccountName = Hostname$
        #    - or CN = Hostname
        $name    = $Hostname
        $samName = ($name -notmatch '\$$') ? "$name$" : $name

        $c = Get-ADComputer -LDAPFilter "(|(sAMAccountName=$samName)(cn=$name))" `
                            -Properties OperatingSystem, DNSHostName, DistinguishedName `
                            -ErrorAction SilentlyContinue
    }

    if ($c) { $c } else { $null }
}


function Invoke-ADResolveComputerDN {
    param([Parameter(Mandatory=$true)][string]$ComputerIdentityOrName)
    $sb = {
        param($idOrName)
        Import-Module ActiveDirectory -ErrorAction Stop
        try {
            $c = Get-ADComputer -Identity $idOrName -Properties DistinguishedName -ErrorAction Stop
        } catch {
            $name    = $idOrName
            $samName = ($name -notmatch '\$$') ? "$name$" : $name
            $c = Get-ADComputer -LDAPFilter "(|(sAMAccountName=$samName)(cn=$name))" -Properties DistinguishedName -ErrorAction Stop
        }
        if ($c) {
            [PSCustomObject]@{ DN=$c.DistinguishedName; Name=$c.Name }
        } else {
            $null
        }
    }
    Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($ComputerIdentityOrName)
}

function Invoke-ADGetWUGroups {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ComputerIdentityOrDN
    )

    $sb = {
        param($compIdOrDn, $excGroupName)
        Import-Module ActiveDirectory -ErrorAction Stop

        # -------- Resolve the computer DN (same as before) --------
        $comp = $null
        try {
            $comp = Get-ADComputer -Identity $compIdOrDn -Properties DistinguishedName -ErrorAction Stop
        } catch {
            $name    = $compIdOrDn
            $samName = ($name -notmatch '\$$') ? "$name$" : $name
            $comp = Get-ADComputer -LDAPFilter "(|(sAMAccountName=$samName)(cn=$name))" -Properties DistinguishedName -ErrorAction Stop
        }

        if (-not $comp) { return @() }
        $targetDN = $comp.DistinguishedName

        # -------- Working WU logic (from PatchingReport.ps1 style) --------
        $searchBase = (Get-ADRootDSE).DefaultNamingContext
        $wuPattern  = "*app.WindowsUpdate*"

        # All *app.WindowsUpdate* groups under the domain
        $wuGroups = Get-ADGroup -Filter "Name -like '$wuPattern'" `
                                -SearchBase $searchBase `
                                -Properties DistinguishedName, Name

        # Exception group by name
        $excGroup = $null
        if ($excGroupName) {
            $excGroup = Get-ADGroup -Filter "Name -eq '$excGroupName'" `
                                    -SearchBase $searchBase `
                                    -Properties DistinguishedName, Name
        }
        $excDN = if ($excGroup) { $excGroup.DistinguishedName } else { $null }

        # Build list of groups this computer is in
        $matched = New-Object System.Collections.Generic.List[object]

        foreach ($g in $wuGroups) {
            $gName = $g.Name
            $gDN   = $g.DistinguishedName
            try {
                Get-ADGroupMember -Identity $gDN -Recursive -ErrorAction Stop |
                    Where-Object { $_.objectClass -eq 'computer' } |
                    ForEach-Object {
                        if ($_.distinguishedName -eq $targetDN) {
                            $isExc = ($excDN -and $gDN -eq $excDN)
                            $matched.Add([PSCustomObject]@{
                                Name              = $gName
                                DistinguishedName = $gDN
                                IsException       = $isExc
                            }) | Out-Null
                        }
                    }
            } catch {
                Write-Verbose "Failed expanding members of group '$gName': $_"
            }
        }

        return $matched
    }

    Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($ComputerIdentityOrDN, $WUExceptionGroupName)
}


function Invoke-ADRemoveFromGroup {
    param(
        [Parameter(Mandatory=$true)][string]$ComputerDN,
        [Parameter(Mandatory=$true)][string]$GroupDN,
        [switch]$WhatIf
    )
    $sb = {
        param($compDn,$grpDn,$doWhatIf)
        Import-Module ActiveDirectory -ErrorAction Stop
        if ($doWhatIf) {
            Remove-ADGroupMember -Identity $grpDn -Members $compDn -WhatIf
            'WHATIF'
        } else {
            Remove-ADGroupMember -Identity $grpDn -Members $compDn -Confirm:$false
            'OK'
        }
    }
    Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($ComputerDN,$GroupDN,[bool]$WhatIf)
}

function Invoke-ADRemoveComputer {
    param(
        [Parameter(Mandatory=$true)]$AdComputer
    )
    $sb = {
        param($compDN)
        Import-Module ActiveDirectory -ErrorAction Stop
        Remove-ADComputer -Identity $compDN -Confirm:$false -ErrorAction Stop
        'REMOVED'
    }
    Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($AdComputer.DistinguishedName)
}

function Invoke-DNSQuery([string]$Zone,[string]$Name,[string]$RRType) {
    $sb = {
        param($zone,$name,$rr)
        Import-Module DNSServer -ErrorAction Stop
        Get-DnsServerResourceRecord -ZoneName $zone -Name $name -RRType $rr -ErrorAction SilentlyContinue
    }
    Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($Zone,$Name,$RRType)
}
function Invoke-DNSRemove([string]$Zone,[string]$Name,[string]$RRType) {
    $sb = {
        param($zone,$name,$rr)
        Import-Module DNSServer -ErrorAction Stop
        Remove-DnsServerResourceRecord -ZoneName $zone -Name $name -RRType $rr -Force
        'OK'
    }
    Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($Zone,$Name,$RRType)
}

# SCOM: remove if registered (ACTION), WhatIf in TEST
function Test-SCOMRegistration {
    param([Parameter(Mandatory=$true)][string]$HostnameFQDN,[Parameter(Mandatory=$true)][string]$SCOMServer,[switch]$Test)
    $result = @{ SCOMStatus=''; ActionsNeeded=[System.Collections.ArrayList]@(); Notes='' }
    if ([string]::IsNullOrWhiteSpace($HostnameFQDN)) {
        $result.SCOMStatus='No FQDN available'
        Log-Message "No FQDN available for SCOM check"
        return $result
    }
    try {
        Log-Message "Checking SCOM registration for ${HostnameFQDN} on ${SCOMServer}"
        $scomStatus = Invoke-Remote -ComputerName $SCOMServer -ScriptBlock {
            param($serverFQDN,$doRemove)
            Import-Module OperationsManager -ErrorAction Stop
            $agent = Get-SCOMAgent -DNSHostName $serverFQDN -ErrorAction SilentlyContinue
            if ($agent) {
                if ($doRemove) {
                    try { Remove-SCOMAgent -Agent $agent -Confirm:$false; @{ IsRegistered=$true; Removed=$true; HealthState=$agent.HealthState } }
                    catch { @{ IsRegistered=$true; Removed=$false; Error=$_.Exception.Message } }
                } else {
                    @{ IsRegistered=$true; Removed=$false; HealthState=$agent.HealthState }
                }
            } else { @{ IsRegistered=$false } }
        } -ArgumentList @($HostnameFQDN, (-not $Test)) -StopOnError

        if ($scomStatus -and $scomStatus.IsRegistered) {
            if ($scomStatus.Removed) {
                $result.SCOMStatus = 'Removed from SCOM'
                $result.ActionsNeeded.Add("ACTION: Removed SCOM agent for $HostnameFQDN") | Out-Null
                Log-Message "ACTION: Removed SCOM agent for ${HostnameFQDN}"
            } elseif ($Test) {
                $result.SCOMStatus = 'Registered - Would remove'
                $result.ActionsNeeded.Add("TEST: Would remove SCOM agent for $HostnameFQDN") | Out-Null
                Log-Message "TEST: Would remove SCOM agent for ${HostnameFQDN}"
            } elseif ($scomStatus.Error) {
                $result.SCOMStatus = 'Removal failed'
                $result.Notes += "SCOM removal error: $($scomStatus.Error)"
                Log-Message "ERROR: Failed to remove SCOM agent for ${HostnameFQDN}: $($scomStatus.Error)" 'ERROR'
            } else { $result.SCOMStatus = 'Registered - Not removed' }
        } else {
            $result.SCOMStatus = 'Not registered'
            Log-Message "SCOM agent not found for ${HostnameFQDN}"
        }
    } catch {
        $result.SCOMStatus='Error checking SCOM'
        $result.Notes += "SCOM Check Error: $_"
        Log-Message "Error checking SCOM status for ${HostnameFQDN}: $_" 'ERROR'
    }
    return $result
}

# ========== VCENTER SHUTDOWN ==========
function Invoke-VMShutdownIfNeeded {
    param(
        [Parameter(Mandatory=$true)][string]$VMName,
        [Parameter(Mandatory=$true)][switch]$IsActionMode,
        [int]$GraceMinutes=8,
        [string]$ChangeNumber,
        [switch]$EnsureConsistency
    )
    try { $vmObj = Get-VM -Name $VMName -ErrorAction Stop } catch { Log-Message "VM ${VMName} not found while attempting shutdown/rename: $_" 'ERROR'; return $false }

    function Add-DecomNoteAndMaybeRename {
        param($vm,[string]$chg,[switch]$DoIt)
        $ts  = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
        $who = $Cred.UserName
        $tag = if ([string]::IsNullOrWhiteSpace($chg)) { '' } else { " (CHANGE $chg)" }
        $newNoteLine = "Decom Shutdown${tag}: $ts by $who"
        if ($DoIt) {
            try {
                $existing = $vm.Notes
                $updatedNotes = if ([string]::IsNullOrWhiteSpace($existing)) { $newNoteLine } else { "$existing`r`n$newNoteLine" }
                Set-VM -VM $vm -Notes $updatedNotes -Confirm:$false | Out-Null
                Log-Message "Appended decom note$tag to VM Notes for ${($vm.Name)}"
            } catch { Log-Message "Failed to update VM Notes for ${($vm.Name)}: $_" 'WARN' }
            try {
                if ($chg -and ($vm.Name -notmatch [Regex]::Escape($chg))) {
                    $newName = "$($vm.Name)-$chg"
                    Set-VM -VM $vm -Name $newName -Confirm:$false | Out-Null
                    Log-Message "Renamed VM '${($vm.Name)}' to '$newName' (appended change)"
                } else {
                    Log-Message "Rename skipped for ${($vm.Name)} (already contains change or change not supplied)"
                }
            } catch { Log-Message "Failed to rename VM ${($vm.Name)}: $_" 'WARN' }
        } else {
            Log-Message "TEST: Would append decom note$tag and rename VM to include change if missing for ${($vm.Name)}"
        }
    }

    # If VM is already off and we're ensuring consistency, just stamp/rename
    if ($vmObj.PowerState -eq 'PoweredOff' -and $EnsureConsistency) {
        Add-DecomNoteAndMaybeRename -vm $vmObj -chg $ChangeNumber -DoIt:$IsActionMode
        return $true
    }

    if ($vmObj.PowerState -eq 'PoweredOff') { Log-Message "VM ${VMName} already PoweredOff; no shutdown needed."; return $true }
    if (-not $IsActionMode) { Log-Message "TEST: Would initiate shutdown for ${VMName}"; return $false }

    $toolsOk = ($vmObj.ExtensionData.Guest.ToolsStatus -match 'toolsOk|toolsOld|toolsSupported')
    if ($toolsOk) {
        try { Log-Message "Attempting graceful guest shutdown for ${VMName}"; Shutdown-VMGuest -VM $vmObj -Confirm:$false -ErrorAction SilentlyContinue | Out-Null }
        catch { Log-Message "Graceful shutdown call failed for ${VMName}: $_" 'WARN' }
    } else { Log-Message "VMware Tools not healthy on ${VMName}; skipping guest shutdown" }

    $deadline = (Get-Date).AddMinutes($GraceMinutes)
    while ((Get-VM -Id $vmObj.Id).PowerState -ne 'PoweredOff' -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 10 }
    $vmObj = Get-VM -Id $vmObj.Id
    if ($vmObj.PowerState -ne 'PoweredOff') {
        Log-Message "Graceful shutdown timed out; forcing Stop-VM on ${VMName}"
        Stop-VM -VM $vmObj -Confirm:$false | Out-Null
        $deadline = (Get-Date).AddMinutes(2)
        while ((Get-VM -Id $vmObj.Id).PowerState -ne 'PoweredOff' -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 5 }
        $vmObj = Get-VM -Id $vmObj.Id
    }
    if ($vmObj.PowerState -eq 'PoweredOff') {
        Add-DecomNoteAndMaybeRename -vm $vmObj -chg $ChangeNumber -DoIt:$IsActionMode
        return $true
    } else { Log-Message "Failed to power off VM ${VMName}" 'ERROR'; return $false }
}

# ========== CORE CHECKS ==========

function Test-ServerConnectivity {
    param([Parameter(Mandatory=$true)][string]$Hostname,[string]$FallbackDnsSuffix)

    $candidates = @()
    if ($Hostname -match '\.') { $candidates += $Hostname }
    elseif ($FallbackDnsSuffix) { $candidates += "$Hostname.$FallbackDnsSuffix" }
    $candidates += $Hostname
    $lat = $null
    $status = 'No ping'   # default when we don't get a response

    foreach ($h in $candidates | Select-Object -Unique) {
        try {
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            if (Test-Connection -ComputerName $h -Count 1 -Quiet -ErrorAction SilentlyContinue) {
                $sw.Stop()
                $status = 'Running'
                $lat    = [Math]::Round($sw.Elapsed.TotalMilliseconds,2)
                break
            }
            $sw.Stop()
        } catch {}
    }
    @{ Status=$status; LatencyMs=$lat }
}

function Get-VMShutdownAge {
    param([Parameter(Mandatory=$true)][string]$VMName)
    try {
        $vm = Get-VM -Name $VMName -ErrorAction SilentlyContinue
        if (-not $vm) { return [PSCustomObject]@{ VMName=$VMName; ShutdownDate=$null; DaysSinceShutdown=$null; Error='VM not found' } }

        $evt = Get-VIEvent -Entity $vm -MaxSamples 400 |
            Where-Object {
                $_.GetType().Name -in @('VmPoweredOffEvent','VmPoweredOffByHostEvent','VmPoweredOffUsingGuestOperationsEvent','TaskEvent') -or
                ($_.EventTypeId -eq 'TaskEvent' -and $_.Info.DescriptionId -eq 'VirtualMachine.powerOff')
            } |
            Sort-Object CreatedTime -Descending | Select-Object -First 1
        if ($evt) {
            $sd = $evt.CreatedTime
            $days = [math]::Round(((Get-Date) - $sd).TotalDays,1)
            [PSCustomObject]@{ VMName=$VMName; ShutdownDate=$sd; DaysSinceShutdown=$days; Error=$null }
        } else {
            if ($vm.PowerState -eq 'PoweredOff') {
                [PSCustomObject]@{ VMName=$VMName; ShutdownDate=$null; DaysSinceShutdown=$null; Error='Powered off but no event found' }
            } else {
                [PSCustomObject]@{ VMName=$VMName; ShutdownDate=$null; DaysSinceShutdown=$null; Error='No power-off event found' }
            }
        }
    } catch { [PSCustomObject]@{ VMName=$VMName; ShutdownDate=$null; DaysSinceShutdown=$null; Error="Error: $_" } }
}

function Test-ServerDNSRecords {
    param([string]$ShortHost,[string]$FQDN,[string]$IPAddress,[string]$DnsZone,[switch]$Test)
    $result = @{ DNSStatus=''; ActionsNeeded=[System.Collections.ArrayList]@(); Notes='' }
    try {
        if ([string]::IsNullOrWhiteSpace($ShortHost)) { $ShortHost = $FQDN.Split('.')[0] }
        $aRecord = Invoke-DNSQuery -Zone $DnsZone -Name $ShortHost -RRType 'A'
        $ptrRecord = $null; $ptrInfo=$null
        if ($IPAddress -and $IPAddress -ne 'Not Resolvable') {
            $ptrInfo = Get-ReversePTRInfo -IPv4 $IPAddress
            if ($ptrInfo) { $ptrRecord = Invoke-DNSQuery -Zone $ptrInfo.Zone -Name $ptrInfo.Name -RRType 'PTR' }
            else { Log-Message "PTR zone not inferred for $IPAddress; skipping PTR removal" 'WARN' }
        }
        if ($aRecord -or $ptrRecord) {
            Log-Message "DNS records found for ${FQDN} (A: $([bool]$aRecord), PTR: $([bool]$ptrRecord))"
            if ($Test) {
                $result.DNSStatus = 'TEST: DNS records present - would remove'
                $result.ActionsNeeded.Add("TEST: Would remove DNS A/PTR for $FQDN") | Out-Null
            } else {
                try {
                    if ($aRecord) { Invoke-DNSRemove -Zone $DnsZone -Name $ShortHost -RRType 'A' | Out-Null; $result.ActionsNeeded.Add("ACTION: Removed DNS A for $FQDN") | Out-Null; Log-Message "Removed DNS A for ${FQDN}" }
                    if ($ptrRecord) { Invoke-DNSRemove -Zone $ptrInfo.Zone -Name $ptrInfo.Name -RRType 'PTR' | Out-Null; $result.ActionsNeeded.Add("ACTION: Removed DNS PTR for $IPAddress") | Out-Null; Log-Message "Removed DNS PTR $($ptrInfo.Name).$($ptrInfo.Zone)" }
                    $result.DNSStatus = 'ACTION: DNS records removed'
                } catch {
                    $err = "ERROR: Failed to remove DNS records: $_"; Log-Message $err 'ERROR'
                    $result.ActionsNeeded.Add($err) | Out-Null; $result.Notes += "`nDNS Removal Error: $_"
                }
            }
        } else { Log-Message "No DNS records found for ${FQDN}"; $result.DNSStatus='No DNS records found' }
    } catch {
        $err = "Error checking DNS records: $_"; Log-Message $err 'ERROR'
        $result.ActionsNeeded.Add($err) | Out-Null; $result.Notes += "`nDNS Check Error: $_"
    }
    return $result
}

function Remove-WindowsUpdateGroups {
    param([Parameter(Mandatory=$true)]$AdComputer,[switch]$Test)

    $result = @{ IvantiStatus=''; ActionsNeeded=[System.Collections.ArrayList]@(); Notes='' }

    try {
        $compDN   = $AdComputer.DistinguishedName
        $compName = $AdComputer.Name

        if (-not $compDN) {
            $result.IvantiStatus = 'No DN'; $result.Notes += 'Computer has no DistinguishedName'
            Log-Message "No DN present for $compName (cannot evaluate WU groups)" 'WARN'
            return $result
        }

        # Use recursive membership logic and skip Exception group from removal
        $groups = Invoke-ADGetWUGroups -ComputerIdentityOrDN $compDN

        if ($groups -and $groups.Count -gt 0) {
            $processed = @()
            foreach ($g in $groups) {
                $gDN = $g.DistinguishedName
                $gName = $g.Name
                $isExc = ($g.PSObject.Properties['IsException'] -and $g.IsException)

                if ($isExc) {
                    Log-Message "$compName is in Exception group '$gName' — will not remove"
                    continue
                }

                Log-Message "$compName is member of $gName"
                try {
                    if ($Test) {
                        Invoke-ADRemoveFromGroup -ComputerDN $compDN -GroupDN $gDN -WhatIf | Out-Null
                        $processed += "TEST: Would remove from $gName"
                    } else {
                        Invoke-ADRemoveFromGroup -ComputerDN $compDN -GroupDN $gDN | Out-Null
                        $processed += "ACTION: Removed from $gName"
                        Log-Message "Removed $compName from $gName"
                    }
                }
                catch {
                    $err = "ERROR: Failed to remove from ${gName}: $($_)"
                    Log-Message $err 'ERROR'
                    $processed += $err
                    $result.Notes += "`nUpdate Group Removal Error: $_"
                }
            }
            $result.IvantiStatus = if ($Test) { 'TEST: Would remove from app.WindowsUpdate groups (excluding Exceptions)' } else { 'ACTION: Removed from app.WindowsUpdate groups (excluding Exceptions)' }
            if ($processed.Count -gt 0) { $result.ActionsNeeded.Add(($processed -join ' | ')) | Out-Null }
        }
        else {
            $result.IvantiStatus = 'No matching app.WindowsUpdate groups'
            Log-Message "No matching app.WindowsUpdate* groups for $compName"
        }
    }
    catch {
        $err = "Error checking Update Group membership: $_"
        Log-Message $err 'ERROR'
        $result.IvantiStatus = 'Check error'
        $result.ActionsNeeded.Add($err) | Out-Null
        $result.Notes += "`nUpdate Group Check Error: $_"
    }

    return $result
}

function Test-DecommissionStatus {
    param([string]$Hostname,$ConnectivityResult,[string]$DnsZone,[string]$vCenterPowerState,[switch]$Test)
    $result = @{ DecommissionStatus=''; ActionsNeeded=[System.Collections.ArrayList]@(); ADExists=$null; OS=''; IPAddress=''; ADStatus=''; Notes=''; FQDN=''; DNSStatus='' }
    try {
        $adObj = Invoke-ADQuery -Hostname $Hostname
        if ($adObj) {
            $result.ADExists=$adObj; $result.OS=$adObj.OperatingSystem; $result.ADStatus='Exists in AD'; $result.FQDN=$adObj.DNSHostName
            $shortHost = if ($result.FQDN) { $result.FQDN.Split('.')[0] } else { $Hostname }
            try {
                $nameForDns = if ($result.FQDN) { $result.FQDN } else { $Hostname }
                $ipObj = [System.Net.Dns]::GetHostAddresses($nameForDns) | Where-Object { $_.AddressFamily -eq 'InterNetwork' } | Select-Object -First 1
                $result.IPAddress = if ($ipObj) { $ipObj.IPAddressToString } else { 'Not Resolvable' }
            } catch { $result.IPAddress='Not Resolvable' }

            $safeToDecom = ($ConnectivityResult.Status -in @('Shutdown','No ping') -and $vCenterPowerState -eq 'PoweredOff')
            if ($safeToDecom) {
                $result.DecommissionStatus = 'Ready for decommission'
                if ($Test) {
                    $result.ActionsNeeded.Add('TEST: Would remove AD computer account') | Out-Null
                } else {
                    try { Invoke-ADRemoveComputer -AdComputer $adObj | Out-Null; $result.ADStatus='Removed from AD'; $result.ActionsNeeded.Add('ACTION: AD computer account removed') | Out-Null }
                    catch { $err="ERROR: Failed to remove AD computer account: $_"; Log-Message $err 'ERROR'; $result.ActionsNeeded.Add($err) | Out-Null; $result.Notes += "`nAD Removal Error: $_" }
                }
                $dnsResult = Test-ServerDNSRecords -ShortHost $shortHost -FQDN $result.FQDN -IPAddress $result.IPAddress -DnsZone $DnsZone -Test:$Test
                $result.DNSStatus = $dnsResult.DNSStatus
                if ($dnsResult.ActionsNeeded) { $result.ActionsNeeded.Add(($dnsResult.ActionsNeeded -join ' | ')) | Out-Null }
                $result.Notes += $dnsResult.Notes
            } else {
                $why=@(); if ($ConnectivityResult.Status -ne 'Shutdown') { $why+='responds to ping' }; if ($vCenterPowerState -ne 'PoweredOff') { $why+='vCenter shows PoweredOn' }
                $reason = if ($why) { $why -join ' & ' } else { 'unknown condition' }
                $result.DecommissionStatus = "Not safe to decommission ($reason)"
                $msg = if ($Test) { 'TEST: Would wait until: no ping AND PoweredOff' } else { 'ACTION: Must shut down VM and stop ping before removal' }
                $result.ActionsNeeded.Add($msg) | Out-Null
            }
        } else { $result.ADStatus='Not found in AD'; Log-Message "AD lookup failed for ${Hostname}" 'WARN' }
    } catch { $result.ADStatus='Error during AD step'; Log-Message "AD step error for ${Hostname}: $_" 'ERROR' }
    return $result
}

# ========== MODE PICKER ==========
if (-not $PSBoundParameters.ContainsKey('Test')) {
    Write-Host "Select run mode:"
    Write-Host "  T) TEST mode (safe; no changes)"
    Write-Host "  A) ACTION mode (will make changes)"
    $choice = Read-Host "Enter T or A"
    if ($choice) { $choice = $choice.Trim().ToUpper() } else { $choice = 'T' }
    switch ($choice) {
        'A' { $Test = $false }
        'T' { $Test = $true }
        default { $Test = $true }
    }
    $modeWord = if ($Test) { 'TEST' } else { 'ACTION' }
    Write-Host ("Running in {0} mode (per-host CHG mapping will be used if present)." -f $modeWord)
}

# ========== CONNECT ==========
$Cred = Get-Credential
try {
    Connect-VIServer -Server $vCenterServer -Credential $Cred -WarningAction SilentlyContinue | Out-Null
    Log-Message "Connected to vCenter servers: $($vCenterServer -join ', ')"
} catch { Log-Message "Failed to connect to vCenter: $_" 'ERROR'; throw }

if ($Test) {
    Write-Host "=== RUNNING IN TEST MODE - NO CHANGES WILL BE MADE ===" -ForegroundColor Yellow -BackgroundColor Red
    Log-Message 'Script started in TEST MODE - no changes will be made'
} else {
    Write-Host "=== RUNNING IN ACTION MODE - CHANGES WILL BE MADE ===" -ForegroundColor White -BackgroundColor Red
    $confirmation = Read-Host "Type 'YES' to confirm ACTION mode"
    if ($confirmation -ne 'YES') { Write-Host "Operation cancelled." -ForegroundColor Yellow; Log-Message 'User cancelled ACTION mode start'; Disconnect-VIServer -Server $vCenterServer -Confirm:$false | Out-Null; try { Stop-Transcript | Out-Null } catch {}; return }
    Log-Message 'Script started in ACTION MODE - changes will be made' 'WARNING'
}

# ========== BUILD TARGETS ==========
$Targets = $ChangeItems |
    Group-Object Host | ForEach-Object { $_.Group | Select-Object -First 1 } | # dedupe by Host
    Sort-Object Host

$TargetHosts = $Targets.Host
if ($TargetHosts.Count -gt 0) { Log-Message ("Targets loaded from change list: {0}" -f ($TargetHosts -join ', ')) }

# helper to fetch AD OS safely
function Get-ADOperatingSystem([string]$ShortName) {
    try { 
        $ad = Invoke-ADQuery -Hostname $ShortName
        if ($ad) { return $ad.OperatingSystem }
        else { return 'Unable to retrieve OS' }
    } catch { return 'Unable to retrieve OS' }
}

# ========== INVENTORY DISCOVERY ==========
Log-Message 'Getting VM inventory data'
$vmResults = @()

foreach ($t in $TargetHosts) {
    # Try exact name first, then prefix match (faster/safer than contains)
    $vmMatches = Get-VM -Name $t -ErrorAction SilentlyContinue
    if (-not $vmMatches) { $vmMatches = Get-VM -Name "$t*" -ErrorAction SilentlyContinue }

    if (-not $vmMatches -or $vmMatches.Count -eq 0) {
        Log-Message "WARN: VM '$t' not found across connected vCenters (PowerState will be 'Unknown')." 'WARN'
        $adName = Get-ADLookupName -VMName $t
        $osVal  = Get-ADOperatingSystem -ShortName $adName
        $chg    = ($Targets | Where-Object Host -eq $t | Select-Object -ExpandProperty Change -First 1)

        $vmResults += [PSCustomObject]@{
            VMName     = $t
            Hostname   = $adName
            Datastore  = ''
            PowerState = 'Unknown'
            Notes      = ''
            GuestFQDN  = ''
            OS         = $osVal
            Change     = $chg
        }
        continue
    }

    foreach ($m in $vmMatches) {
        $adName    = Get-ADLookupName -VMName $m.Name
        $guestFqdn = $null
        try { $guestFqdn = $m.Guest.HostName } catch {}
        $chg   = ($Targets | Where-Object Host -eq $t | Select-Object -ExpandProperty Change -First 1)
        $osVal = Get-ADOperatingSystem -ShortName $adName

        $dsNames = ''
        try { $dsNames = [string]::Join(",", (Get-Datastore -Id $m.DatastoreIdList | Select-Object -ExpandProperty Name)) } catch {}

        $vmResults += [PSCustomObject]@{
            VMName     = $m.Name
            Hostname   = $adName
            Datastore  = $dsNames
            PowerState = $m.PowerState.ToString()
            Notes      = $m.Notes
            GuestFQDN  = $guestFqdn
            OS         = $osVal
            Change     = $chg
        }
    }
}

$vmResults | Export-Csv -Path $CsvDiscovery -NoTypeInformation
Log-Message "Exported VM discovery list to $CsvDiscovery"

# ========== PER-VM RESULT MODEL ==========
function New-ResultRow($vm) {
    [PSCustomObject]@{
        Hostname            = $vm.Hostname
        VMName              = $vm.VMName
        Datastore           = $vm.Datastore
        PowerState          = $vm.PowerState
        OS                  = $vm.OS
        Status              = ''
        LatencyMs           = ''
        IPAddress           = ''
        ADStatus            = ''
        DNSStatus           = ''
        IvantiStatus        = ''
        SCOMStatus          = ''
        DecommissionStatus  = ''
        ActionsNeeded       = ''
        Notes               = ''
        ProcessingErrors    = ''
        Mode                = if ($Test) { 'TEST' } else { 'ACTION' }
        ShutdownDate        = ''
        DaysSinceShutdown   = ''
        CanDeleteVMDisk     = ''
        FQDN                = ''
        ChangeNumber        = $vm.Change
    }
}

$finalResults = @()

# ========== STEP FUNCTIONS ==========
function Step1-ConnectivityAndAge($vms) {
    Log-Message ("Step 1 running in SERIAL for {0} target(s)" -f $vms.Count)
    $rows = @()
    foreach ($vm in $vms) {
        $row = New-ResultRow $vm
        try {
            $conn = Test-ServerConnectivity -Hostname $vm.Hostname -FallbackDnsSuffix $DnsZone
            $row.Status    = $conn.Status
            $row.LatencyMs = $conn.LatencyMs

            if ($vm.PowerState -eq 'PoweredOff') {
                $age = Get-VMShutdownAge -VMName $vm.VMName
                $row.ShutdownDate      = if ($age.ShutdownDate) { $age.ShutdownDate.ToString('yyyy-MM-dd HH:mm:ss') } else { 'Unknown' }
                $row.DaysSinceShutdown = if ($age.DaysSinceShutdown) { $age.DaysSinceShutdown } else { 'Unknown' }
                $row.CanDeleteVMDisk   = if ($age.DaysSinceShutdown -ne $null -and $age.DaysSinceShutdown -gt 30) { 'Yes' } else { 'No' }
                if ($age.Error) { $row.Notes += "VM Shutdown Check Error: $($age.Error)`n" }
            } else {
                $row.CanDeleteVMDisk   = 'No - VM Still Running'
                $row.ShutdownDate      = 'N/A - VM Running'
                $row.DaysSinceShutdown = 'N/A'
            }
        } catch { $row.ProcessingErrors = "Step1 error: $_"; Log-Message "Step1 error on ${($vm.VMName)}: $_" 'ERROR' }
        $rows += $row
    }

    $rows |
      Select-Object Hostname,
                    @{N='MatchedVM';E={$_.VMName}},
                    PowerState, Status, LatencyMs, ShutdownDate, DaysSinceShutdown, CanDeleteVMDisk, ChangeNumber |
      Format-Table -AutoSize | Out-Host
    Log-Message ("Step 1 completed for {0} target(s)" -f $vms.Count)
    return $rows
}

function Step2-SCOM($rows) {
    foreach ($row in $rows) {
        try {
            $adObj = Invoke-ADQuery -Hostname $row.Hostname
            $guestFqdnFromInv = ($vmResults | Where-Object VMName -eq $row.VMName | Select-Object -ExpandProperty GuestFQDN -First 1)
            $fqdnForScom = if ($adObj -and $adObj.DNSHostName) { $adObj.DNSHostName }
                           elseif ($guestFqdnFromInv) { $guestFqdnFromInv }
                           else { "$($row.Hostname).$DnsZone" }

            $row.FQDN = $fqdnForScom

            $sc = Test-SCOMRegistration -HostnameFQDN $fqdnForScom -SCOMServer $SCOMSvr -Test:$Test
            $row.SCOMStatus = $sc.SCOMStatus
            if ($sc.ActionsNeeded) { $row.ActionsNeeded += (($sc.ActionsNeeded | Where-Object {$_}) -join ' | ') + ' | ' }
            if ($sc.Notes)        { $row.Notes        += $sc.Notes + "`n" }
        } catch { $row.ProcessingErrors += " Step2 error: $_"; Log-Message "Step2 error on ${($row.VMName)}: $_" 'ERROR' }
    }
    return $rows
}

function Step3-Shutdown($rows) {
    foreach ($row in $rows) {
        try {
            # In ACTION mode: always ensure consistency (stamp + rename) for powered-off VMs, and
            # attempt shutdown + stamp/rename for running ones.
            if (-not $Test) {
                if ($row.PowerState -eq 'PoweredOff' -or $row.PowerState -eq 'Unknown') {
                    [void](Invoke-VMShutdownIfNeeded -VMName $row.VMName -IsActionMode -ChangeNumber $row.ChangeNumber -EnsureConsistency)
                } else {
                    [void](Invoke-VMShutdownIfNeeded -VMName $row.VMName -IsActionMode -ChangeNumber $row.ChangeNumber -EnsureConsistency)
                    $ref = Get-VM -Name "$($row.VMName)*" -ErrorAction SilentlyContinue | Sort-Object -Property Name | Select-Object -Last 1
                    if ($ref) { $row.PowerState = $ref.PowerState.ToString(); $row.VMName = $ref.Name }
                }
            } else {
                if ($row.PowerState -ne 'Unknown') {
                    Log-Message "TEST: Would shut down (if running), append decom note with change '$($row.ChangeNumber)', and rename VM to include change for ${($row.VMName)}"
                }
            }
        } catch { $row.ProcessingErrors += " Step3 error: $_"; Log-Message "Step3 error on ${($row.VMName)}: $_" 'ERROR' }
    }
    return $rows
}

function Step4-AD_DNS_Groups($rows) {
    foreach ($row in $rows) {
        try {
            $conn  = @{ Status = $row.Status }
            $decom = Test-DecommissionStatus -Hostname $row.Hostname -ConnectivityResult $conn -DnsZone $DnsZone -vCenterPowerState $row.PowerState -Test:$Test
            $row.ADStatus           = $decom.ADStatus
            $row.DecommissionStatus = $decom.DecommissionStatus
            $row.IPAddress          = $decom.IPAddress
            $row.DNSStatus          = $decom.DNSStatus
            if (-not [string]::IsNullOrWhiteSpace($decom.FQDN)) { $row.FQDN = $decom.FQDN }

            if ($decom.ADExists) {
                $iv = Remove-WindowsUpdateGroups -AdComputer $decom.ADExists -Test:$Test
                $row.IvantiStatus = $iv.IvantiStatus
                if ($iv.ActionsNeeded) { $row.ActionsNeeded += (($iv.ActionsNeeded | Where-Object {$_}) -join ' | ') + ' | ' }
                if ($iv.Notes)        { $row.Notes        += $iv.Notes + "`n" }
            } else {
                $row.ActionsNeeded += 'Server not found in AD | '
                if (-not $row.SCOMStatus) { $row.SCOMStatus = 'Cannot check - not in AD' }
            }
            $row.ActionsNeeded = $row.ActionsNeeded.TrimEnd(' | ')
        } catch { $row.ProcessingErrors += " Step4 error: $_"; Log-Message "Step4 error on ${($row.VMName)}: $_" 'ERROR' }
    }
    return $rows
}

function Step5-FindOlder30d($rows) {
    $older = $rows | Where-Object { $_.CanDeleteVMDisk -eq 'Yes' }
    if ($older.Count -gt 0) {
        Write-Host "PoweredOff >30d candidates:" -ForegroundColor Yellow
        $older | Select-Object Hostname,VMName,ShutdownDate,DaysSinceShutdown | Format-Table -AutoSize | Out-Host
    } else { Write-Host "No PoweredOff VMs older than 30 days were found based on notes." -ForegroundColor DarkYellow }
    return $rows
}

function Step6-RemoveVMs($rows) {
    $older = $rows | Where-Object { $_.CanDeleteVMDisk -eq 'Yes' -and $_.PowerState -eq 'PoweredOff' }
    if ($older.Count -eq 0) { Write-Host "No VMs qualify for deletion (>30d & PoweredOff)." -ForegroundColor Yellow; return $rows }
    if ($Test) {
        foreach ($r in $older) { Log-Message "TEST: Would Remove-VM -DeletePermanently ${($r.VMName)}" }
    } else {
        $confirm = Read-Host "Type 'DELETE' to permanently remove $($older.Count) VM(s)"
        if ($confirm -eq 'DELETE') {
            foreach ($r in $older) {
                try { Remove-VM -VM (Get-VM -Name $r.VMName) -DeletePermanently -Confirm:$false | Out-Null; Log-Message "ACTION: Deleted VM ${($r.VMName)}"; $r.ActionsNeeded += ' | ACTION: VM deleted' }
                catch { Log-Message "ERROR: Failed to delete VM ${($r.VMName)}: $_" 'ERROR' }
            }
        } else { Write-Host "Deletion cancelled." -ForegroundColor Yellow }
    }
    return $rows
}

function Export-Results($rows) { $rows | Export-Csv -Path $CsvResults -NoTypeInformation; Log-Message "Results exported to: $CsvResults" }

function Show-Summary($rows) {
    $modeText = if ($Test) { 'TEST MODE' } else { 'ACTION MODE' }
    Write-Host "`n=== DECOMMISSION SUMMARY ($modeText) ===" -ForegroundColor Cyan
    Write-Host ("Total servers processed: {0}" -f $rows.Count) -ForegroundColor Green
    Write-Host ("Servers not responding to ping: {0}" -f (($rows | Where-Object {$_.Status -eq 'No ping'}).Count)) -ForegroundColor Yellow
    Write-Host ("Servers still running (ping OK): {0}" -f (($rows | Where-Object {$_.Status -eq 'Running'}).Count)) -ForegroundColor Red
    Write-Host ("Servers in AD: {0}" -f (($rows | Where-Object {$_.ADStatus -eq 'Exists in AD'}).Count)) -ForegroundColor Magenta
    Write-Host ("VMs eligible for disk deletion: {0}" -f (($rows | Where-Object {$_.CanDeleteVMDisk -eq 'Yes'}).Count)) -ForegroundColor Cyan
    Write-Host ("SCOM agents removed: {0}" -f (($rows | Where-Object {$_.SCOMStatus -eq 'Removed from SCOM'}).Count)) -ForegroundColor Green
    Write-Host ("PoweredOff >30d flagged: {0}" -f (($rows | Where-Object {$_.CanDeleteVMDisk -eq 'Yes'}).Count))
}

# ========== MENU LOOP ==========
$rows = @()
function Run-All {
    param($vmResultsRef)
    $r = Step1-ConnectivityAndAge $vmResultsRef
    $r = Step2-SCOM $r
    $r = Step3-Shutdown $r
    $r = Step4-AD_DNS_Groups $r
    $r = Step5-FindOlder30d $r
    $r = Step6-RemoveVMs $r
    Export-Results $r
    Show-Summary $r
    return $r
}

# Initial banner + pre-load
$rows = Step1-ConnectivityAndAge $vmResults
$rows | Out-Null

while ($true) {
    Write-Host "`n============== STEP MENU ==============" -ForegroundColor Cyan
    Write-Host "1) Connectivity check + compute shutdown age"
    Write-Host "2) Remove SCOM agents (or WhatIf in TEST)"
    Write-Host "3) Shut down VMs if needed (only acts in ACTION mode, appends decom note)"
    Write-Host "4) AD/DNS decommission (+ remove Windows Update groups)"
    Write-Host "5) Find VMs powered off for > 30 days"
    Write-Host "6) Remove VMs if over 30 days shutdown"
    Write-Host "7) Export current results to CSV"
    Write-Host "8) Show summary counters"
    Write-Host "9) Run ALL steps (1→6) in order"
    Write-Host "X) Exit"
    $opt = Read-Host "Select an option"
    switch ($opt.ToUpper()) {
        '1' { $rows = Step1-ConnectivityAndAge $vmResults }
        '2' { if (-not $rows -or $rows.Count -eq 0) { $rows = Step1-ConnectivityAndAge $vmResults }; $rows = Step2-SCOM $rows }
        '3' { if (-not $rows -or $rows.Count -eq 0) { $rows = Step1-ConnectivityAndAge $vmResults }; $rows = Step3-Shutdown $rows }
        '4' { if (-not $rows -or $rows.Count -eq 0) { $rows = Step1-ConnectivityAndAge $vmResults }; $rows = Step4-AD_DNS_Groups $rows }
        '5' { if (-not $rows -or $rows.Count -eq 0) { $rows = Step1-ConnectivityAndAge $vmResults }; $rows = Step5-FindOlder30d $rows }
        '6' { if (-not $rows -or $rows.Count -eq 0) { $rows = Step1-ConnectivityAndAge $vmResults }; $rows = Step6-RemoveVMs $rows }
        '7' { if (-not $rows -or $rows.Count -eq 0) { $rows = Step1-ConnectivityAndAge $vmResults }; Export-Results $rows }
        '8' { if (-not $rows -or $rows.Count -eq 0) { $rows = Step1-ConnectivityAndAge $vmResults }; Show-Summary $rows }
        '9' { $rows = Run-All $vmResults }
        'X' { break }
        default { Write-Host "Invalid option." -ForegroundColor Yellow }
    }
}

Disconnect-VIServer -Server $vCenterServer -Confirm:$false | Out-Null
Log-Message 'Disconnected from vCenter servers'
try { Stop-Transcript | Out-Null } catch {}
Write-Host "Done."
