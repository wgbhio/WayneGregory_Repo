param(
    [Parameter(Mandatory = $false)]
    [bool]$Test = $true  # default to TEST
)

# ========== CONFIG ==========
$vCenterServer = @('ukprim098.bfl.local','usprim004.bfl.local','ieprim018.bfl.local')
$SCOMSvr   = 'UKPRAP184'        # SCOM management server
$DnsServer = 'ukprdc011'        # Hop for AD/DNS remoting
$DnsZone   = 'bfl.local'
$InfraHop  = $DnsServer
# Windows Update (Ivanti) group handling
$WUExceptionGroupName = 'app.WindowsUpdate.GBL.ExceptionServers'

$RootPath  = 'C:\Users\GrWay\OneDrive - Beazley Group\Documents\Scripts\DeploymentScript\Logs\DecomLogs'

# Change list mapping (deduplicated)
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

    # Newly added (not previously present)
    @{ Change='CHG0092047'; Host='USDVAP071' }
    @{ Change='CHG0092038'; Host='USDVAP088' }
    @{ Change='CHG0092037'; Host='USDVAP087' }
    @{ Change='CHG0092046'; Host='USDVAP079' }
    @{ Change='CHG0092043'; Host='USDVAP075' }
    @{ Change='CHG0092048'; Host='USDVAP072' }
    @{ Change='CHG0092044'; Host='USDVAP077' }
    @{ Change='CHG0092051'; Host='USDVAP074' }
    @{ Change='CHG0092050'; Host='USDVAP073' }
    @{ Change='CHG0092039'; Host='USDVAP089' }
    @{ Change='CHG0092027'; Host='USPRAP124' }
    @{ Change='CHG0092028'; Host='USPRAP125' }
    @{ Change='CHG0092026'; Host='USPRAP123' }
    @{ Change='CHG0092025'; Host='USPRAP122' }
    @{ Change='CHG0092009'; Host='USPRCM066' }
    @{ Change='CHG0092012'; Host='USPRCM067' }
    @{ Change='CHG0092064'; Host='USPRAP111' }
    @{ Change='CHG0092057'; Host='USPRAP118' }
    @{ Change='CHG0092054'; Host='USPRAP120' }
    @{ Change='CHG0092061'; Host='USPRAP114' }
    @{ Change='CHG0092055'; Host='USPRAP119' }
    @{ Change='CHG0092041'; Host='USPRAP127' }
    @{ Change='CHG0092063'; Host='USPRAP112' }
    @{ Change='CHG0092052'; Host='USPRAP121' }
    @{ Change='CHG0092065'; Host='USPRAP110' }
    @{ Change='CHG0092040'; Host='USPRAP126' }
    @{ Change='CHG0092042'; Host='USPRAP128' }
    @{ Change='CHG0092062'; Host='USPRAP113' }
    @{ Change='CHG0092060'; Host='USPRAP115' }
    @{ Change='CHG0092058'; Host='USPRAP117' }
    @{ Change='CHG0092059'; Host='USPRAP116' }
    # Added by Angela can be deleted
    @{ Change='CHG0092496'; Host='USDVDB016' }
    @{ Change='CHG0092502'; Host='USDVDB017' }
    @{ Change='CHG0092503'; Host='USDVDB018' }
    @{ Change='CHG0092494'; Host='USDVDB078' }
    @{ Change='CHG0092504'; Host='USPRDB013' }
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
    param(
        [Parameter(Mandatory=$true)]
        [string]$VMName
    )

    # 1) Drop " - description"
    $name = $VMName -replace '\s+-\s+.*$', ''

    # 2) Drop spaces but keep hyphens/underscores/digits
    $name = $name -replace '\s+', ''

    # 3) Drop trailing -CHG######## suffix if present
    #    e.g. USDVAP085-CHG0092018 -> USDVAP085
    $name = $name -replace '-CHG\d+$',''

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

    # Derive base name without -CHG########
    # e.g. USDVAP085-CHG0092018 -> USDVAP085
    $shortName = $Hostname
    $baseName  = $shortName -replace '-CHG\d+$',''

    # We'll try Hostname first, then baseName
    $nameCandidates = @($Hostname)
    if ($baseName -and $baseName -ne $Hostname) {
        $nameCandidates += $baseName
    }
    $nameCandidates = $nameCandidates | Select-Object -Unique

    foreach ($name in $nameCandidates) {
        if (-not $name) { continue }

        # 1) Try direct identity
        if (-not $c) {
            try {
                $c = Get-ADComputer -Identity $name `
                                    -Properties OperatingSystem, DNSHostName, DistinguishedName `
                                    -ErrorAction Stop
            } catch { }
        }

        # 2) Fallback – sAMAccountName = name$ or CN = name
        if (-not $c) {
            $samName = ($name -notmatch '\$$') ? "$name$" : $name
            try {
                $c = Get-ADComputer -LDAPFilter "(|(sAMAccountName=$samName)(cn=$name))" `
                                    -Properties OperatingSystem, DNSHostName, DistinguishedName `
                                    -ErrorAction SilentlyContinue
            } catch { }
        }

        if ($c) { break }
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
        param($idOrDn, $wuExceptionName)

        Import-Module ActiveDirectory -ErrorAction Stop

        # Resolve computer first
        $comp = $null
        try {
            $comp = Get-ADComputer -Identity $idOrDn -Properties DistinguishedName,Name -ErrorAction Stop
        } catch {
            $name    = $idOrDn
            $samName = ($name -notmatch '\$$') ? "$name$" : $name
            $comp = Get-ADComputer -LDAPFilter "(|(sAMAccountName=$samName)(cn=$name))" `
                                   -Properties DistinguishedName,Name `
                                   -ErrorAction SilentlyContinue
        }

        if (-not $comp) {
            return @()
        }

        $groups = @()
        try {
            $groups = Get-ADPrincipalGroupMembership -Identity $comp -ErrorAction SilentlyContinue
        } catch { }

        if (-not $groups -or $groups.Count -eq 0) {
            return @()
        }

        $wuPattern = 'app.WindowsUpdate*'
        $wuGroups  = $groups | Where-Object { $_.Name -like $wuPattern }

        if (-not $wuGroups -or $wuGroups.Count -eq 0) {
            return @()
        }

        # Resolve the Exception group DN (if it exists)
        $excDN = $null
        if ($wuExceptionName) {
            $excGroup = $groups | Where-Object { $_.Name -eq $wuExceptionName } | Select-Object -First 1

            if (-not $excGroup) {
                try {
                    $searchBase = (Get-ADRootDSE).DefaultNamingContext
                    $excGroup = Get-ADGroup -Filter "Name -eq '$wuExceptionName'" `
                                            -SearchBase $searchBase `
                                            -Properties DistinguishedName,Name `
                                            -ErrorAction SilentlyContinue
                } catch { }
            }

            if ($excGroup) {
                $excDN = $excGroup.DistinguishedName
            }
        }

        $matched = New-Object System.Collections.Generic.List[object]

        foreach ($g in $wuGroups) {
            $gDN   = $g.DistinguishedName
            $gName = $g.Name
            $isExc = ($excDN -and $gDN -eq $excDN)

            $matched.Add(
                [PSCustomObject]@{
                    Name              = $gName
                    DistinguishedName = $gDN
                    IsException       = $isExc
                }
            ) | Out-Null
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
        param($compDn,$groupDn,$doWhatIf)
        Import-Module ActiveDirectory -ErrorAction Stop

        if ($doWhatIf) {
            Remove-ADGroupMember -Identity $groupDn -Members $compDn -WhatIf
        } else {
            Remove-ADGroupMember -Identity $groupDn -Members $compDn -Confirm:$false
        }
        'OK'
    }

    Invoke-Remote -ComputerName $InfraHop `
                  -ScriptBlock $sb `
                  -ArgumentList @($ComputerDN,$GroupDN,$WhatIf.IsPresent) `
                  -StopOnError
}

function Invoke-ADRemoveComputer {
    param(
        [Parameter(Mandatory=$true)]
        $AdComputer
    )

    $sb = {
        param($dn)
        Import-Module ActiveDirectory -ErrorAction Stop

        try {
            # Clear accidental deletion protection
            Set-ADObject -Identity $dn -ProtectedFromAccidentalDeletion:$false -ErrorAction SilentlyContinue
        } catch { }

        Remove-ADComputer -Identity $dn -Confirm:$false -ErrorAction Stop
        'REMOVED'
    }

    Invoke-Remote -ComputerName $InfraHop -ScriptBlock $sb -ArgumentList @($AdComputer.DistinguishedName) -StopOnError
}



function Invoke-DNSQuery([string]$Zone,[string]$Name,[string]$RRType) {
    $sb = {
        param($zone,$name,$rr)
        Import-Module DNSServer -ErrorAction Stop
        Get-DnsServerResourceRecord -ZoneName $zone -Name $name -RRType $rr -ErrorAction SilentlyContinue
    }
    Invoke-Remote -ComputerName $DnsServer -ScriptBlock $sb -ArgumentList @($Zone,$Name,$RRType)
}

function Invoke-DNSRemove([string]$Zone,[string]$Name,[string]$RRType) {
    $sb = {
        param($zone,$name,$rr)
        Import-Module DNSServer -ErrorAction Stop
        Remove-DnsServerResourceRecord -ZoneName $zone -Name $name -RRType $rr -Force
        'OK'
    }
    Invoke-Remote -ComputerName $DnsServer -ScriptBlock $sb -ArgumentList @($Zone,$Name,$RRType)
}


function Invoke-DNSRemove([string]$Zone,[string]$Name,[string]$RRType) {
    $sb = {
        param($zone,$name,$rr)
        Import-Module DNSServer -ErrorAction Stop
        Remove-DnsServerResourceRecord -ZoneName $zone -Name $name -RRType $rr -Force
        'OK'
    }
    Invoke-Remote -ComputerName $DnsServer -ScriptBlock $sb -ArgumentList @($Zone,$Name,$RRType)
}

# SCOM: remove if registered (ACTION), WhatIf in TEST

function Test-SCOMRegistration {
    param(
        [Parameter(Mandatory=$true)][string]$HostnameFQDN,
        [Parameter(Mandatory=$true)][string]$SCOMServer,
        [switch]$Test
    )

    $result = @{ SCOMStatus=''; ActionsNeeded=[System.Collections.ArrayList]@(); Notes='' }

    if ([string]::IsNullOrWhiteSpace($HostnameFQDN)) {
        $result.SCOMStatus = 'No FQDN available'
        Log-Message "No FQDN available for SCOM check"
        return $result
    }

    # Derive short name and a "base" short name without the -CHG suffix
    $shortName = $HostnameFQDN.Split('.')[0]                  # e.g. USDVAP085-CHG0092018
    $baseName  = $shortName -replace '-CHG\d+$',''            # e.g. USDVAP085

    try {
        Log-Message "Checking SCOM registration for ${HostnameFQDN} (short='$shortName', base='$baseName') on ${SCOMSvr}"

        $scomStatus = Invoke-Remote -ComputerName $SCOMServer -ScriptBlock {
            param($serverFQDN,$shortName,$baseName,$doRemove)

            Import-Module OperationsManager -ErrorAction Stop

            # Get all agents once, then filter locally
            $agents = Get-SCOMAgent -ErrorAction SilentlyContinue
            if (-not $agents) {
                return @{ IsRegistered = $false }
            }

            $agent = $null

            # 1) Exact DNSHostName match on full FQDN
            if ($serverFQDN) {
                $agent = $agents | Where-Object { $_.DNSHostName -eq $serverFQDN } | Select-Object -First 1
            }

            # 2) Try base short name (preferred – avoids -CHG suffix problems)
            if (-not $agent -and $baseName) {
                $agent = $agents |
                    Where-Object {
                        ($_.DNSHostName  -like "$baseName.*") -or
                        ($_.Name         -like "$baseName*")  -or
                        ($_.DisplayName  -like "$baseName*")
                    } |
                    Select-Object -First 1
            }

            # 3) Fall back to raw shortName if still no match
            if (-not $agent -and $shortName) {
                $agent = $agents |
                    Where-Object {
                        ($_.DNSHostName  -like "$shortName*") -or
                        ($_.Name         -like "$shortName*")  -or
                        ($_.DisplayName  -like "$shortName*")
                    } |
                    Select-Object -First 1
            }

            if (-not $agent) {
                return @{ IsRegistered = $false }
            }

            if ($doRemove) {
                try {
                    Remove-SCOMAgent -Agent $agent -Confirm:$false
                    return @{
                        IsRegistered = $true
                        Removed      = $true
                        HealthState  = $agent.HealthState
                        MatchedName  = $agent.Name
                        DNSHostName  = $agent.DNSHostName
                    }
                }
                catch {
                    return @{
                        IsRegistered = $true
                        Removed      = $false
                        Error        = $_.Exception.Message
                        MatchedName  = $agent.Name
                        DNSHostName  = $agent.DNSHostName
                    }
                }
            }
            else {
                return @{
                    IsRegistered = $true
                    Removed      = $false
                    HealthState  = $agent.HealthState
                    MatchedName  = $agent.Name
                    DNSHostName  = $agent.DNSHostName
                }
            }
        } -ArgumentList @($HostnameFQDN, $shortName, $baseName, (-not $Test)) -StopOnError

        if ($scomStatus -and $scomStatus.IsRegistered) {

            $matchedInfo = ""
            if ($scomStatus.DNSHostName -or $scomStatus.MatchedName) {
                $matchedInfo = " (Matched: DNSHostName='$($scomStatus.DNSHostName)' Name='$($scomStatus.MatchedName)')"
            }

            if ($scomStatus.Removed) {
                $result.SCOMStatus = 'Removed from SCOM'
                $result.ActionsNeeded.Add("ACTION: Removed SCOM agent for $HostnameFQDN$matchedInfo") | Out-Null
                Log-Message "ACTION: Removed SCOM agent for ${HostnameFQDN}$matchedInfo"
            }
            elseif ($Test) {
                $result.SCOMStatus = 'Registered - Would remove'
                $result.ActionsNeeded.Add("TEST: Would remove SCOM agent for $HostnameFQDN$matchedInfo") | Out-Null
                Log-Message "TEST: Would remove SCOM agent for ${HostnameFQDN}$matchedInfo"
            }
            elseif ($scomStatus.Error) {
                $result.SCOMStatus = 'Removal failed'
                $result.Notes += "SCOM removal error: $($scomStatus.Error)"
                Log-Message "ERROR: Failed to remove SCOM agent for ${HostnameFQDN}: $($scomStatus.Error)$matchedInfo" 'ERROR'
            }
            else {
                $result.SCOMStatus = 'Registered - Not removed'
                Log-Message "SCOM agent registered for ${HostnameFQDN}$matchedInfo but not removed (doRemove was false?)"
            }
        }
        else {
            $result.SCOMStatus = 'Not registered'
            Log-Message "SCOM agent not found for ${HostnameFQDN} using FQDN/short/base matching (short='$shortName', base='$baseName')"
        }
    }
    catch {
        $result.SCOMStatus = 'Error checking SCOM'
        $result.Notes     += "SCOM Check Error: $_"
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

            try {
                $vmObj = Get-VM -Name $VMName -ErrorAction Stop
            } catch {
                Log-Message "VM ${VMName} not found while attempting shutdown/rename: $_" 'ERROR'
                return $false
            }

            function Add-DecomNoteAndMaybeRename {
    param(
        $vm,
        [string]$chg,
        [switch]$DoIt
    )

    # Snapshot the name up-front so logging still has something sensible
    $vmNameLocal = $vm.Name

    $ts  = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    $who = $Cred.UserName
    $tag = if ([string]::IsNullOrWhiteSpace($chg)) { '' } else { " (CHANGE $chg)" }
    $newNoteLine = "Decom Shutdown${tag}: $ts by $who"

    if ($DoIt) {
        # 1) Update Notes
        try {
            $existing = $vm.Notes
            $updatedNotes = if ([string]::IsNullOrWhiteSpace($existing)) {
                $newNoteLine
            } else {
                "$existing`r`n$newNoteLine"
            }
            Set-VM -VM $vm -Notes $updatedNotes -Confirm:$false | Out-Null
            Log-Message "Appended decom note${tag} to VM Notes for ${vmNameLocal}"
        } catch {
            Log-Message "Failed to update VM Notes for ${vmNameLocal}: $_" 'WARN'
        }

        # 2) Rename VM to include change number (if not already present)
        try {
            if ($chg -and ($vm.Name -notmatch [Regex]::Escape($chg))) {
                $newName = "$($vm.Name)-$chg"
                Set-VM -VM $vm -Name $newName -Confirm:$false | Out-Null
                Log-Message "Renamed VM '${vmNameLocal}' to '${newName}' (appended change)"
                $vmNameLocal = $newName
            } else {
                Log-Message "Rename skipped for ${vmNameLocal} (already contains change or change not supplied)"
            }
        } catch {
            Log-Message "Failed to rename VM ${vmNameLocal}: $_" 'WARN'
        }

        # 3) Remove any tags on the VM (fixed to use TagAssignment objects directly)
        try {
            $tagAssignments = Get-TagAssignment -Entity $vm -ErrorAction SilentlyContinue
            if ($tagAssignments) {
                $countBefore = $tagAssignments.Count
                $tagAssignments | Remove-TagAssignment -Confirm:$false -ErrorAction SilentlyContinue
                Log-Message "Removed ${countBefore} tag assignment(s) from VM ${vmNameLocal}"
            } else {
                Log-Message "No tag assignments found on VM ${vmNameLocal} to remove"
            }
        } catch {
            Log-Message "Failed to remove tag assignments for VM ${vmNameLocal}: $_" 'WARN'
        }

        # 4) Remove VM from any DRS rules
        try {
            $cluster = Get-Cluster -VM $vm -ErrorAction SilentlyContinue
            if ($cluster) {
                $drsRules = Get-DrsRule -Cluster $cluster -ErrorAction SilentlyContinue |
                            Where-Object { $_.VM -and ($_.VM -contains $vm) }

                if ($drsRules -and $drsRules.Count -gt 0) {
                    foreach ($rule in $drsRules) {
                        $ruleName = $rule.Name
                        $currentVMs = @($rule.VM)
                        $vmCount   = $currentVMs.Count

                        if ($vmCount -le 1) {
                            try {
                                Remove-DrsRule -DrsRule $rule -Confirm:$false -ErrorAction Stop
                                Log-Message "Removed DRS rule '${ruleName}' that only referenced VM ${vmNameLocal}"
                            } catch {
                                Log-Message "Failed to remove DRS rule '${ruleName}' for VM ${vmNameLocal}: $_" 'WARN'
                            }
                        }
                        else {
                            try {
                                $remainingVMs = $currentVMs | Where-Object { $_.Id -ne $vm.Id }
                                if ($remainingVMs.Count -eq 0) {
                                    Remove-DrsRule -DrsRule $rule -Confirm:$false -ErrorAction Stop
                                    Log-Message "Removed DRS rule '${ruleName}' after removing last VM ${vmNameLocal}"
                                } else {
                                    Set-DrsRule -DrsRule $rule -VM $remainingVMs -Confirm:$false -ErrorAction Stop
                                    Log-Message "Removed VM ${vmNameLocal} from DRS rule '${ruleName}' (remaining: $($remainingVMs.Count) VM(s))"
                                }
                            } catch {
                                Log-Message "Failed to update DRS rule '${ruleName}' for VM ${vmNameLocal}: $_" 'WARN'
                            }
                        }
                    }
                } else {
                    Log-Message "No DRS rules found containing VM ${vmNameLocal}"
                }
            } else {
                Log-Message "No cluster found for VM ${vmNameLocal} when checking DRS rules" 'WARN'
            }
        } catch {
            Log-Message "Failed to process DRS rules for VM ${vmNameLocal}: $_" 'WARN'
        }
    }
    else {
        Log-Message "TEST: Would append decom note${tag}, rename VM to include change if missing, remove all tag assignments, AND remove VM '${vmNameLocal}' from any DRS rules"
    }
}


    # If VM is already off and we're ensuring consistency, just stamp/rename/remove-tags
    if ($vmObj.PowerState -eq 'PoweredOff' -and $EnsureConsistency) {
        Add-DecomNoteAndMaybeRename -vm $vmObj -chg $ChangeNumber -DoIt:$IsActionMode
        return $true
    }

    if ($vmObj.PowerState -eq 'PoweredOff') {
        Log-Message "VM ${VMName} already PoweredOff; no shutdown needed."
        return $true
    }

    if (-not $IsActionMode) {
        Log-Message "TEST: Would initiate shutdown for ${VMName}"
        return $false
    }

    # Attempt graceful guest shutdown
    $toolsOk = ($vmObj.ExtensionData.Guest.ToolsStatus -match 'toolsOk|toolsOld|toolsSupported')
    if ($toolsOk) {
        try {
            Log-Message "Attempting graceful guest shutdown for ${VMName}"
            Shutdown-VMGuest -VM $vmObj -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
        } catch {
            Log-Message "Graceful shutdown call failed for ${VMName}: $_" 'WARN'
        }
    }
    else {
        Log-Message "VMware Tools not healthy on ${VMName}; skipping guest shutdown"
    }

    # Wait for shutdown, then force if needed
    $deadline = (Get-Date).AddMinutes($GraceMinutes)
    while ((Get-VM -Id $vmObj.Id).PowerState -ne 'PoweredOff' -and (Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 10
    }
    $vmObj = Get-VM -Id $vmObj.Id

    if ($vmObj.PowerState -ne 'PoweredOff') {
        Log-Message "Graceful shutdown timed out; forcing Stop-VM on ${VMName}"
        Stop-VM -VM $vmObj -Confirm:$false | Out-Null
        $deadline = (Get-Date).AddMinutes(2)
        while ((Get-VM -Id $vmObj.Id).PowerState -ne 'PoweredOff' -and (Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 5
        }
        $vmObj = Get-VM -Id $vmObj.Id
    }

    if ($vmObj.PowerState -eq 'PoweredOff') {
        Add-DecomNoteAndMaybeRename -vm $vmObj -chg $ChangeNumber -DoIt:$IsActionMode
        return $true
    }
    else {
        Log-Message "Failed to power off VM ${VMName}" 'ERROR'
        return $false
    }
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
    param(
        [string]$ShortHost,
        [string]$FQDN,
        [string]$IPAddress,
        [string]$DnsZone,
        [switch]$Test
    )

    $result = @{ DNSStatus=''; ActionsNeeded=[System.Collections.ArrayList]@(); Notes='' }

    try {
        if ([string]::IsNullOrWhiteSpace($ShortHost) -and $FQDN) {
            $ShortHost = $FQDN.Split('.')[0]
        }

        # Derive base short host without -CHG########
        $baseShort = if ($ShortHost) { $ShortHost -replace '-CHG\d+$','' } else { $null }

        # We'll try the original and the base (if different)
        $namesToCheck = @()
        if ($ShortHost) { $namesToCheck += $ShortHost }
        if ($baseShort -and $baseShort -ne $ShortHost) { $namesToCheck += $baseShort }
        $namesToCheck = $namesToCheck | Select-Object -Unique

        $aRecord  = $null
        $usedName = $null

        foreach ($name in $namesToCheck) {
            $candidate = Invoke-DNSQuery -Zone $DnsZone -Name $name -RRType 'A'
            if ($candidate) {
                $aRecord  = $candidate
                $usedName = $name
                break
            }
        }

        $ptrRecord = $null
        $ptrInfo   = $null
        if ($IPAddress -and $IPAddress -ne 'Not Resolvable') {
            $ptrInfo = Get-ReversePTRInfo -IPv4 $IPAddress
            if ($ptrInfo) {
                $ptrRecord = Invoke-DNSQuery -Zone $ptrInfo.Zone -Name $ptrInfo.Name -RRType 'PTR'
            }
            else {
                Log-Message "PTR zone not inferred for $IPAddress; skipping PTR removal" 'WARN'
            }
        }

        $displayFqdn = if ($FQDN) {
            $FQDN
        } elseif ($usedName) {
            "$usedName.$DnsZone"
        } elseif ($ShortHost) {
            "$ShortHost.$DnsZone"
        } else {
            "<unknown>"
        }

        if ($aRecord -or $ptrRecord) {
            Log-Message "DNS records found for ${displayFqdn} (A: $([bool]$aRecord), PTR: $([bool]$ptrRecord))"

            if ($Test) {
                $result.DNSStatus = 'TEST: DNS records present - would remove'
                $result.ActionsNeeded.Add("TEST: Would remove DNS A/PTR for $displayFqdn") | Out-Null
            }
            else {
                try {
                    if ($aRecord -and $usedName) {
                        Invoke-DNSRemove -Zone $DnsZone -Name $usedName -RRType 'A' | Out-Null
                        $result.ActionsNeeded.Add("ACTION: Removed DNS A for $displayFqdn") | Out-Null
                        Log-Message "Removed DNS A for ${displayFqdn}"
                    }
                    if ($ptrRecord -and $ptrInfo) {
                        Invoke-DNSRemove -Zone $ptrInfo.Zone -Name $ptrInfo.Name -RRType 'PTR' | Out-Null
                        $result.ActionsNeeded.Add("ACTION: Removed DNS PTR for $IPAddress") | Out-Null
                        Log-Message "Removed DNS PTR $($ptrInfo.Name).$($ptrInfo.Zone)"
                    }
                    $result.DNSStatus = 'ACTION: DNS records removed'
                }
                catch {
                    $err = "ERROR: Failed to remove DNS records: $_"
                    Log-Message $err 'ERROR'
                    $result.ActionsNeeded.Add($err) | Out-Null
                    $result.Notes += "`nDNS Removal Error: $_"
                }
            }
        }
        else {
            Log-Message "No DNS records found for ${displayFqdn}"
            $result.DNSStatus = 'No DNS records found'
        }
    }
    catch {
        $err = "Error checking DNS records: $_"
        Log-Message $err 'ERROR'
        $result.ActionsNeeded.Add($err) | Out-Null
        $result.Notes += "`nDNS Check Error: $_"
    }

    return $result
}

function Remove-WindowsUpdateGroups {
    param(
        [Parameter(Mandatory=$true)]
        $AdComputer,
        [switch]$Test
    )

    $result = @{ IvantiStatus=''; ActionsNeeded=[System.Collections.ArrayList]@(); Notes='' }

    try {
        $compDN   = $AdComputer.DistinguishedName
        $compName = $AdComputer.Name

        if (-not $compDN) {
            $result.IvantiStatus = 'No DN'
            $result.Notes       += 'Computer has no DistinguishedName'
            Log-Message "No DN present for $compName (cannot evaluate WU groups)" 'WARN'
            return $result
        }

        # Use recursive membership logic and skip Exception group from removal
        $groups = Invoke-ADGetWUGroups -ComputerIdentityOrDN $compDN

        if ($groups -and $groups.Count -gt 0) {
            $processed = @()
            foreach ($g in $groups) {
                $gDN   = $g.DistinguishedName
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

            $result.IvantiStatus = if ($Test) {
                'TEST: Would remove from app.WindowsUpdate groups (excluding Exceptions)'
            } else {
                'ACTION: Removed from app.WindowsUpdate groups (excluding Exceptions)'
            }

            if ($processed.Count -gt 0) {
                $result.ActionsNeeded.Add(($processed -join ' | ')) | Out-Null
            }
        }
        else {
            # AD computer exists, but no patch groups with this naming
            $result.IvantiStatus = 'No matching app.WindowsUpdate groups'
            Log-Message "No matching app.WindowsUpdate* groups for $compName (AD computer exists but not in any patching group)"
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
    param(
        [string]$Hostname,
        $ConnectivityResult,
        [string]$DnsZone,
        [string]$vCenterPowerState,
        [switch]$Test
    )

    $result = @{
        DecommissionStatus = ''
        ActionsNeeded      = [System.Collections.ArrayList]@()
        ADExists           = $null
        OS                 = ''
        IPAddress          = ''
        ADStatus           = ''
        Notes              = ''
        FQDN               = ''
        DNSStatus          = ''
    }

    try {
        # Try to find the computer in AD
        $adObj = Invoke-ADQuery -Hostname $Hostname

        if ($adObj) {
            # AD computer exists
            $result.ADExists = $adObj
            $result.OS       = $adObj.OperatingSystem
            $result.ADStatus = 'Exists in AD'
            $result.FQDN     = $adObj.DNSHostName

            $shortHost = if ($result.FQDN) { $result.FQDN.Split('.')[0] } else { $Hostname }

            # Work out an IPv4 address (if possible)
            try {
                $nameForDns = if ($result.FQDN) { $result.FQDN } else { $Hostname }
                $ipObj = [System.Net.Dns]::GetHostAddresses($nameForDns) |
                         Where-Object { $_.AddressFamily -eq 'InterNetwork' } |
                         Select-Object -First 1
                $result.IPAddress = if ($ipObj) { $ipObj.IPAddressToString } else { 'Not Resolvable' }
            }
            catch {
                $result.IPAddress = 'Not Resolvable'
            }

            # Safe to decom only if no ping / shutdown AND vCenter shows PoweredOff
            $safeToDecom = ($ConnectivityResult.Status -in @('Shutdown', 'No ping') -and
                            $vCenterPowerState -eq 'PoweredOff')

            if ($safeToDecom) {
                $result.DecommissionStatus = 'Ready for decommission'

                if ($Test) {
                    # In TEST, just record what we would do
                    $result.ActionsNeeded.Add('TEST: Would remove AD computer account') | Out-Null
                }
                else {
                    # ACTION mode – actually remove the AD computer
                    try {
                        Invoke-ADRemoveComputer -AdComputer $adObj | Out-Null
                        $result.ADStatus = 'Removed from AD'
                        $result.ActionsNeeded.Add('ACTION: AD computer account removed') | Out-Null

                        Log-Message ("ACTION: AD computer account removed for {0} ({1})" -f `
                                     $adObj.Name, $adObj.DistinguishedName)

                        # Important: AD object no longer exists for downstream steps
                        $result.ADExists = $null
                    }
                    catch {
                        $err = "ERROR: Failed to remove AD computer account: {0}" -f $_
                        Log-Message $err 'ERROR'
                        $result.ActionsNeeded.Add($err) | Out-Null
                        $result.Notes += ("`nAD Removal Error: {0}" -f $_)
                    }
                }

                # DNS cleanup (A + PTR)
                $dnsResult = Test-ServerDNSRecords `
                                -ShortHost  $shortHost `
                                -FQDN       $result.FQDN `
                                -IPAddress  $result.IPAddress `
                                -DnsZone    $DnsZone `
                                -Test:$Test

                $result.DNSStatus = $dnsResult.DNSStatus

                if ($dnsResult.ActionsNeeded) {
                    $result.ActionsNeeded.Add(($dnsResult.ActionsNeeded -join ' | ')) | Out-Null
                }

                $result.Notes += $dnsResult.Notes
            }
            else {
                # Not yet safe to decom – report why
                $why = @()
                if ($ConnectivityResult.Status -eq 'Running') { $why += 'responds to ping' }
                if ($vCenterPowerState -ne 'PoweredOff')       { $why += 'vCenter shows PoweredOn' }

                $reason = if ($why) { $why -join ' & ' } else { 'unknown condition' }

                $result.DecommissionStatus = "Not safe to decommission ($reason)"

                $msg = if ($Test) {
                    'TEST: Would wait until: no ping AND PoweredOff'
                }
                else {
                    'ACTION: Must shut down VM and stop ping before removal'
                }

                $result.ActionsNeeded.Add($msg) | Out-Null
            }
        }
        else {
            # Not found in AD at all
            $result.ADStatus = 'Not found in AD'
            Log-Message ("AD lookup failed for {0}" -f $Hostname) 'WARN'
        }
    }
    catch {
        $result.ADStatus = 'Error during AD step'
        Log-Message ("AD step error for {0}: {1}" -f $Hostname, $_) 'ERROR'
    }

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
        TagsRemoved         = ''   # NEW COLUMN
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
            if (-not $Test) {
                # ACTION mode – actually operate on the VM
                if ($row.PowerState -eq 'PoweredOff' -or $row.PowerState -eq 'Unknown') {
                    [void](Invoke-VMShutdownIfNeeded -VMName $row.VMName -IsActionMode -ChangeNumber $row.ChangeNumber -EnsureConsistency)
                }
                else {
                    [void](Invoke-VMShutdownIfNeeded -VMName $row.VMName -IsActionMode -ChangeNumber $row.ChangeNumber -EnsureConsistency)

                    # Refresh VM info (rename may have occurred)
                    $ref = Get-VM -Name "$($row.VMName)*" -ErrorAction SilentlyContinue |
                           Sort-Object -Property Name |
                           Select-Object -Last 1
                    if ($ref) {
                        $row.PowerState = $ref.PowerState.ToString()
                        $row.VMName     = $ref.Name
                    }
                }

                # We attempted tag removal in Invoke-VMShutdownIfNeeded
                $row.TagsRemoved = 'Yes (attempted)'
            }
            else {
                # TEST mode – just log what would happen
                if ($row.PowerState -ne 'Unknown') {
                    Log-Message "TEST: Would shut down (if running), append decom note with change '$($row.ChangeNumber)', rename VM to include change, AND remove all tag assignments for ${($row.VMName)}"
                    $row.TagsRemoved = 'TEST: Would remove tags'
                }
                else {
                    if (-not $row.TagsRemoved) {
                        $row.TagsRemoved = 'Unknown (VM not found)'
                    }
                }
            }
        }
        catch {
            $row.ProcessingErrors += " Step3 error: $_"
            Log-Message "Step3 error on ${($row.VMName)}: $_" 'ERROR'
            if (-not $row.TagsRemoved) { $row.TagsRemoved = 'Error' }
        }
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
    Write-Host ("VMs where tags removed (or would be in TEST): {0}" -f ((
        $rows | Where-Object {
            $_.TagsRemoved -like 'Yes*' -or $_.TagsRemoved -like 'TEST:*'
        }
    ).Count)) -ForegroundColor DarkCyan
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
    'X' {
        Write-Host "Exiting script..." -ForegroundColor Yellow
        Disconnect-VIServer -Server $vCenterServer -Confirm:$false | Out-Null
        try { Stop-Transcript | Out-Null } catch {}
        return
    }
    default { Write-Host "Invalid option." -ForegroundColor Yellow }
}

}

Disconnect-VIServer -Server $vCenterServer -Confirm:$false | Out-Null
Log-Message 'Disconnected from vCenter servers'
try { Stop-Transcript | Out-Null } catch {}
Write-Host "Done."
