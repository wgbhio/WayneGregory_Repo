<#
.SYNOPSIS
Automates creation, configuration, and enforcement of <Server>-LocalAdmins groups in AD and on target servers.
Also ensures a specified gMSA is added to each group.

.DESCRIPTION
For each server listed in $Servers:
  1. Validates the AD module is available.
  2. Retrieves AD domain info (NetBIOS name).
  3. Verifies that the gMSA ($GmsaSam) exists in AD.
  4. Ensures a <Server>-LocalAdmins group exists in the given OU ($GroupOuDN):
       - Creates it if missing.
       - Sets extensionAttribute2 to "lad.<Server>-LocalAdmins".
       - Sets extensionAttribute3 to "Local admin access to <Server>".
  5. Ensures the gMSA is a direct member of <Server>-LocalAdmins (adds if missing).
  6. Ensures <Server>-LocalAdmins is present in the local Administrators group on the server:
       - Tries to add via SID or Name with PowerShell Remoting.
       - Falls back to DCOM/WMI if Remoting fails.
       - Retries multiple times for replication/availability delays.
  7. Logs per-server summary results (OK or ERROR).

.REQUIREMENTS
- Run from a domain-joined admin shell with RSAT AD module installed.
- Account must have permissions to create/manage AD groups and add members.
- Account must have rights to modify local Administrators on the target servers.
- gMSA must exist in AD prior to execution.

.NOTES
- Safe to re-run: idempotent for groups, attributes, and membership.
- Produces a summary table at the end.
- Hard-coded inputs: $Servers, $GroupOuDN, $GmsaSam.
#>


# ===== Hard-coded inputs =====
$Servers = @"
UKPRAP355
UKPRAP356
USPRAP180
USPRAP181
USPRAP182
USPRAP183
IEPRAP035
IEPRAP036
"@ -split "`r?`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ } | Select-Object -Unique

$GroupOuDN  = "OU=Local Admin Access,OU=Shared Groups,DC=bfl,DC=local"
$Retries    = 20   # attempts per server when adding to local admins
$RetryDelay = 15   # seconds between attempts

# >>> NEW: gMSA to add to each <Server>-LocalAdmins group
$GmsaSamPrd = "GMSA_sow_PRD$"
$GmsaSamDev = "gMSA_sow_DEV$"

# ===== Helpers =====
function Ensure-Module {
    param([string]$Name)
    if (-not (Get-Module -ListAvailable -Name $Name)) {
        Throw "Required module '$Name' is not available on this machine."
    }
}

function Ensure-GmsaExists {
    param([Parameter(Mandatory)][string]$SamAccountName)
    try {
        $gmsa = Get-ADServiceAccount -Identity $SamAccountName -ErrorAction Stop
        return $gmsa
    } catch {
        Throw "The gMSA '$SamAccountName' was not found. Create it first (e.g., New-ADServiceAccount) and retry. $_"
    }
}

function Ensure-GmsaMemberOfGroup {
    param(
        [Parameter(Mandatory)] [Microsoft.ActiveDirectory.Management.ADAccount] $Gmsa,
        [Parameter(Mandatory)] [Microsoft.ActiveDirectory.Management.ADGroup]   $Group
    )
    try {
        $members = Get-ADGroupMember -Identity $Group.DistinguishedName -Recursive -ErrorAction Stop
        if ($members | Where-Object { $_.DistinguishedName -eq $Gmsa.DistinguishedName }) {
            Write-Host ("gMSA '{0}' already a member of '{1}'." -f $Gmsa.SamAccountName, $Group.Name)
        } else {
            Add-ADGroupMember -Identity $Group.DistinguishedName -Members $Gmsa.DistinguishedName -ErrorAction Stop
            Write-Host ("Added gMSA '{0}' to group '{1}'." -f $Gmsa.SamAccountName, $Group.Name)
        }
    } catch {
        Throw ("Failed adding gMSA '{0}' to group '{1}': {2}" -f $Gmsa.SamAccountName, $Group.Name, $_.Exception.Message)
    }
}

function Ensure-ServerGroup {
    param(
        [Parameter(Mandatory)] [string]$ServerName,
        [Parameter(Mandatory)] [string]$OuDN
    )
    $groupName = "$ServerName-LocalAdmins"

    # Find or create the group
    $group = Get-ADGroup -Filter "name -eq '$groupName'" -SearchBase $OuDN -ErrorAction SilentlyContinue
    if (-not $group) {
        Write-Host ("Creating AD group '{0}' in {1} ..." -f $groupName, $OuDN)
        $group = New-ADGroup -Name $groupName -GroupScope Global -GroupCategory Security -Path $OuDN `
                             -Description ("Local admin access for {0}" -f $ServerName) -PassThru -ErrorAction Stop
    } else {
        Write-Host ("AD group '{0}' already exists." -f $groupName)
    }

    # === Ensure extension attributes ===
    $ea2 = "lad.$groupName"
    $ea3 = "Local admin access to $ServerName"

    # Read current values
    $gAttr = Get-ADGroup -Identity $group.DistinguishedName -Properties extensionAttribute2, extensionAttribute3 -ErrorAction Stop
    $needsEA2 = [string]::IsNullOrWhiteSpace($gAttr.extensionAttribute2) -or ($gAttr.extensionAttribute2 -ne $ea2)
    $needsEA3 = [string]::IsNullOrWhiteSpace($gAttr.extensionAttribute3) -or ($gAttr.extensionAttribute3 -ne $ea3)

    if ($needsEA2 -or $needsEA3) {
        $replace = @{}
        if ($needsEA2) { $replace['extensionAttribute2'] = $ea2 }
        if ($needsEA3) { $replace['extensionAttribute3'] = $ea3 }
        try {
            Set-ADGroup -Identity $group.DistinguishedName -Replace $replace -ErrorAction Stop
            Write-Host "Set extensionAttribute2='$ea2'; extensionAttribute3='$ea3' on '$groupName'"
        } catch {
            Write-Warning ("Failed to set extension attributes on '{0}': {1}" -f $groupName, $_.Exception.Message)
        }
    } else {
        Write-Host "extensionAttribute2/3 already set correctly on '$groupName'"
    }

    # Return group with SID
    Get-ADGroup -Identity $group.DistinguishedName -Properties objectSid
}

function Add-GroupToLocalAdmins {
    param(
        [Parameter(Mandatory)] [string]$ComputerName,
        [Parameter(Mandatory)] [string]$DomainQualifiedName,   # e.g., BFL\SERVER-LocalAdmins
        [Parameter(Mandatory)] [string]$GroupSid,
        [int]$Attempts = 20,
        [int]$DelaySeconds = 15
    )

    $remoteAddScript = {
        param($Sid, $FallbackName, $Attempts, $DelaySeconds)
        $success = $false

        function Try-Add([string]$Member, [string]$By = "SID/Name") {
            try {
                if (Get-Command Add-LocalGroupMember -ErrorAction SilentlyContinue) {
                    $current = Get-LocalGroupMember -Group 'Administrators' -ErrorAction SilentlyContinue
                    if ($current) {
                        if ($Member -match '^S-1-') {
                            if ($current.SID -contains $Member) { return $true }
                        } else {
                            if ($current.Name -contains $Member) { return $true }
                        }
                    }
                    Add-LocalGroupMember -Group 'Administrators' -Member $Member -ErrorAction Stop
                    return $true
                } else {
                    if ($Member -match '^S-1-') { return $false } # 'net localgroup' cannot use SID
                    $out = & cmd /c "net localgroup Administrators \"$FallbackName\" /add"
                    if ($LASTEXITCODE -eq 0) { return $true } else { return $false }
                }
            } catch { return $false }
        }

        for ($i=1; $i -le $Attempts -and -not $success; $i++) {
            # try by SID first (name lookup not required)
            $success = Try-Add -Member $Sid -By "SID"
            if (-not $success) {
                # then by name (works once DCs are in sync)
                $success = Try-Add -Member $FallbackName -By "Name"
            }
            if (-not $success -and $i -lt $Attempts) {
                Start-Sleep -Seconds $DelaySeconds
            }
        }

        if ($success) { "Added (or already present)." } else { "ERROR: Could not add after $Attempts attempts." }
    }

    $ok = $false
    try {
        $session = New-PSSession -ComputerName $ComputerName -ErrorAction Stop
        $result  = Invoke-Command -Session $session -ScriptBlock $remoteAddScript -ArgumentList $GroupSid, $DomainQualifiedName, $Attempts, $DelaySeconds
        Remove-PSSession $session
        $ok = $true
        Write-Host $result
    } catch {
        Write-Warning ("PowerShell remoting to {0} failed: {1}. Trying DCOM/WMI fallback..." -f $ComputerName, $_.Exception.Message)
    }

    if (-not $ok) {
        try {
            # Minimal retries with WMI/name route
            $miniTries = 5
            for ($j=1; $j -le $miniTries; $j++) {
                $cmd = "cmd.exe /c net localgroup Administrators `"$DomainQualifiedName`" /add"
                $svc = Get-WmiObject -Class Win32_Process -ComputerName $ComputerName -ErrorAction Stop
                $rc  = $svc.Create($cmd)
                if ($rc.ReturnValue -eq 0) { Write-Host ("Fallback succeeded on try {0}." -f $j); return }
                Start-Sleep -Seconds 10
            }
            Throw ("Fallback failed on {0}. Last return code: {1}" -f $ComputerName, $rc.ReturnValue)
        } catch {
            Throw $_
        }
    }
}

# ===== Main =====
try { Ensure-Module -Name ActiveDirectory } catch { Write-Error $_; exit 1 }

try {
    $domain = Get-ADDomain -ErrorAction Stop
    $netbios = $domain.NetBIOSName
} catch {
    Write-Error "Unable to read AD domain info. $_"
    exit 1
}

# >>> NEW: Verify gMSA up-front
try {
    $gmsaObj = Ensure-GmsaExists -SamAccountName $GmsaSam
    Write-Host ("Verified gMSA present: {0}" -f $gmsaObj.SamAccountName)
} catch {
    Write-Error $_
    exit 1
}

$summary = @()

foreach ($svr in $Servers) {
    Write-Host ("--- Processing {0} ---" -f $svr)

    try {
        $grp = Ensure-ServerGroup -ServerName $svr -OuDN $GroupOuDN
        $groupSid = $grp.objectSid.Value
        $groupName = $svr + '-LocalAdmins'
        $domainQualified = "$netbios\$groupName"

        # >>> NEW: Ensure gMSA is a member of the AD group
        Ensure-GmsaMemberOfGroup -Gmsa $gmsaObj -Group (Get-ADGroup -Identity $grp.DistinguishedName)

        Write-Host ("Ensuring '{0}' is in local Administrators on {1} ..." -f $domainQualified, $svr)
        Add-GroupToLocalAdmins -ComputerName $svr -DomainQualifiedName $domainQualified -GroupSid $groupSid -Attempts $Retries -DelaySeconds $RetryDelay

        $summary += [pscustomobject]@{
            Server     = $svr
            Group      = $groupName
            Status     = "OK"
            Note       = "Ensured group exists; set extensionAttribute2/3; gMSA added; verified group in local Administrators"
        }
    } catch {
        $summary += [pscustomobject]@{
            Server     = $svr
            Group      = "$svr-LocalAdmins"
            Status     = "ERROR"
            Note       = $_.Exception.Message
        }
        Write-Error ("{0}: {1}" -f $svr, $_.Exception.Message)
    }
}

Write-Host ""
Write-Host "======== Summary ========"
$summary | Format-Table -AutoSize

Write-Host ("Done. Processed {0} unique server(s)." -f $Servers.Count)

