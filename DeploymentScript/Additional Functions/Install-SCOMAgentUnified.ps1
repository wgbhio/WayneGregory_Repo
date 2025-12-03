<# ======================================================================
   Install-SCOMBulk.ps1
   - Install, Remove, or Check SCOM agents on multiple servers
   - Prompts for credential
   - Runs locally on SCOM server OR remotely via WinRM
====================================================================== #>

param(
    [string]$SCOMServer = "UKPRAP184.bfl.local"
)

# ================================================================
# Action + static server list + creds
# ================================================================
$Action = Read-Host "Action (Install / Remove / Check)"
if ($Action -notin @('Install','Remove','Check')) {
    Write-Warning "Invalid action. Must be Install, Remove, or Check."
    exit
}

# Fixed list of servers to process
$Servers = @(
    'IEPRAP035',
    'IEPRAP036',
    'UKDVAP339',
    'UKPRAP351',
    'UKPRAP359',
    'UKPRAP360',
    'UKPRDB214',
    'UKPRWS107',
    'USPRAP180',
    'USPRAP181',
    'USPRAP182',
    'USPRAP183',
    'USPRDB117'
)

$Cred = Get-Credential -Message "Enter credential for remote SCOM server connection (e.g. bfl\a1_wg7)"

# ================================================================
# Helper: Resolve FQDN
# ================================================================
function Resolve-FQDN {
    param([string]$Name)
    try {
        $obj = Get-ADComputer $Name -Properties DNSHostName -ErrorAction Stop
        return $obj.DNSHostName
    } catch {
        return "$Name.bfl.local"
    }
}

# ================================================================
# Helper: Ensure SCOM MG connection
# ================================================================
function Connect-MG {
    param([string]$MgmtSrv)
    if (-not (Get-SCOMManagementGroupConnection -ErrorAction SilentlyContinue)) {
        New-SCOMManagementGroupConnection -ComputerName $MgmtSrv | Out-Null
    }
}

# ================================================================
# Helper: Get Agent object
# ================================================================
function Get-AgentObject {
    param([string]$FQDN,[string]$Short)
    return Get-SCOMAgent | Where-Object {
        $_.DNSHostName -eq $FQDN -or
        $_.DNSHostName -eq $Short -or
        $_.Name        -eq $FQDN -or
        $_.Name        -eq $Short
    }
}

# ================================================================
# Helper: Approve pending
# ================================================================
function Approve-Pending {
    param([string]$Short)

    Start-Sleep 10

    $pending = Get-SCOMPendingManagement | Where-Object { $_.Computer -like "$Short*" }

    if ($pending) {
        Approve-SCOMPendingManagement -Instance $pending -ErrorAction SilentlyContinue
        return $true
    }

    return $false
}

# ================================================================
# LOCAL EXECUTION BLOCK (runs directly on SCOM server)
# ================================================================
function Invoke-LocalSCOM {
    param(
        [string]$Action,
        [string]$ServerName,
        [string]$SCOMServer
    )

    Import-Module OperationsManager -ErrorAction Stop
    Connect-MG -MgmtSrv $SCOMServer

    $mgmtObj = Get-SCOMManagementServer -Name $SCOMServer -ErrorAction Stop
    $fqdn    = Resolve-FQDN $ServerName
    $agentObj = Get-AgentObject -FQDN $fqdn -Short $ServerName

    switch ($Action) {

        'Check' {
            if ($agentObj) { return "[$ServerName] REGISTERED" }
            else           { return "[$ServerName] NOT REGISTERED" }
        }

        'Install' {
            if ($agentObj) {
                Enable-SCOMAgentProxy -Agent $agentObj -ErrorAction SilentlyContinue
                return "[$ServerName] Already registered."
            }

            Install-SCOMAgent -DNSHostName $fqdn -PrimaryManagementServer $mgmtObj -ErrorAction Stop

            if (Approve-Pending -Short $ServerName) {
                Start-Sleep 8
                $agentObj = Get-AgentObject -FQDN $fqdn -Short $ServerName

                if ($agentObj) {
                    Enable-SCOMAgentProxy -Agent $agentObj -ErrorAction SilentlyContinue
                    return "[$ServerName] SUCCESS: Installed + Registered"
                }
            }

            return "[$ServerName] WARNING: Installed but not visible yet"
        }

        'Remove' {
            if (-not $agentObj) { return "[$ServerName] No agent found." }
            Uninstall-SCOMAgent -Agent $agentObj -Confirm:$false -ErrorAction Stop
            return "[$ServerName] SUCCESS: Removed"
        }
    }
}

# ================================================================
# REMOTE EXECUTION BLOCK (runs over WinRM → SCOM server)
# ================================================================
function Invoke-RemoteSCOM {
    param(
        [string]$Action,
        [string]$ServerName,
        [string]$SCOMServer,
        [pscredential]$Credential
    )

    $sb = {
        param($Action,$ServerName,$SCOMServer)

        # === Self-contained inner function call ===
        Import-Module OperationsManager -ErrorAction Stop

        function Resolve-FQDN {
            param([string]$Name)
            try {
                $obj = Get-ADComputer $Name -Properties DNSHostName -ErrorAction Stop
                return $obj.DNSHostName
            } catch {
                return "$Name.bfl.local"
            }
        }

        function Connect-MG {
            param([string]$MgmtSrv)
            if (-not (Get-SCOMManagementGroupConnection -ErrorAction SilentlyContinue)) {
                New-SCOMManagementGroupConnection -ComputerName $MgmtSrv | Out-Null
            }
        }

        function Get-AgentObject {
            param([string]$FQDN,[string]$Short)
            return Get-SCOMAgent | Where-Object {
                $_.DNSHostName -eq $FQDN -or
                $_.DNSHostName -eq $Short -or
                $_.Name        -eq $FQDN -or
                $_.Name        -eq $Short
            }
        }

        function Approve-Pending {
            param([string]$Short)
            Start-Sleep 10
            $pending = Get-SCOMPendingManagement | Where-Object { $_.Computer -like "$Short*" }
            if ($pending) {
                Approve-SCOMPendingManagement -Instance $pending -ErrorAction SilentlyContinue
                return $true
            }
            return $false
        }

        # === EXECUTION ===
        Connect-MG -MgmtSrv $SCOMServer
        $mgmtObj = Get-SCOMManagementServer -Name $SCOMServer -ErrorAction Stop
        $fqdn    = Resolve-FQDN $ServerName
        $agentObj = Get-AgentObject -FQDN $fqdn -Short $ServerName

        switch ($Action) {
            'Check' {
                if ($agentObj) { return "[$ServerName] REGISTERED" }
                else           { return "[$ServerName] NOT REGISTERED" }
            }

            'Install' {
                if ($agentObj) {
                    Enable-SCOMAgentProxy -Agent $agentObj -ErrorAction SilentlyContinue
                    return "[$ServerName] Already registered."
                }

                Install-SCOMAgent -DNSHostName $fqdn -PrimaryManagementServer $mgmtObj -ErrorAction Stop

                if (Approve-Pending -Short $ServerName) {
                    Start-Sleep 8
                    $agentObj = Get-AgentObject -FQDN $fqdn -Short $ServerName

                    if ($agentObj) {
                        Enable-SCOMAgentProxy -Agent $agentObj -ErrorAction SilentlyContinue
                        return "[$ServerName] SUCCESS: Installed + Registered"
                    }
                }
                return "[$ServerName] WARNING: Installed but not visible yet"
            }

            'Remove' {
                if (-not $agentObj) { return "[$ServerName] No agent found." }
                Uninstall-SCOMAgent -Agent $agentObj -Confirm:$false -ErrorAction Stop
                return "[$ServerName] SUCCESS: Removed"
            }
        }
    }

    Invoke-Command -ComputerName $SCOMServer -Credential $Credential `
        -ScriptBlock $sb `
        -ArgumentList $Action,$ServerName,$SCOMServer
}

# ================================================================
# MAIN LOOP – Handles multiple servers
# ================================================================
Write-Host "`n========== PROCESSING ==========" -ForegroundColor Cyan

foreach ($server in $Servers) {

    Write-Host "`n--- $server ---" -ForegroundColor Yellow

    $isLocalSCOM = ($env:COMPUTERNAME -eq ($SCOMServer.Split('.')[0]))

    if ($isLocalSCOM) {
        $result = Invoke-LocalSCOM -Action $Action -ServerName $server -SCOMServer $SCOMServer
    } else {
        $result = Invoke-RemoteSCOM -Action $Action -ServerName $server -SCOMServer $SCOMServer -Credential $Cred
    }

    Write-Host $result -ForegroundColor Green
}

Write-Host "`nDONE.`n" -ForegroundColor Cyan
