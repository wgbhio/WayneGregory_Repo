# Define target servers
$servers = @(
    "UKPRDB101.bfl.local"
)

# Define the service name
$serviceName = "HealthService"

# Prompt for credentials
$cred = Get-Credential -UserName "bfl\a1_wg7" -Message "Enter password for bfl\a1_wg7"

# Create an array to hold results
$results = @()

foreach ($server in $servers) {
    try {
        Write-Host "Restarting MOM Agent on $server ..." -ForegroundColor Cyan

        # Restart the service remotely using alternate credentials
        Invoke-Command -ComputerName $server -Credential $cred -ScriptBlock {
            param($svc)
            Restart-Service -Name $svc -Force -ErrorAction Stop
            Get-Service -Name $svc | Select-Object Name, Status
        } -ArgumentList $serviceName -ErrorAction Stop | ForEach-Object {
            $results += [PSCustomObject]@{
                Server = $server
                Service = $_.Name
                Status = $_.Status
                Result = "Success"
            }
        }
    }
    catch {
        $results += [PSCustomObject]@{
            Server = $server
            Service = $serviceName
            Status = "Unknown"
            Result = "Failed - $($_.Exception.Message)"
        }
    }
}

# Display results
$results | Format-Table -AutoSize

# Optional: export to CSV
# $results | Export-Csv "C:\Reports\MOM_Agent_Restart_Results.csv" -NoTypeInformation
