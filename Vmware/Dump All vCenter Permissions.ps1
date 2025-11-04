# Requires: VMware PowerCLI module

# List of vCenters
$vcsaList = @(
    "ukprim098.bfl.local",
    "usprim004.bfl.local",
    "ieprim018.bfl.local"
)

# Output paths
$outputDir = "C:\Reports\vCenterPermissions"
$combinedCsv = Join-Path -Path $outputDir -ChildPath "All_vCenters_Permissions.csv"

New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
$allPermissions = @()

foreach ($vcsa in $vcsaList) {
    Write-Host "Connecting to $vcsa..." -ForegroundColor Cyan
    try {
        Connect-VIServer -Server $vcsa -WarningAction SilentlyContinue

        $permissions = Get-VIPermission | ForEach-Object {
            [PSCustomObject]@{
                VCenter     = $vcsa
                Entity      = $_.Entity
                Principal   = $_.Principal
                Role        = $_.Role
                Propagated  = $_.Propagate
            }
        }

        $allPermissions += $permissions

        Write-Host "Permissions collected from $vcsa" -ForegroundColor Green
    } catch {
        Write-Warning "Failed to connect or export from ${vcsa}: $_"
    } finally {
        Disconnect-VIServer -Server * -Force -Confirm:$false
    }
}

# Export combined CSV
$allPermissions | Export-Csv -Path $combinedCsv -NoTypeInformation
Write-Host "Combined permissions exported to $combinedCsv" -ForegroundColor Green
