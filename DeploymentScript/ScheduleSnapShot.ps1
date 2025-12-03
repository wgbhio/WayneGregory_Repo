param(
    [string]$vCenter  = "usprim004.bfl.local",
    [string]$User     = "bfl\a1_wg7",
    [string]$Password = "William22!99!99!",
    [string[]]$VMs    = @("USDVDB045","USDVDB046","USDVDB047")
)

# Optional: create a dated snapshot name
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm"
$snapName  = "Daily_0330_$timestamp"

# Connect to vCenter
Connect-VIServer -Server $vCenter -User $User -Password $Password | Out-Null

foreach ($vmName in $VMs) {
    try {
        $vm = Get-VM -Name $vmName -ErrorAction Stop
        Write-Host "Creating snapshot '$snapName' on VM $vmName..."
        New-Snapshot -VM $vm -Name $snapName -Description "Scheduled snapshot at 03:30" -Quiesce:$false -Memory:$false
    }
    catch {
        Write-Warning "Failed to snapshot VM '$vmName': $_"
    }
}

#Disconnect-VIServer -Server $vCenter -Confirm:$false
