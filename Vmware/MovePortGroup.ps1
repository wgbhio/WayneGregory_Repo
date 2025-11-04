# Variables
$vcServer        = 'usprim004.bfl.local'
$sourcePortGroup = 'VLAN_998_DMZ'
$targetPortGroup = 'MAR_VLAN_3803_DMZ_OUT'
$hostNameFilter  = 'mar*'

# 0. (Optional) Ignore certificate warnings
Set-PowerCLIConfiguration -InvalidCertificateAction Ignore -Confirm:$false | Out-Null

# 1. Connect to vCenter using the logged-in user's Windows credentials (suppress output)
Connect-VIServer -Server $vcServer | Out-Null

# 2. Locate all VMs on matching hosts that are currently on the source portgroup
$vms = Get-VMHost -Name $hostNameFilter |
       Get-VM |
       Where-Object {
           (Get-NetworkAdapter -VM $_).NetworkName -contains $sourcePortGroup
       }

if (-not $vms) {
    Write-Warning "No VMs found on hosts matching '$hostNameFilter' with portgroup '$sourcePortGroup'."
    Disconnect-VIServer -Server $vcServer -Confirm:$false | Out-Null
    return
}

# 3. Prompt for confirmation before moving
$vms | ForEach-Object {
    $vm = $_
    $confirm = Read-Host "Move VM '$($vm.Name)' from '$sourcePortGroup' to '$targetPortGroup'? (Y/N)"
    if ($confirm -match '^[Yy]') {
        # 4. Perform the move: change each NIC on the source portgroup to the target portgroup
        Get-NetworkAdapter -VM $vm |
          Where-Object { $_.NetworkName -eq $sourcePortGroup } |
          ForEach-Object {
              Set-NetworkAdapter -NetworkAdapter $_ `
                                -NetworkName $targetPortGroup `
                                -Confirm:$false
          }
        # 5. Retrieve and display the updated adapter(s)
        Get-NetworkAdapter -VM $vm |
          Where-Object { $_.NetworkName -eq $targetPortGroup } |
          Select-Object @{Name='VM';Expression={$_.VMName}}, Name, NetworkName |
          Format-Table -AutoSize

        Write-Host "VM '$($vm.Name)' has been moved to portgroup '$targetPortGroup'."
    }
    else {
        Write-Host "Skipping VM '$($vm.Name)'."
    }
}

# 6. Disconnect when done
Disconnect-VIServer -Server $vcServer -Confirm:$false | Out-Null