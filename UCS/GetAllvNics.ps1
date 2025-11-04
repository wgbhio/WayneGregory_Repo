# Set the configuration to allow multiple default UCS connections
Set-UcsPowerToolConfiguration -SupportMultipleDefaultUcs $true

# Import the Cisco UCS Manager module
Import-Module Cisco.UcsManager

$password = ConvertTo-SecureString "William22!99!99!" -AsPlainText -Force
$cred = New-Object System.Management.Automation.PSCredential("ucs-TACACS_Domain\a1_wg7", $password)
$ucsHandle = Connect-Ucs -Name 10.184.1.22 -Credential $cred

if ($ucsHandle -eq $null) {
    Write-Error "Failed to connect to UCS. Please check your credentials and UCS Manager address."
    exit
}

# Retrieve all vNICs
$vnics = Get-UcsVnic -Ucs $ucsHandle

# Debugging: Output the total number of vNICs retrieved
Write-Output "Total vNICs retrieved: $($vnics.Count)"

# Filter and display all vNICs that start with "PoC"
$pocVnics = $vnics | Where-Object { $_.Name -like "PoC*" }

# Debugging: Output the number of vNICs that match the filter
Write-Output "vNICs starting with 'PoC': $($pocVnics.Count)"

foreach ($vnic in $pocVnics) {
    Write-Output "vNIC Name: $($vnic.Name)"
    Write-Output "Switch ID: $($vnic.SwitchId)"
    Write-Output "Ident Pool Name: $($vnic.IdentPoolName)"
    Write-Output "Stats Policy Name: $($vnic.StatsPolicyName)"
    Write-Output "QoS Policy Name: $($vnic.QosPolicyName)"
    Write-Output "Template Type: $($vnic.TemplType)"
    Write-Output "Adaptor Profile Name: $($vnic.AdaptorProfileName)"
    Write-Output "-----------------------------"
}

# Disconnect session
Disconnect-Ucs -Ucs $ucsHandle