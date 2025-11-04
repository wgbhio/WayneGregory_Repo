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

# Define the service profile template name and target organization
$serviceProfileTemplateName = "LON_B2NGPU_SPT_V3"
$targetOrg = "org-root/org-LON_POC"

# Get the service profile template
$serviceProfileTemplate = Get-UcsServiceProfileTemplate -Name $serviceProfileTemplateName -Ucs $ucsHandle

if ($serviceProfileTemplate -eq $null) {
    Write-Error "Failed to retrieve service profile template '$serviceProfileTemplateName'. Please check the template name."
    Disconnect-Ucs -Ucs $ucsHandle
    exit
}

# Copy the service profile template to the target organization
try {
    $newTemplateName = "${serviceProfileTemplateName}_Copy"
    Copy-UcsServiceProfileTemplate -SourceServiceProfileTemplate $serviceProfileTemplate -TargetOrg $targetOrg -NewName $newTemplateName -Ucs $ucsHandle
    Write-Output "Service profile template '$serviceProfileTemplateName' copied to '$targetOrg' successfully as '$newTemplateName'."
    
    # Remove the original service profile template
    Remove-UcsServiceProfileTemplate -Name $serviceProfileTemplateName -Ucs $ucsHandle -Force
    Write-Output "Original service profile template '$serviceProfileTemplateName' removed successfully."
} catch {
    Write-Error "Failed to copy or remove service profile template '$serviceProfileTemplateName': $_"
}

# Disconnect session
Disconnect-Ucs -Ucs $ucsHandle