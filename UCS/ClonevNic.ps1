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

# Define source and target names
$vnicTemplates = @(
    @{ Source = "B2NGPU_ESXmgtN_A"; Target = "PoC_ESXmgmt_A" },
    @{ Source = "B2NGPU_ESXmgtN_B"; Target = "PoC_ESXmgmt_B" },
    @{ Source = "B2NGPU_ESXvmot_A"; Target = "PoC_ESXvmot_A" },
    @{ Source = "B2NGPU_ESXvmot_B"; Target = "PoC_ESXvmot_B" },
    @{ Source = "B2NGPU_ESXdata_A"; Target = "PoC_ESXdata_A" },
    @{ Source = "B2NGPU_ESXdata_B"; Target = "PoC_ESXdata_B" },
    @{ Source = "B2NGPU_ESXdmz_A"; Target = "PoC_ESXdmz_A" },
    @{ Source = "B2NGPU_ESXdmz_B"; Target = "PoC_ESXdmz_B" },
    @{ Source = "B2NGPU_ESXscsi_A"; Target = "PoC_ESXiscsi_A" },
    @{ Source = "B2NGPU_ESXscsi_B"; Target = "PoC_ESXiscsi_B" }
)

foreach ($template in $vnicTemplates) {
    $sourceVnicTemplateName = $template.Source
    $targetVnicTemplateName = $template.Target

    # Get the source vNIC template
    $sourceVnicTemplate = Get-UcsVnicTemplate -Name $sourceVnicTemplateName -Ucs $ucsHandle

    if ($sourceVnicTemplate -eq $null) {
        Write-Error "Failed to retrieve source vNIC template '$sourceVnicTemplateName'. Please check the template name."
        Disconnect-Ucs -Ucs $ucsHandle
        exit
    }

    # Check if the target vNIC template already exists
    $existingTargetVnicTemplate = Get-UcsVnicTemplate -Name $targetVnicTemplateName -Ucs $ucsHandle
    if ($existingTargetVnicTemplate -eq $null) {
        # Recreate the new template with same settings under sub-organization "LON_POC"
        Add-UcsVnicTemplate -Name $targetVnicTemplateName `
            -SwitchId $sourceVnicTemplate.SwitchId `
            -IdentPoolName $sourceVnicTemplate.IdentPoolName `
            -StatsPolicyName $sourceVnicTemplate.StatsPolicyName `
            -QosPolicyName $sourceVnicTemplate.QosPolicyName `
            -TemplType $sourceVnicTemplate.TemplType `
            -Org "org-root/org-LON_POC"

        Write-Output "vNIC template '$sourceVnicTemplateName' cloned to '$targetVnicTemplateName' successfully under 'LON_POC'."
    } else {
        Write-Output "vNIC template '$targetVnicTemplateName' already exists. Skipping creation."
    }

    # Copy VLANs from source to target vNIC template
    $vlans = Get-UcsVlan -Ucs $ucsHandle | Where-Object { $_.Name -like "$sourceVnicTemplateName*" }
    foreach ($vlan in $vlans) {
        try {
            Add-UcsVlan -Name $vlan.Name -Id $vlan.Id -Org "org-root/org-LON_POC"
            Write-Output "VLAN '$($vlan.Name)' copied to 'LON_POC'."
        } catch {
            Write-Error "Failed to copy VLAN '$($vlan.Name)': $_"
        }
    }
}

# Retrieve and display all vNICs that start with "PoC"
$vnics = Get-UcsVnic -Ucs $ucsHandle | Where-Object { $_.Name -like "PoC*" }

foreach ($vnic in $vnics) {
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