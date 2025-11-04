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

# List of VLANs to add
$vlansToAdd = @(
    "UCS_FI_3000", "UCS_FI_3001", "UCS_FI_3002", "UCS_FI_3003", "UCS_FI_3004", "UCS_FI_3005", "UCS_FI_3006", "UCS_FI_3007",
    "UCS_FI_3008", "UCS_FI_3009", "UCS_FI_3010", "UCS_FI_3011", "UCS_FI_3012", "UCS_FI_3013", "UCS_FI_3014", "UCS_FI_3015",
    "UCS_FI_3016", "UCS_FI_3017", "UCS_FI_3018", "UCS_FI_3019", "UCS_FI_3020", "UCS_FI_3021", "UCS_FI_3022", "UCS_FI_3023",
    "UCS_FI_3024", "UCS_FI_3025", "UCS_FI_3026", "UCS_FI_3027", "UCS_FI_3028", "UCS_FI_3029", "UCS_FI_3030", "UCS_FI_3031",
    "UCS_FI_3032", "UCS_FI_3033", "UCS_FI_3034", "UCS_FI_3035", "UCS_FI_3036", "UCS_FI_3037", "UCS_FI_3038", "UCS_FI_3039",
    "UCS_FI_3040", "UCS_FI_3041", "UCS_FI_3042", "UCS_FI_3043", "UCS_FI_3044", "UCS_FI_3045", "UCS_FI_3046", "UCS_FI_3047",
    "UCS_FI_3048", "UCS_FI_3049", "UCS_FI_3050", "UCS_FI_3051", "UCS_FI_3052", "UCS_FI_3053", "UCS_FI_3054", "UCS_FI_3055",
    "UCS_FI_3056", "UCS_FI_3057", "UCS_FI_3058", "UCS_FI_3059", "UCS_FI_3060", "UCS_FI_3061", "UCS_FI_3062", "UCS_FI_3063",
    "UCS_FI_3064", "UCS_FI_3065", "UCS_FI_3066", "UCS_FI_3067", "UCS_FI_3068", "UCS_FI_3069", "UCS_FI_3070", "UCS_FI_3071",
    "UCS_FI_3072", "UCS_FI_3073", "UCS_FI_3074", "UCS_FI_3075", "UCS_FI_3076", "UCS_FI_3077", "UCS_FI_3078", "UCS_FI_3079",
    "UCS_FI_3080", "UCS_FI_3081", "UCS_FI_3082", "UCS_FI_3083", "UCS_FI_3084", "UCS_FI_3085", "UCS_FI_3086", "UCS_FI_3087",
    "UCS_FI_3088", "UCS_FI_3089", "UCS_FI_3090", "UCS_FI_3091", "UCS_FI_3092", "UCS_FI_3093", "UCS_FI_3094", "UCS_FI_3095",
    "UCS_FI_3096", "UCS_FI_3097", "UCS_FI_3098", "UCS_FI_3099", "UCS_FI_3507"
)

# Target vNIC templates
$targetVnicTemplates = @("PoC_ESXdata_A", "PoC_ESXdata_B")

foreach ($targetVnicTemplateName in $targetVnicTemplates) {
    $vnicTemplate = Get-UcsVnicTemplate -Name $targetVnicTemplateName -Ucs $ucsHandle
    foreach ($vlanName in $vlansToAdd) {
        try {
            $vnicTemplate | Add-UcsVnicInterface -Name $vlanName -DefaultNet $false -ModifyPresent
            Write-Output "VLAN '$vlanName' added to vNIC template '$targetVnicTemplateName'."
        } catch {
            Write-Error "Failed to add VLAN '$vlanName' to vNIC template '$targetVnicTemplateName': $_"
        }
    }
}

# Disconnect session
Disconnect-Ucs -Ucs $ucsHandle