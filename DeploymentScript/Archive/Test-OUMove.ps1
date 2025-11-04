# Prompt for computer name and AD creds
$vmName   = Read-Host "Enter the computer name (without $ sign)"
$adServer = "USPRDC036.bfl.local"
$targetOU = "OU=Application Servers,OU=Servers & Exceptions,OU=Hawthorne,OU=Accounts Computer,DC=bfl,DC=local"

Write-Host "Prompting for AD credentials to use against $adServer..."
$cred = Get-Credential

try {
    # Ensure AD module is loaded
    Import-Module ActiveDirectory -ErrorAction Stop
}
catch {
    Write-Error "ActiveDirectory module not available. Run on a system with RSAT / AD tools."
    return
}

# Find the computer object (use the $ in sAMAccountName for computer accounts)
$comp = Get-ADComputer -Filter "sAMAccountName -eq '$vmName$'" -Server $adServer -Credential $cred

if (-not $comp) {
    Write-Warning "Computer '$vmName' not found in AD on $adServer."
    return
}

Write-Host "Found computer object DN:" $comp.DistinguishedName -ForegroundColor Cyan

# Attempt to move
try {
    Move-ADObject -Identity $comp.DistinguishedName -TargetPath $targetOU -Server $adServer -Credential $cred -ErrorAction Stop
    Write-Host "Successfully moved '$vmName' to:" $targetOU -ForegroundColor Green
}
catch {
    Write-Error "Failed to move '$vmName' to OU. $_"
}
