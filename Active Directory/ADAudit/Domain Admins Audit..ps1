<# 
Creates an Excel workbook:
- Sheet "Domain Admins": all members (users & groups), flattened recursively
- One sheet per user member: full group membership + group details
- Index sheet with hyperlinks to user tabs; user tabs have "Back to Index"
#>

param(
    [string]$OutputPath = "C:\Users\GrWay\OneDrive\OneDrive - Beazley Group\Documents\Reports\Domain Admins Audit.xlsx",
    [string]$DomainAdminsGroup = "Domain Admins"
)

#--- Prereqs -------------------------------------------------------------------
try { Import-Module ActiveDirectory -ErrorAction Stop }
catch { throw "ActiveDirectory module not found. Install RSAT or run on a DC/management host." }

if (-not (Get-Module -ListAvailable -Name ImportExcel)) {
    try { Install-Module ImportExcel -Scope CurrentUser -Force -ErrorAction Stop }
    catch { throw "ImportExcel module not installed and automatic install failed. Install it with: Install-Module ImportExcel -Scope CurrentUser" }
}
Import-Module ImportExcel

#--- Helpers -------------------------------------------------------------------
function Sanitize-SheetName {
    param([string]$Name, [int]$Index)
    if ([string]::IsNullOrWhiteSpace($Name)) { return "User_$Index" }
    $safe = $Name -replace '[\\/*?:\[\]]','_'  # illegal characters
    if ($safe.Length -gt 31) { $safe = $safe.Substring(0,31) }
    if ([string]::IsNullOrWhiteSpace($safe)) { $safe = "User_$Index" }
    return $safe
}

# Cache for group lookups to reduce LDAP chatter
$GroupCache = @{}
function Get-GroupDetails {
    param([string]$Identity)
    if ($GroupCache.ContainsKey($Identity)) { return $GroupCache[$Identity] }
    try {
        $g = Get-ADGroup -Identity $Identity -Properties Description, GroupCategory, GroupScope, ManagedBy, mail, whenCreated, SID, DistinguishedName, SamAccountName, Name
        # Resolve ManagedBy (DN -> CN) when possible
        $managedByName = $null
        if ($g.ManagedBy) {
            try {
                $mb = Get-ADObject -Identity $g.ManagedBy -Properties displayName, name
                $managedByName = $mb.displayName
                if (-not $managedByName) { $managedByName = $mb.name }
            } catch { $managedByName = $g.ManagedBy }
        }
        $obj = [pscustomobject]@{
            Name              = $g.Name
            SamAccountName    = $g.SamAccountName
            GroupCategory     = $g.GroupCategory
            GroupScope        = $g.GroupScope
            Description       = $g.Description
            ManagedBy         = $managedByName
            mail              = $g.mail
            whenCreated       = $g.whenCreated
            SID               = $g.SID
            DistinguishedName = $g.DistinguishedName
        }
        $GroupCache[$Identity] = $obj
        return $obj
    } catch {
        $stub = [pscustomobject]@{
            Name              = $Identity
            SamAccountName    = $null
            GroupCategory     = $null
            GroupScope        = $null
            Description       = $null
            ManagedBy         = $null
            mail              = $null
            whenCreated       = $null
            SID               = $null
            DistinguishedName = $Identity
        }
        $GroupCache[$Identity] = $stub
        return $stub
    }
}

#--- Gather members of Domain Admins (recursive) --------------------------------
Write-Host "Collecting Domain Admins membership..." -ForegroundColor Cyan

$membersRaw = Get-ADGroupMember -Identity $DomainAdminsGroup -Recursive -ErrorAction Stop |
              Sort-Object DistinguishedName -Unique   # extra safety vs duplicates

# Build Domain Admins sheet rows (users & groups)  [FIXED: array subexpression]
$daRows = @(
    foreach ($m in $membersRaw) {
        $type = $m.objectClass
        if ($type -eq 'user') {
            $u = Get-ADUser -Identity $m.DistinguishedName -Properties DisplayName, mail, userPrincipalName, Enabled, whenCreated, LastLogonDate, Title, Department, SID
            [pscustomobject]@{
                Type              = 'User'
                Name              = $u.DisplayName
                SamAccountName    = $u.SamAccountName
                UserPrincipalName = $u.UserPrincipalName
                Enabled           = $u.Enabled
                Title             = $u.Title
                Department        = $u.Department
                Email             = $u.mail
                WhenCreated       = $u.whenCreated
                LastLogonDate     = $u.LastLogonDate
                DistinguishedName = $u.DistinguishedName
                SID               = $u.SID
            }
        }
        elseif ($type -eq 'group') {
            $g = Get-GroupDetails -Identity $m.DistinguishedName
            [pscustomobject]@{
                Type              = 'Group'
                Name              = $g.Name
                SamAccountName    = $g.SamAccountName
                UserPrincipalName = $null
                Enabled           = $null
                Title             = $null
                Department        = $null
                Email             = $g.mail
                WhenCreated       = $g.whenCreated
                LastLogonDate     = $null
                DistinguishedName = $g.DistinguishedName
                SID               = $g.SID
            }
        }
        else {
            [pscustomobject]@{
                Type              = $type
                Name              = $m.Name
                SamAccountName    = $m.SamAccountName
                UserPrincipalName = $null
                Enabled           = $null
                Title             = $null
                Department        = $null
                Email             = $null
                WhenCreated       = $null
                LastLogonDate     = $null
                DistinguishedName = $m.DistinguishedName
                SID               = $null
            }
        }
    }
) | Sort-Object Type, Name

# Users who are in Domain Admins (directly or via nesting)
$userMembers = $membersRaw | Where-Object { $_.objectClass -eq 'user' }

#--- Build Excel ----------------------------------------------------------------
if (Test-Path $OutputPath) { Remove-Item $OutputPath -Force }

# 1) Domain Admins overview sheet
$daRows | Export-Excel -Path $OutputPath -WorksheetName 'Domain Admins' -AutoSize -FreezeTopRow -BoldTopRow `
    -TableName 'DomainAdminsMembers' -ClearSheet

# Prepare Index rows
$indexRows = [System.Collections.Generic.List[object]]::new()

# 2) Per-user sheets with full (recursive) group memberships + group details
$i = 0
foreach ($u in $userMembers) {
    $i++
    $user = Get-ADUser -Identity $u.DistinguishedName -Properties DisplayName, mail, userPrincipalName, Enabled, whenCreated, LastLogonDate, Title, Department, SID

    Write-Host "Processing $($user.SamAccountName)..." -ForegroundColor Yellow

    $grpList = Get-ADPrincipalGroupMembership -Identity $user.DistinguishedName

    # [FIXED: array subexpression]
    $rows = @(
        foreach ($g in $grpList) {
            $gd = Get-GroupDetails -Identity $g.DistinguishedName
            [pscustomobject]@{
                GroupName         = $gd.Name
                GroupSAM          = $gd.SamAccountName
                GroupCategory     = $gd.GroupCategory
                GroupScope        = $gd.GroupScope
                Description       = $gd.Description
                ManagedBy         = $gd.ManagedBy
                Mail              = $gd.mail
                WhenCreated       = $gd.whenCreated
                DistinguishedName = $gd.DistinguishedName
                SID               = $gd.SID
            }
        }
    ) | Sort-Object GroupName

    if (-not $rows) {
        $rows = ,([pscustomobject]@{ GroupName = "<no groups>"; GroupSAM=$null; GroupCategory=$null; GroupScope=$null; Description=$null; ManagedBy=$null; Mail=$null; WhenCreated=$null; DistinguishedName=$null; SID=$null })
    }

    $sheetNameBase = if ($user.UserPrincipalName) { $user.UserPrincipalName.Split('@')[0] } else { $user.SamAccountName }
    $sheetName = Sanitize-SheetName -Name $sheetNameBase -Index $i

    # --- User info header
    $userInfo = [pscustomobject]@{
        DisplayName        = $user.DisplayName
        SamAccountName     = $user.SamAccountName
        UserPrincipalName  = $user.UserPrincipalName
        Enabled            = $user.Enabled
        Title              = $user.Title
        Department         = $user.Department
        Email              = $user.mail
        WhenCreated        = $user.whenCreated
        LastLogonDate      = $user.LastLogonDate
        DistinguishedName  = $user.DistinguishedName
        SID                = $user.SID
    }

    $startRowGroups = 6

    # Clear/create the sheet and write user info
    $userInfo | Export-Excel -Path $OutputPath -WorksheetName $sheetName -AutoSize -TableName ("UserInfo_{0}" -f $sheetName) -StartRow 1 -FreezeTopRow -BoldTopRow -ClearSheet

    # Add Back-to-Index link (Excel formula).  NOTE: removed -NoNumberConversion
    $back = [pscustomobject]@{ Navigation = '=HYPERLINK("#''Index''!A1","← Back to Index")' }
    $back | Export-Excel -Path $OutputPath -WorksheetName $sheetName -StartRow 1 -StartColumn 12 -TableName ("Nav_{0}" -f $sheetName)

    # Title and groups table
    $title = [pscustomobject]@{ Note = "Group Memberships (recursive)" }
    $title | Export-Excel -Path $OutputPath -WorksheetName $sheetName -StartRow ($startRowGroups - 2) -TableName ("Title_{0}" -f $sheetName) -AutoSize

    $rows | Export-Excel -Path $OutputPath -WorksheetName $sheetName -StartRow $startRowGroups -AutoSize -FreezeTopRow `
        -TableName ("Groups_{0}" -f $sheetName) -BoldTopRow

    # Index entry (add a hyperlink formula to jump to the sheet)
    $linkFormula = '=HYPERLINK("#''{0}''!A1","Open")' -f $sheetName
    $indexRows.Add([pscustomobject]@{
        User              = $user.DisplayName
        SamAccountName    = $user.SamAccountName
        UserPrincipalName = $user.UserPrincipalName
        Worksheet         = $sheetName
        Link              = $linkFormula
    })
}

# 3) Index sheet with hyperlinks (NOTE: removed -NoNumberConversion)
$indexRows | Sort-Object User | Export-Excel -Path $OutputPath -WorksheetName 'Index' -AutoSize -FreezeTopRow -BoldTopRow -TableName 'UserSheets' -ClearSheet

Write-Host "`nDone. Workbook saved to: $OutputPath" -ForegroundColor Green
