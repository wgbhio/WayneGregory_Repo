# ============================
# Bulk: Copy missing installers → Install CS (all) + SCOM (PR hosts only)
#       + Force Windows Power Plan to **High performance** via WinRM
# PS 5.1 compatible
# ============================

$Servers = @(
   'UKPRDB214'
      ) | Select-Object -Unique

# --- Local source paths on YOUR admin machine ---
$Local_CS   = "C:\Beazley\Software\Crowdstrike\FalconSensor_Windows.exe"
$Local_SCOM = "C:\Beazley\Software\SCOM\MOMAgent.msi"

# --- Remote destinations on target servers ---
$Remote_CS_Folder   = "C:\Beazley\Software\Crowdstrike"
$Remote_CS_Exe      = Join-Path $Remote_CS_Folder "FalconSensor_Windows.exe"
$Remote_SCOM_Folder = "C:\Beazley\Software\SCOM"
$Remote_SCOM_MSI    = Join-Path $Remote_SCOM_Folder "MOMAgent.msi"

# --- Install parameters ---
$CS_CID        = "ADB1C14C8F2B4BF6BAAE8ACC90511E6C-71"
$SCOM_MG       = "SCOMPROD"
$SCOM_MGMT_SVR = "UKPRAP183.bfl.local"

# --- Power Plan target (High performance) ---
$HighPerfGuid = '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'

# --- Prompt for creds once ---
$cred = Get-Credential -Message "Enter domain creds (e.g. BFL\a1_wg7) for remote installs"

# Validate local sources
if (-not (Test-Path $Local_CS))   { throw "Local CrowdStrike not found at ${Local_CS}" }
if (-not (Test-Path $Local_SCOM)) { Write-Warning "Local SCOM MSI not found at ${Local_SCOM}. SCOM will be skipped where MSI is missing remotely." }

$results = @()

foreach ($srv in $Servers) {
  Write-Host "`n==== ${srv} ====" -ForegroundColor Cyan

  # Decide SCOM policy + reason text
  $doSCOM     = $false
  $scomReason = 'Not requested'
  if ($srv -match 'PR') {
    $doSCOM = $true
    $scomReason = ''     # actual result will be filled later
  } elseif ($srv -match 'DV') {
    $scomReason = 'Skipped (DV server)'
  }

  # Connectivity check
  try { Test-WSMan -ComputerName $srv -ErrorAction Stop | Out-Null }
  catch {
    Write-Warning "WSMan unreachable on ${srv}. Skipping."
    $ppBefore = '' ; $ppAfter = '' ; $ppChanged = $false ; $ppStatus = 'Skipped (offline)'
    $csMsg = 'Skipped (offline)'
    $scomMsg = if ($doSCOM) { 'Skipped (offline)' } else { $scomReason }
    $results += [pscustomobject]@{ Server=$srv; Copied='No'; CS=$csMsg; SCOM=$scomMsg; PP_Before=$ppBefore; PP_After=$ppAfter; PP_Changed=$ppChanged; PP_Status=$ppStatus; Notes='WSMan unreachable' }
    continue
  }

  $copied = $false
  $csResult = ''
  $scomResult = $scomReason
  $ppBefore = ''
  $ppAfter  = ''
  $ppChanged = $false
  $ppStatus  = ''

  $sess = $null
  try {
    $sess = New-PSSession -ComputerName $srv -Credential $cred

    # Ensure remote folders exist (idempotent)
    Invoke-Command -Session $sess -ScriptBlock {
      param($csFolder,$scomFolder)
      if (-not (Test-Path $csFolder))   { New-Item -Path $csFolder   -ItemType Directory -Force | Out-Null }
      if (-not (Test-Path $scomFolder)) { New-Item -Path $scomFolder -ItemType Directory -Force | Out-Null }
    } -ArgumentList $Remote_CS_Folder,$Remote_SCOM_Folder

    # Copy CS if missing
    $needCopyCS = Invoke-Command -Session $sess -ScriptBlock { param($p) -not (Test-Path $p) } -ArgumentList $Remote_CS_Exe
    if ($needCopyCS) {
      Copy-Item -ToSession $sess -Path $Local_CS -Destination $Remote_CS_Exe -Force
      Write-Host "Copied CS installer to ${srv}"
      $copied = $true
    } else {
      Write-Host "CS installer already present on ${srv}"
    }

    # Copy SCOM if needed + allowed (PR only)
    if ($doSCOM) {
      $needCopySCOM = Invoke-Command -Session $sess -ScriptBlock { param($p) -not (Test-Path $p) } -ArgumentList $Remote_SCOM_MSI
      if ($needCopySCOM) {
        if (Test-Path $Local_SCOM) {
          Copy-Item -ToSession $sess -Path $Local_SCOM -Destination $Remote_SCOM_MSI -Force
          Write-Host "Copied SCOM MSI to ${srv}"
          $copied = $true
        } else {
          Write-Warning "Local SCOM MSI missing; SCOM will be skipped on ${srv}"
        }
      } else {
        Write-Host "SCOM MSI already present on ${srv}"
      }
    }

    # ---- Install CrowdStrike (all servers) ----
    $cs = Invoke-Command -Session $sess -ScriptBlock {
      param($exe,$cid)
      $svc = Get-Service -Name CSFalconService -ErrorAction SilentlyContinue
      if ($svc) { return "Already installed ($($svc.Status))" }
      if (-not (Test-Path $exe)) { return "Skipped (installer missing)" }
      $args = "/install /quiet /norestart CID=$cid"
      $p = Start-Process -FilePath $exe -ArgumentList $args -Wait -PassThru
      if ($p.ExitCode -ne 0) { return "Install failed (code $($p.ExitCode))" }
      Start-Sleep -Seconds 3
      $svc2 = Get-Service -Name CSFalconService -ErrorAction SilentlyContinue
      if ($svc2) {
        if ($svc2.Status -ne 'Running') { Start-Service CSFalconService -ErrorAction SilentlyContinue }
        $svc2.Refresh()
        return "Installed ($($svc2.Status))"
      }
      return "Installed (service not detected yet)"
    } -ArgumentList $Remote_CS_Exe,$CS_CID
    $csResult = $cs

    # ---- Install SCOM (PR servers only) ----
    if ($doSCOM) {
      $scom = Invoke-Command -Session $sess -ScriptBlock {
        param($msi,$mg,$ms)
        $hs = Get-Service -Name HealthService -ErrorAction SilentlyContinue
        if ($hs) { return "Already installed ($($hs.Status))" }
        if (-not (Test-Path $msi)) { return "Skipped (MSI missing)" }
        $props = @(
          "AcceptEndUserLicenseAgreement=1",
          "MANAGEMENT_GROUP=$mg",
          "MANAGEMENT_SERVER_DNS=$ms",
          "USE_SETTINGS_FROM_AD=0",
          "ACTIONS_USE_COMPUTER_ACCOUNT=1"
        )
        $args = @("/i","`"$msi`"","/qn","/norestart") + $props
        $proc = Start-Process -FilePath "msiexec.exe" -ArgumentList ($args -join ' ') -Wait -PassThru
        if ($proc.ExitCode -ne 0) { return "Install failed (code $($proc.ExitCode))" }
        Start-Service -Name HealthService -ErrorAction SilentlyContinue
        (Get-Service HealthService).WaitForStatus('Running','00:00:20')
        $hs2 = Get-Service HealthService -ErrorAction SilentlyContinue
        if ($hs2) { return "Installed ($($hs2.Status))" }
        return "Installed (service not detected yet)"
      } -ArgumentList $Remote_SCOM_MSI,$SCOM_MG,$SCOM_MGMT_SVR
      $scomResult = $scom
    } else {
      # Keep the DV/Not requested reason
      $scomResult = $scomReason
    }

    # ---- Set Power Plan to High performance ----
    $pp = Invoke-Command -Session $sess -ScriptBlock {
      param($targetGuid)
      $ErrorActionPreference = 'Stop'

      function Get-ActiveScheme {
        $raw = (powercfg -getactivescheme) 2>&1
        $guid = ($raw -replace '.*GUID\s+([a-f0-9-]{36}).*','$1').ToLower()
        if ([string]::IsNullOrWhiteSpace($guid)) { $guid = 'unknown' }
        return @{ Raw=$raw; Guid=$guid }
      }
      function Get-SchemeName([string]$Guid){
        $schemes = (powercfg -list) 2>&1
        $name = 'Unknown'
        foreach ($line in ($schemes -split "`n")){
          if ($line -match [regex]::Escape($Guid)){
            $m = [regex]::Match($line,'GUID:[^\(]*\(([^\)]*)\)')
            if ($m.Success){ $name = $m.Groups[1].Value.Trim() }
            break
          }
        }
        return $name
      }

      $before = Get-ActiveScheme
      $beforeName = Get-SchemeName $before.Guid
      $err = $null
      try { powercfg -setactive $targetGuid 2>&1 | Out-Null } catch { $err = $_.Exception.Message }
      $after = Get-ActiveScheme
      $afterName = Get-SchemeName $after.Guid
      $changed = [string]::Compare($before.Guid, $after.Guid, $true) -ne 0

      [pscustomobject]@{
        BeforeGuid = $before.Guid
        BeforeName = $beforeName
        AfterGuid  = $after.Guid
        AfterName  = $afterName
        Changed    = $changed
        Error      = $err
      }
    } -ArgumentList $HighPerfGuid

    if ($pp) {
      $ppBefore  = $pp.BeforeName
      $ppAfter   = $pp.AfterName
      $ppChanged = [bool]$pp.Changed
      $ppStatus  = if ($pp.Error) { "Completed with error: $($pp.Error)" } else { 'OK' }
      Write-Host ("Power plan: {0} -> {1} | Changed={2} | {3}" -f $ppBefore,$ppAfter,$ppChanged,$ppStatus)
    } else {
      $ppStatus = 'No data'
    }

    # Build result row
    $copiedText = 'No changes'
    if ($copied) { $copiedText = 'Yes' }

    $results += [pscustomobject]@{
      Server=$srv
      Copied=$copiedText
      CS=$csResult
      SCOM=$scomResult
      PP_Before=$ppBefore
      PP_After=$ppAfter
      PP_Changed=$ppChanged
      PP_Status=$ppStatus
      Notes=''
    }

    Write-Host ("CS -> {0} | SCOM -> {1}" -f $csResult,$scomResult)
  }
  catch {
    $fallbackSCOM = $scomReason
    if ($doSCOM) { $fallbackSCOM = 'Error' }

    $ppBefore = '' ; $ppAfter = '' ; $ppChanged = $false ; $ppStatus = 'Error'

    $results += [pscustomobject]@{
      Server=$srv; Copied='N/A'; CS='Error'; SCOM=$fallbackSCOM; PP_Before=$ppBefore; PP_After=$ppAfter; PP_Changed=$ppChanged; PP_Status=$ppStatus; Notes=$_.Exception.Message
    }
    Write-Warning "Failure on ${srv}: $($_.Exception.Message)"
  }
  finally {
    if ($sess) { Remove-PSSession $sess }
  }
}

# ---- Summary ----
$results | Sort-Object Server | Format-Table -AutoSize Server, Copied, CS, SCOM, PP_Before, PP_After, PP_Changed, PP_Status, Notes
