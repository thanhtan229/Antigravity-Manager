param(
    [Parameter(Mandatory=$true)][string]$ArtifactDirectory,
    [Parameter(Mandatory=$true)][ValidatePattern('^[a-f0-9]{40}$')][string]$ExpectedSourceCommit
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$artifactRoot = (Resolve-Path -LiteralPath $ArtifactDirectory).Path
$profilePath = Join-Path $projectRoot '.runtime/profile'
$runtimeRoot = Join-Path $projectRoot '.runtime'
$pidPath = Join-Path $runtimeRoot 'native-image-test.pid'
$source = (Get-Content -LiteralPath (Join-Path $artifactRoot 'source-commit.txt') -Raw).Trim()
if ($source -ne $ExpectedSourceCommit) { throw 'Artifact source SHA does not match the approved workflow commit' }
$expectedFiles = @('antigravity-tools.exe', 'WebView2Loader.dll')
$verifiedFiles = @()
foreach ($entry in Get-Content -LiteralPath (Join-Path $artifactRoot 'sha256.txt')) {
    if ($entry -notmatch '^([a-f0-9]{64})  (antigravity-tools\.exe|WebView2Loader\.dll)$') { throw 'Unexpected checksum manifest entry' }
    $expectedHash = $Matches[1]
    $fileName = $Matches[2]
    $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $artifactRoot $fileName)).Hash.ToLowerInvariant()
    if ($expectedHash -ne $actualHash) { throw "Artifact checksum mismatch: $fileName" }
    $verifiedFiles += $fileName
}
if (@(Compare-Object ($expectedFiles | Sort-Object) ($verifiedFiles | Sort-Object)).Count -gt 0) { throw 'Incomplete checksum manifest' }
$testConfig = Get-Content -LiteralPath (Join-Path $profilePath 'gui_config.json') -Raw | ConvertFrom-Json
if ($testConfig.proxy.port -ne 8045 -or $testConfig.proxy.allow_lan_access) { throw 'Isolated profile must select loopback port 8045' }
if ($testConfig.scheduled_warmup.enabled) { throw 'Disable warmup in the isolated profile before launch' }
Remove-Variable testConfig

if (-not (Test-Path -LiteralPath (Join-Path $profilePath 'native_image_attempt_budget.sqlite3'))) { throw 'Attempt budget database is missing; refusing to start a fresh counter' }
$budgetState = python -c "import sqlite3,sys,json; c=sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True); print(json.dumps(c.execute('select used,limit_count from native_image_attempt_budget where singleton=1').fetchone()))" (Join-Path $profilePath 'native_image_attempt_budget.sqlite3') | ConvertFrom-Json
if ($budgetState.Count -ne 2 -or $budgetState[0] -ne 1 -or $budgetState[1] -ne 5) { throw 'Attempt budget must remain at exactly 1/5 for this deployment' }

$binaryPath = Join-Path $artifactRoot 'antigravity-tools.exe'
$previousTestRoot = Join-Path $runtimeRoot 'native-image-safe-b32c2774'
$recordedPid = if (Test-Path -LiteralPath $pidPath) { [int](Get-Content -LiteralPath $pidPath -Raw) } else { 0 }
foreach ($listener in @(Get-NetTCPConnection -LocalPort 8045 -State Listen -ErrorAction SilentlyContinue)) {
    $owner = Get-CimInstance Win32_Process -Filter "ProcessId=$($listener.OwningProcess)"
    if (-not $owner.ExecutablePath -or $owner.ProcessId -ne $recordedPid -or
        (-not $owner.ExecutablePath.StartsWith($previousTestRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -and
         $owner.ExecutablePath -ne $binaryPath)) { throw 'Port 8045 is owned by an unrelated or unidentified process' }
    Stop-Process -Id $owner.ProcessId -Force
    Wait-Process -Id $owner.ProcessId -Timeout 15 -ErrorAction SilentlyContinue
}

$launchEnvironment = @{
    ABV_DATA_DIR = $profilePath
    ABV_BIND_LOCAL_ONLY = '1'
    RUST_LOG = 'info'
    ABV_API_KEY = $null
    API_KEY = $null
    ABV_WEB_PASSWORD = $null
    WEB_PASSWORD = $null
    ABV_AUTH_MODE = 'all_except_health'
    AUTH_MODE = $null
}
$savedEnvironment = @{}
try {
    foreach ($name in $launchEnvironment.Keys) {
        $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $launchEnvironment[$name], 'Process')
    }
    $startedProcess = Start-Process -FilePath $binaryPath -ArgumentList '--headless' -WorkingDirectory $artifactRoot -WindowStyle Hidden -PassThru
} finally {
    foreach ($name in $savedEnvironment.Keys) { [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process') }
}
$healthy = $false
for ($check = 0; $check -lt 30; $check++) {
    Start-Sleep -Seconds 1
    if ($startedProcess.HasExited) { throw 'Dedicated test process exited during startup' }
    try {
        $health = Invoke-RestMethod -Uri 'http://127.0.0.1:8045/health' -TimeoutSec 2
        if ($health.status -eq 'ok') { $healthy = $true; break }
    } catch { }
}
if (-not $healthy) { throw 'Dedicated test health did not become ready' }
$listeners = @(Get-NetTCPConnection -LocalPort 8045 -State Listen)
if ($listeners.Count -ne 1 -or $listeners[0].LocalAddress -ne '127.0.0.1' -or $listeners[0].OwningProcess -ne $startedProcess.Id) { throw 'Unexpected listener binding or owner' }
$running = Get-CimInstance Win32_Process -Filter "ProcessId=$($startedProcess.Id)"
if ($running.ExecutablePath -ne $binaryPath) { throw 'Running process path does not match the checked artifact' }
$budgetAfter = python -c "import sqlite3,sys,json; c=sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True); print(json.dumps(c.execute('select used,limit_count from native_image_attempt_budget where singleton=1').fetchone()))" (Join-Path $profilePath 'native_image_attempt_budget.sqlite3') | ConvertFrom-Json
if ($budgetAfter.Count -ne 2 -or $budgetAfter[0] -ne 1 -or $budgetAfter[1] -ne 5) { throw 'Attempt budget changed during startup' }
Set-Content -LiteralPath $pidPath -Value $startedProcess.Id
[pscustomobject]@{pid=$startedProcess.Id; listener='127.0.0.1:8045'; health=$health.status; version=$health.version; source_commit=$source; binary=$binaryPath}
