[CmdletBinding()]
param(
    [int]$Messages = 10000,
    [int]$Warmup = 500,
    [int]$Runs = 3,
    [int]$PayloadBytes = 256,
    [int]$Producers = 1,
    [int]$Consumers = 1,
    [int]$Prefetch = 200,
    [int]$KueuePort = 18080,
    [int]$RabbitPort = 5673,
    [string]$Output = "benchmark-results/latest.json"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$tmpDir = Join-Path $repoRoot "tmp"
$containerName = "kueue-benchmark-rabbitmq"
$dbPath = Join-Path $tmpDir ("benchmark-badger-" + $KueuePort)
$stdoutLogPath = Join-Path $tmpDir ("benchmark-kueue-" + $KueuePort + ".stdout.log")
$stderrLogPath = Join-Path $tmpDir ("benchmark-kueue-" + $KueuePort + ".stderr.log")

if ([System.IO.Path]::IsPathRooted($Output)) {
    $outputPath = $Output
} else {
    $outputPath = Join-Path $repoRoot $Output
}

function Get-CanonicalPath {
    param([string]$PathToNormalize)

    $fullPath = [System.IO.Path]::GetFullPath($PathToNormalize)
    if ($fullPath.Length -gt 3) {
        $fullPath = $fullPath.TrimEnd(
            [System.IO.Path]::DirectorySeparatorChar,
            [System.IO.Path]::AltDirectorySeparatorChar
        )
    }

    return $fullPath
}

$repoRoot = Get-CanonicalPath $repoRoot

function Assert-WithinRepo {
    param([string]$PathToCheck)

    $repoFull = $repoRoot
    $targetFull = Get-CanonicalPath $PathToCheck
    $repoPrefix = $repoFull + [System.IO.Path]::DirectorySeparatorChar

    if (
        $targetFull -ne $repoFull -and
        -not $targetFull.StartsWith($repoPrefix, [System.StringComparison]::OrdinalIgnoreCase)
    ) {
        throw "Refusing to operate outside the repo: $targetFull"
    }

    return $targetFull
}

function Reset-RepoDir {
    param([string]$PathToReset)

    $resolvedPath = Assert-WithinRepo -PathToCheck $PathToReset
    if (Test-Path -LiteralPath $resolvedPath) {
        Remove-Item -LiteralPath $resolvedPath -Recurse -Force
    }
}

function Stop-ListenerOnPort {
    param([int]$Port)

    $listeners = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    foreach ($listener in $listeners) {
        Stop-Process -Id $listener.OwningProcess -Force -ErrorAction SilentlyContinue
    }
}

function Wait-HttpReady {
    param([string]$Url)

    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        try {
            Invoke-WebRequest -Uri $Url -Method Post -TimeoutSec 3 | Out-Null
            return
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }

    throw "kueue server did not become ready at $Url"
}

function Wait-RabbitReady {
    param([string]$Name)

    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        $logs = docker logs $Name 2>&1
        if ($LASTEXITCODE -eq 0 -and (($logs | Out-String) -match "Server startup complete")) {
            return
        }

        Start-Sleep -Seconds 1
    }

    throw "RabbitMQ container did not become ready"
}

$tmpDir = Assert-WithinRepo -PathToCheck $tmpDir
$dbPath = Assert-WithinRepo -PathToCheck $dbPath
$stdoutLogPath = Assert-WithinRepo -PathToCheck $stdoutLogPath
$stderrLogPath = Assert-WithinRepo -PathToCheck $stderrLogPath
$outputPath = Assert-WithinRepo -PathToCheck $outputPath
if ($outputPath -eq $repoRoot) {
    $outputDir = $repoRoot
} else {
    $outputDir = Assert-WithinRepo -PathToCheck ([System.IO.Path]::GetDirectoryName($outputPath))
}

New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

Stop-ListenerOnPort -Port $KueuePort
Start-Sleep -Milliseconds 500
Reset-RepoDir -PathToReset $dbPath
foreach ($logPath in @($stdoutLogPath, $stderrLogPath)) {
    if (Test-Path -LiteralPath $logPath) {
        Remove-Item -LiteralPath $logPath -Force
    }
}

docker rm -f $containerName *> $null
docker run -d --rm --name $containerName -p "${RabbitPort}:5672" rabbitmq:4-management | Out-Null
Wait-RabbitReady -Name $containerName

$server = $null
try {
    $server = Start-Process `
        -FilePath "go" `
        -ArgumentList @("run", ".") `
        -WorkingDirectory $repoRoot `
        -PassThru `
        -NoNewWindow `
        -RedirectStandardOutput $stdoutLogPath `
        -RedirectStandardError $stderrLogPath `
        -Environment @{
            PORT = "$KueuePort"
            KUEUE_DB_PATH = $dbPath
        }

    Wait-HttpReady -Url "http://127.0.0.1:$KueuePort/"

    go run ./cmd/bench `
        --targets kueue,rabbitmq `
        --kueue-url "http://127.0.0.1:$KueuePort" `
        --rabbitmq-uri "amqp://guest:guest@127.0.0.1:$RabbitPort/" `
        --messages $Messages `
        --warmup $Warmup `
        --runs $Runs `
        --payload-bytes $PayloadBytes `
        --producers $Producers `
        --consumers $Consumers `
        --prefetch $Prefetch `
        --json-out $outputPath

    Write-Host "benchmark report written to $outputPath"
} finally {
    Stop-ListenerOnPort -Port $KueuePort
    if ($server -and -not $server.HasExited) {
        Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
    }
    docker rm -f $containerName *> $null
}
