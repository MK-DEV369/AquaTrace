#!/usr/bin/env pwsh
# Ship Analytics Quickstart — build, run, and verify the full stack
$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ProjectRoot

Write-Host "=== Ship Analytics Quickstart ===" -ForegroundColor Cyan

# 1. Build API image with all Phase 2 dependencies
Write-Host "`n[1/4] Building API image..." -ForegroundColor Yellow
docker compose build api
if ($LASTEXITCODE -ne 0) { throw "Build failed" }

# 2. Start all services
Write-Host "`n[2/4] Starting services..." -ForegroundColor Yellow
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "Compose up failed" }

# 3. Wait for API to be ready
Write-Host "`n[3/4] Waiting for API..." -ForegroundColor Yellow
$retries = 30
do {
    Start-Sleep -Seconds 2
    $retries--
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction SilentlyContinue
    } catch {
        $resp = $null
    }
} while ($resp -and $retries -gt 0)

if (-not $resp) { throw "API did not become ready" }
Write-Host "  API is ready" -ForegroundColor Green

# 4. Run tests
Write-Host "`n[4/4] Running tests..." -ForegroundColor Yellow
python3 -m pytest tests/ -v
if ($LASTEXITCODE -ne 0) { throw "Tests failed" }

Write-Host "`n=== Done ===" -ForegroundColor Green
Write-Host "API:        http://localhost:8000"
Write-Host "Docs:       http://localhost:8000/docs"
Write-Host "Dashboard:  http://localhost:8501"
Write-Host "PostgreSQL: localhost:5432"
Write-Host "Kafka:      localhost:29092"