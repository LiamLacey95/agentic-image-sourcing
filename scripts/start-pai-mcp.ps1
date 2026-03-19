$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "Virtual environment not found at $python. Create it first with: python -m venv .venv"
}

Set-Location $repoRoot
& $python -m agentic_image_sourcing.main mcp
