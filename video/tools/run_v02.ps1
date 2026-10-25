# Rebuild video 02 end to end, log to video/out/run_v02.log.
# (ASCII only: Windows PowerShell 5.1 reads .ps1 files without BOM as ANSI.)
#   powershell -ExecutionPolicy Bypass -File .\video\tools\run_v02.ps1               # all steps
#   powershell -ExecutionPolicy Bypass -File .\video\tools\run_v02.ps1 -From render  # resume from a step
# Steps: seed (pick seeds, costs ElevenLabs characters), tts (assemble audio, uses cache),
#        check (Whisper term check), render (MP4), frames (extract 6 PNG frames).
param([ValidateSet("seed", "tts", "check", "render", "frames")][string]$From = "seed")

# No ErrorActionPreference = Stop: PowerShell 5.1 turns any stderr line of a native
# program (for example "npm notice") into an error. Only exit codes are checked.
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:PYTHONIOENCODING = "utf-8"
$log = "out\run_v02.log"
New-Item -ItemType Directory -Force out, out\frames | Out-Null
$order = "seed", "tts", "check", "render", "frames"
$start = [array]::IndexOf($order, $From)

function Write-Log([string]$line) {
    Write-Host $line
    Add-Content -Path $log -Value $line -Encoding UTF8
}

function Step([string]$key, [string]$name, [scriptblock]$body) {
    if ([array]::IndexOf($order, $key) -lt $start) { return }
    Write-Log "--- $name"
    & $body 2>&1 | ForEach-Object { "$_" } | Where-Object { $_ -notmatch "Warning|npm notice" } | ForEach-Object { Write-Log $_ }
    if ($LASTEXITCODE -ne 0) { Write-Log "ERROR in step: $name (exit $LASTEXITCODE)"; exit 1 }
}

Set-Content -Path $log -Value "=== $(Get-Date -Format s) start from step $From" -Encoding UTF8
Step "seed" "pick seeds s3 s5 s7" { .\.venv\Scripts\python tools\pick_seed.py v02 s3 s5 s7 --seeds 1 2 3 4 }
Step "tts" "assemble audio" { .\.venv\Scripts\python tools\tts.py v02 }
Step "check" "term check" { .\.venv\Scripts\python tools\check_terms.py v02 }
Step "render" "render" { npx remotion render Video02Hdfs out/02_hdfs.mp4 --codec=h264 --log=error }
Step "frames" "extract 6 frames" {
    $ff = "node_modules\@remotion\compositor-win32-x64-msvc\ffmpeg.exe"
    $i = 0
    foreach ($t in "00:00:20", "00:01:10", "00:02:05", "00:03:00", "00:03:45", "00:04:40") {
        $i++
        & $ff -v error -y -ss $t -i out\02_hdfs.mp4 -frames:v 1 ("out\frames\khung_{0}_{1}.png" -f $i, $t.Replace(":", "-"))
    }
}
Write-Log "=== $(Get-Date -Format s) done"
