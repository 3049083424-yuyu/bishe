[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string[]]$Paths,

    [string]$BackupRoot = "D:\graduate\_csv_backups"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $BackupRoot)) {
    New-Item -ItemType Directory -Path $BackupRoot | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$sessionDir = Join-Path $BackupRoot $timestamp
New-Item -ItemType Directory -Path $sessionDir | Out-Null

foreach ($rawPath in $Paths) {
    $resolved = Resolve-Path -Path $rawPath
    foreach ($item in $resolved) {
        $source = $item.Path
        if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
            continue
        }

        $leaf = Split-Path -Path $source -Leaf
        $base = [System.IO.Path]::GetFileNameWithoutExtension($leaf)
        $ext = [System.IO.Path]::GetExtension($leaf)
        $target = Join-Path $sessionDir ("{0}__{1}{2}" -f $base, $timestamp, $ext)

        Copy-Item -LiteralPath $source -Destination $target -Force

        [pscustomobject]@{
            Source      = $source
            Backup      = $target
            BackupBatch = $timestamp
        }
    }
}
