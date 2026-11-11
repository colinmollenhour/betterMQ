$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$Fixture = Join-Path ([IO.Path]::GetTempPath()) ('bettermq-installer-test-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory $Fixture | Out-Null
$env:BETTERMQ_VERSION = '0.6.2'
$env:BETTERMQ_FORCE = '1'

function Invoke-WebRequest {
    param($Uri, $OutFile, [switch]$UseBasicParsing)
    if ($Uri.EndsWith('/checksums.txt')) {
        if ($script:Mode -eq 'missing') { throw 'injected checksum download failure' }
        $Name = if ($script:Mode -eq 'entry') { 'other.zip' } else { 'bettermq-windows-amd64.exe.zip' }
        Set-Content $OutFile (('0' * 64) + '  ' + $Name)
    } else {
        Set-Content $OutFile 'test archive bytes'
    }
}

try {
    foreach ($script:Mode in @('missing', 'entry', 'mismatch')) {
        $env:BETTERMQ_INSTALL_DIR = Join-Path $Fixture $script:Mode
        $Failed = $false
        try { & (Join-Path $Root 'install.ps1') } catch { $Failed = $true }
        if (-not $Failed) { throw "Installer accepted $script:Mode checksums" }
        if (Test-Path (Join-Path $env:BETTERMQ_INSTALL_DIR 'bin/bettermq.exe')) {
            throw 'Installed an unchecked binary'
        }
    }
} finally {
    Remove-Item -Recurse -Force $Fixture
}
