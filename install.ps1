$ErrorActionPreference = 'Stop'
Write-Host 'Installing stock monitor dependencies...' -ForegroundColor Cyan
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install pykrx==1.2.4 --no-deps
Write-Host 'Install complete.' -ForegroundColor Green
