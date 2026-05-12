# Stock Manager

Local stock dashboard for collection, storage, portfolio monitoring, watchlists, recommendations, and daily price analysis.

## Local run

```powershell
python -m pip install -r requirements.txt
python -m pip install pykrx==1.2.4 --no-deps
powershell -ExecutionPolicy Bypass -File .\run_stock_web.ps1
```

Open `http://127.0.0.1:8060`.

## EC2 run on port 81

```bash
curl -fsSL https://raw.githubusercontent.com/keonam/stock-manager/main/deploy/install_ec2.sh -o install_ec2.sh
chmod +x install_ec2.sh
APP_USER=ubuntu ./install_ec2.sh
```

The service runs with:

- `STOCK_WEB_HOST=0.0.0.0`
- `STOCK_WEB_PORT=81`
- systemd service name: `stock-manager`

Make sure the EC2 security group allows inbound TCP `81`.

