# Oracle Deploy

## 1. SSH In

```bash
ssh ubuntu@YOUR_ORACLE_IP
```

## 2. Install Base Packages

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip curl
```

## 3. Install Node + PM2

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
sudo npm install -g pm2
```

## 4. Clone Repo

```bash
cd $HOME
git clone YOUR_REPO_URL rudis-bot
cd rudis-bot
```

## 5. Create Python Venv

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 6. Copy Env

Create `.env` in the repo root and make sure at minimum these are set:

```env
DISCORD_BOT_TOKEN=
DISCORD_LOG_CHANNEL_ID=
DISCORD_GUILD_ID=
ALLOWED_CHANNEL_IDS=
ALLOWED_USER_IDS=
CRYPTO_SCAN_MODE=graduation
CRYPTO_CONFIRMED_WEBHOOK=
RUDIS_API_BASE_URL=https://rudis-ai-production.up.railway.app
HELIUS_API_KEY=
HELIUS_RPC_URL=
COINGECKO_API_KEY=
COINGECKO_API_KEY_TYPE=pro
JUPITER_API_KEY=
GRADUATED_MINTS_FILE=/home/ubuntu/rudis-bot/logs/graduated_mints.txt
HELIUS_WEBHOOK_PORT=8080
HELIUS_WEBHOOK_PATH=/helius-webhook
PUMPFUN_PROGRAM_ID=6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
STOCKS_WEBHOOK=
ALPACA_KEY=
ALPACA_SECRET=
RTH_FLOAT_CANDIDATES_PATH=/home/ubuntu/rudis-bot/float_candidates.csv
```

Important:

- Set `CRYPTO_CONFIRMED_WEBHOOK` to your Discord webhook for `crypto-confirmed`
- Do not set `RUDIS_EXECUTION_CONTEXT_URL` yet unless that endpoint is actually live
- Place `float_candidates.csv` in the repo root if you want the stock scanner to use the merged Finviz low-float candidate list

## 7. Start All Processes In PM2

From the repo root:

```bash
pm2 start ./run_rudis.sh --name rudis-execution-bot --interpreter /bin/zsh
pm2 start ./run_helius_listener.sh --name rudis-helius-listener --interpreter /bin/zsh
pm2 start ./run_graduation_scanner.sh --name rudis-crypto-scanner --interpreter /bin/zsh
pm2 start ./run_stock_scanner.sh --name rudis-stock-scanner --interpreter /bin/zsh
pm2 save
sudo env "PATH=$PATH" pm2 startup systemd -u $USER --hp $HOME
```

## 8. Useful PM2 Commands

```bash
pm2 status
pm2 logs rudis-execution-bot
pm2 logs rudis-helius-listener
pm2 logs rudis-crypto-scanner
pm2 logs rudis-stock-scanner
pm2 restart all
pm2 stop all
```

## 9. Open Port 8080

If Oracle networking is locked down, allow TCP `8080` in:

- Oracle Cloud Security List / Network Security Group
- Host firewall if enabled

## 10. Create Helius Webhook

In Helius dashboard:

- Webhook URL: `http://YOUR_ORACLE_IP:8080/helius-webhook`
- Watch address: `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`
- Transaction type: `all`

## What Runs

- `rudis-execution-bot`: Discord slash commands like `/prepare` and `/status`
- `rudis-helius-listener`: receives pump.fun graduation-style webhook events
- `rudis-crypto-scanner`: enriches/scorers queued graduations and posts to `crypto-confirmed`
- `rudis-stock-scanner`: runs the stock lane
