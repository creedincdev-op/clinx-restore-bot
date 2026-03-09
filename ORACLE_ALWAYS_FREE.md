# Oracle Always Free 24/7 Setup (CLINX Bot)

This is the recommended free 24/7 path for your Discord bot.

## 1) Create VM
- Cloud: Oracle Cloud Free Tier
- Image: Ubuntu 22.04 LTS
- Shape: Ampere A1 (Always Free)
- Boot volume: default
- SSH key: your public key

## 2) Open SSH and connect
```bash
ssh ubuntu@<YOUR_VM_PUBLIC_IP>
```

## 3) Bootstrap app
```bash
curl -fsSL https://raw.githubusercontent.com/creedincdev-op/clinx-restore-bot/main/oracle/bootstrap_oracle.sh -o bootstrap_oracle.sh
chmod +x bootstrap_oracle.sh
./bootstrap_oracle.sh
```

## 4) Add bot env vars
```bash
cd /home/ubuntu/clinx-restore-bot
cat > .env <<'EOF'
BOT_TOKEN=YOUR_NEW_BOT_TOKEN
DEFAULT_BACKUP_GUILD_ID=OPTIONAL_GUILD_ID
EOF
chmod 600 .env
```

## 5) Install service
```bash
sudo cp /home/ubuntu/clinx-restore-bot/oracle/clinx-bot.service /etc/systemd/system/clinx-bot.service
sudo systemctl daemon-reload
sudo systemctl enable --now clinx-bot
```

## 6) Verify
```bash
sudo systemctl status clinx-bot --no-pager
journalctl -u clinx-bot -n 100 --no-pager
```

## 7) Update flow
```bash
cd /home/ubuntu/clinx-restore-bot
git pull origin main
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart clinx-bot
```

## Notes
- Run the bot in only one place (this VM only).
- If you reset token, update `.env` and restart service.
- Do not use Render/UptimeRobot once this VM is live with the same token.
