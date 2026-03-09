# Render + UptimeRobot settings (CLINX bot)

## 1) Render service type
- Create: `Web Service` (not Static Site)
- Runtime: `Python 3`
- Branch: `main`
- Root Directory: *(leave empty / repo root)*

## 2) Build and start commands
- Build Command:
`pip install -r requirements.txt`

- Start Command:
`python render_start.py`

## 3) Instance plan
- Plan: `Free`

## 4) Health check
- Health Check Path:
`/healthz`

## 5) Environment variables
- `BOT_TOKEN` = your Discord bot token
- `DEFAULT_BACKUP_GUILD_ID` = optional guild id for `/restore_missing`

## 6) UptimeRobot monitor
- Monitor Type: `HTTP(s)`
- URL: your render URL, example `https://clinx-bot.onrender.com/healthz`
- Interval: `5 minutes` (free plan)
- Keyword monitoring: optional (`{"ok": true}`)

## 7) Notes
- Keep one monitor only; too many pings can look abusive.
- On free Render, cold starts/restarts can still happen.
- If you need guaranteed always-on, move to a paid instance or free VM.
