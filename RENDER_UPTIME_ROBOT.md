# Render settings (CLINX bot)

## Render fields
- Service type: `Web Service`
- Runtime: `Python 3`
- Branch: `main`
- Root directory: leave empty
- Build command: `pip install -r requirements.txt`
- Start command: `python render_start.py`
- Health check path: `/healthz`
- Plan: `Free`

## Environment variables
- `BOT_TOKEN` = your Discord bot token
- `DEFAULT_BACKUP_GUILD_ID` = optional guild id for `/restore_missing`
- `BOT_LOGIN_429_COOLDOWN` = `1800`
- `BOT_LOGIN_429_COOLDOWN_MAX` = `7200`
- `BOT_RESTART_BACKOFF_INITIAL` = `900`
- `BOT_RESTART_BACKOFF_MAX` = `7200`
- `BOT_RAPID_EXIT_SECONDS` = `180`
- `BOT_STARTUP_JITTER_MAX` = `45`

## Repo defaults
- `.python-version` pins Render to Python `3.13`
- `render.yaml` includes the same deploy settings for Blueprint deploys
- `render_start.py` keeps the health endpoint alive while the bot cools down after a Discord 429

## Important note
- These changes reduce retry pressure and stop restart loops
- They cannot guarantee `0` login 429s if Discord is already blocking the Render outbound IP
