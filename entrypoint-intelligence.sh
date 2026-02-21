#!/bin/bash
# Dump current environment vars to /app/.env so cron jobs can load them via python-dotenv
# (cron doesn't inherit parent process env vars)

printenv | grep -v "^BASH\|^SHLVL\|^_=" | while IFS= read -r line; do
    key="${line%%=*}"
    val="${line#*=}"
    printf '%s=%s\n' "$key" "$val"
done > /app/.env

echo "[$(date -u '+%Y-%m-%d %H:%M UTC')] Intelligence container started. Env written to /app/.env."
echo "[$(date -u '+%Y-%m-%d %H:%M UTC')] Cron schedule: 8am, 2pm, 8pm EST (13/19/01 UTC)"

# Start cron in foreground
exec cron -f
