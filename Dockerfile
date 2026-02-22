FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

WORKDIR /app

# Install Python deps (playwright already installed in base image)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV STATE_DIR=/app/state
ENV EVENTS_DIR=/app/state/events

# State + events directories (mount as volume for persistence)
RUN mkdir -p /app/state/events

# Copy monitor + digest scripts
COPY skool_apprise_monitor.py .
COPY skool_daily_digest_v3.py .

# Daemon mode: members + cancellations + posts every 3 minutes
# Daily digest triggers at 9:30pm EST (02:30 UTC) automatically
CMD ["python", "skool_apprise_monitor.py", "--daemon", "--interval", "180"]
