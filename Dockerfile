FROM python:3.11-slim

# Minimal base deps for Playwright to call install-deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright + Chromium (install-deps handles all system packages)
RUN playwright install chromium --with-deps

ENV STATE_DIR=/app/state

# State directory (mount as volume for persistence)
RUN mkdir -p /app/state

# Copy monitor script
COPY skool_apprise_monitor.py .

# Default: daemon mode — persistent loop, members only, every 3 minutes
CMD ["python", "skool_apprise_monitor.py", "--daemon", "--interval", "180", "--members-only"]
