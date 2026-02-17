FROM python:3.11-slim

# Install system deps for Playwright Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg ca-certificates fonts-liberation libasound2 libatk-bridge2.0-0 \
    libatk1.0-0 libcups2 libdbus-1-3 libdrm2 libgbm1 libgtk-3-0 libnspr4 \
    libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libxshmfence1 xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright + Chromium
RUN playwright install chromium && playwright install-deps chromium

# Copy monitor script (copied from parent dir during build, or mounted)
COPY skool_apprise_monitor.py .

ENV STATE_DIR=/app/state

# State directory (mount as volume for persistence)
RUN mkdir -p /app/state

# Default: daemon mode — persistent loop, members only, every 3 minutes
CMD ["python", "skool_apprise_monitor.py", "--daemon", "--interval", "180", "--members-only"]
