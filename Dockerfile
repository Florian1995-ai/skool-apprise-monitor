FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

WORKDIR /app

# Install Python deps (playwright already installed in base image)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV STATE_DIR=/app/state

# State directory (mount as volume for persistence)
RUN mkdir -p /app/state

# Copy monitor script
COPY skool_apprise_monitor.py .

# Default: daemon mode — persistent loop, members only, every 3 minutes
CMD ["python", "skool_apprise_monitor.py", "--daemon", "--interval", "180", "--members-only"]
