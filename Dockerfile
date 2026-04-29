FROM python:3.11-slim

# Install Chrome + Xvfb (virtual display for non-headless CDP)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg2 \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    google-chrome-stable \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

ENV DISPLAY=:99

WORKDIR /app

COPY pyproject.toml ./
COPY src/ src/
COPY migrations/ migrations/

RUN pip install --no-cache-dir -e .

RUN mkdir -p /app/data

# Start Xvfb then run the scraper
# Configurable via Railway env vars:
#   START_OFFSET (default 0), END_OFFSET (default 100), WORKERS (default 2)
#   PROXY_URL (e.g. http://user:pass@host:port)
CMD ["sh", "-c", "Xvfb :99 -screen 0 1280x900x24 -nolisten tcp & sleep 1 && exec hltv-scraper --start-offset ${START_OFFSET:-0} --end-offset ${END_OFFSET:-100} --workers ${WORKERS:-2}"]
