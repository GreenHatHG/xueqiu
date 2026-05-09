# Minimal image: do NOT `pip install .` to avoid Playwright/browser deps.
# We run the code directly via PYTHONPATH.

FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app/src

# Install Node.js for the optional WAF signer runtime used by show.json.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates nodejs \
    && if [ ! -x /usr/bin/node ] && [ -x /usr/bin/nodejs ]; then ln -s /usr/bin/nodejs /usr/bin/node; fi \
    && rm -rf /var/lib/apt/lists/*

# Copy only source code.
COPY src /app/src

# Install only RSS server runtime deps (avoid Playwright).
RUN python -m pip install --no-cache-dir "fastapi>=0.111.0" "uvicorn>=0.30.0"

# Persistent data (SQLite) should be mounted here.
RUN mkdir -p /app/data

EXPOSE 8000

CMD ["python", "-m", "xueqiu_crawler.rss_server", "--host", "0.0.0.0", "--port", "8000"]
