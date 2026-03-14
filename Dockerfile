# Minimal image: do NOT `pip install .` to avoid Playwright/browser deps.
# We run the code directly via PYTHONPATH.

FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app/src

# Copy only source code.
COPY src /app/src

# Install only RSS server runtime deps (avoid Playwright).
RUN python -m pip install --no-cache-dir "fastapi>=0.111.0" "uvicorn>=0.30.0"

# Persistent data (SQLite) should be mounted here.
RUN mkdir -p /app/data

EXPOSE 8000

CMD ["python", "-m", "xueqiu_crawler.rss_server", "--host", "0.0.0.0", "--port", "8000"]
