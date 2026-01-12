# Dockerfile
FROM python:3.11-slim
WORKDIR /app

# Install minimal system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd -m -u 1000 worker && \
    mkdir -p /tmp && \
    chown -R worker:worker /app /tmp

USER worker

EXPOSE 8080

# Health check - lightweight version
HEALTHCHECK --interval=45s --timeout=10s --start-period=30s --retries=2 \
    CMD python -c "import requests; r = requests.get('http://localhost:8080/health/a', timeout=5); exit(0 if r.status_code == 200 else 1)" || exit 1

# Start with optimized gunicorn settings for limited resources
CMD python -c "import threading; threading.stack_size(512*1024)" && \
    gunicorn --bind 0.0.0.0:8080 \
    --workers=1 \
    --threads=20 \           # Reduced from 40 to 20 for 500m CPU
    --worker-class=gthread \
    --timeout=30 \
    --keepalive=5 \
    --max-requests=5000 \    # Reduced from 10000
    --max-requests-jitter=500 \
    --graceful-timeout=30 \
    --limit-request-line=4096 \  # Reduced from 8190
    --preload \              # Preload app to save memory
    app:app
