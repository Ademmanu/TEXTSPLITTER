FROM python:3.11-slim
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

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

# Health check for all bots
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import requests; r=requests.get('http://localhost:8080/health/a', timeout=2); exit(0 if r.status_code==200 else 1)" || exit 1

# Run with optimized gunicorn settings for multiple bots
CMD gunicorn --bind 0.0.0.0:8080 \
    --workers=2 \
    --threads=20 \
    --worker-class=gthread \
    --timeout 30 \
    --keep-alive 5 \
    --max-requests 1000 \
    --max-requests-jitter 50 \
    --log-level info \
    --access-logfile - \
    --error-logfile - \
    app:app
