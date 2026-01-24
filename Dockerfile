FROM python:3.11-slim
WORKDIR /app

# Install PostgreSQL dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd -m -u 1000 worker
USER worker
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health/a', timeout=2)" || exit 1
CMD python -c "import threading; threading.stack_size(1024*1024)" && \
    gunicorn --bind 0.0.0.0:8080 --workers=1 --threads=40 --worker-class=gthread --timeout=120 app:app
