FROM python:3.9-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=10000
RUN useradd -m -u 1000 user && chown -R user:user /app
USER user
CMD ["sh", "-c", "gunicorn app:app --bind 0.0.0.0:$PORT --timeout 120"]
