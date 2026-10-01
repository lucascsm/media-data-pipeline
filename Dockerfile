# syntax=docker/dockerfile:1
FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY scripts ./scripts
COPY fluxo_antigo ./fluxo_antigo
COPY weekly_reports.py .
# Credentials are mounted from Secret Manager at runtime, never baked into the image.
RUN mkdir -p /app/config /mnt/gcs/media/output/backup
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src:/app CONFIG_DIR=/app/config GCS_MOUNT=/mnt/gcs/media
ENTRYPOINT ["python", "-u", "-m", "src.main"]
