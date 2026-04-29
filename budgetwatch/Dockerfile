FROM python:3.12-slim

WORKDIR /app

# System deps (curl for healthchecks, tzdata for Asia/Jakarta)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && cp /usr/share/zoneinfo/Asia/Jakarta /etc/localtime \
    && echo "Asia/Jakarta" > /etc/timezone

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY *.py ./

# Storage for raw API payloads
RUN mkdir -p /var/budgetwatch/raw

EXPOSE 8000

# Use 1 worker for the prototype — single-process is fine for beta traffic
# Bump to gunicorn workers when traffic grows
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
