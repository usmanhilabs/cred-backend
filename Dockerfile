# syntax=docker/dockerfile:1

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install minimal OS packages; extend if native deps are needed
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (better cache)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Ensure runtime dirs exist and are writable
RUN mkdir -p /app/uploads /app/ref_uploads \
    && chmod -R 775 /app/uploads

RUN [ -f /etc/apt/sources.list ] || echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list && \
    apt-get update -o Acquire::CompressionTypes::Order::=gz && \
    count=0; \
    until apt-get install -y --no-install-recommends --allow-unauthenticated iputils-ping curl || [ $count -ge 2 ]; do \
        count=$((count + 1)); \
        echo "Retrying... Others (attempt $count)"; \
    done; \
    if [ $count -ge 2 ]; then \
        echo "Installation of Others failed after 3 attempts. Aborting."; \
        exit 1; \
    fi && \
    rm -rf /var/lib/apt/lists/*

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
