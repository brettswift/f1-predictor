# =============================================================================
# Stage 1: builder — install all dependencies
# =============================================================================
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --prefix=/install -r requirements.txt

# =============================================================================
# Stage 2: test — run pytest in isolated environment
# =============================================================================
FROM python:3.12-slim AS test

WORKDIR /app

# Install runtime deps only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed deps from builder
COPY --from=builder /install /usr/local

# Copy source and tests
COPY src/ ./src/
COPY tests/ ./tests/
COPY conftest.py ./

# Run tests and exit
CMD ["pytest", "-v", "--tb=short"]

# =============================================================================
# Stage 3: production — Flask app served by gunicorn
# =============================================================================
FROM python:3.12-slim AS production

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local/

COPY src/ ./src/
COPY templates/ ./templates/
COPY requirements.txt ./

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "app:app"]
