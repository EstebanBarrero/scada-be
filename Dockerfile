FROM python:3.13-slim AS base

WORKDIR /app

# Install gcc — required to compile pandas/numpy C extensions on slim image
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# data/ is tracked in git (contains .gitkeep) but runtime files (DB, CSV, lock)
# are excluded via .dockerignore and written to a mounted volume at runtime.
RUN mkdir -p /app/data

# Pre-generate the synthetic dataset at build time so the ETL endpoint
# can be triggered immediately without a separate setup step.
# The `|| true` ensures the build doesn't fail if numpy/faker have issues
# in certain base image variants.
RUN python etl/generate.py /app/data/raw_alarms.csv || true

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
