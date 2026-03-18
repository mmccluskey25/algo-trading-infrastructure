FROM python:3.12-slim-bookworm

WORKDIR /app

COPY services/orchestrator/pyproject.toml .
RUN pip install --no-cache-dir dagster dagster-webserver

COPY services/orchestrator/src/ ./src/

# Compactor core logic imported by Dagster assets
COPY services/compactor_ingestor/src/ ./compactor/
RUN pip install --no-cache-dir polars

ENV PYTHONPATH="/app/src:/app/compactor"
