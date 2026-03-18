FROM python:3.12-slim-bookworm

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY services/orchestrator/pyproject.toml services/orchestrator/uv.lock ./

RUN uv sync --frozen --no-dev

COPY services/orchestrator/src/ ./src/

# Compactor core logic imported by Dagster assets
COPY services/compactor_ingestor/src/ ./compactor/
RUN uv pip install polars

ENV PYTHONPATH="/app/src:/app/compactor"
