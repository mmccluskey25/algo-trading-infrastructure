FROM python:3.12-slim-bookworm

WORKDIR /app

COPY services/orchestrator/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/orchestrator/src/ ./src/

ENV PYTHONPATH="/app/src"
