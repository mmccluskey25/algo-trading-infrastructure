FROM python:3.12-slim-bookworm

WORKDIR /app

COPY services/orchestrator/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/orchestrator/pyproject.toml .
COPY services/orchestrator/src/ ./src/

ENV PYTHONPATH="/app/src"

COPY services/orchestrator/entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
