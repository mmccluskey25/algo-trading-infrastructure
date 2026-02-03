FROM python:3.13-slim-bookworm

WORKDIR /app

COPY services/compactor_ingestor/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/compactor_ingestor/src/ ./src/
COPY services/shared/ ./services/shared/

CMD ["tail", "-f", "/dev/null"]
