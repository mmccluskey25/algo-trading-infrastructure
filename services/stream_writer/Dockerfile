FROM python:3.13-slim-bookworm

WORKDIR /app

COPY services/stream_writer/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/stream_writer/src/ ./src/
COPY services/shared/ ./services/shared/

ENV PYTHONPATH="${PYTHONPATH}:/app"

CMD ["python", "src/main.py"]
