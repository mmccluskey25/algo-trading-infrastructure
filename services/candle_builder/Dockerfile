FROM python:3.13-slim-bookworm

WORKDIR /app

COPY services/candle_builder/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/candle_builder/src/ ./src/
COPY services/shared/ ./services/shared/

ENV PYTHONPATH="${PYTHONPATH}:/app"

CMD ["python", "src/main.py"]
