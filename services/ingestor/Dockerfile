FROM python 3.13-slim-bookworm

WORKDIR /app

COPY services/ingestor/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/ingestor/src/ ./src/

CMD ["python", "src/main.py"]