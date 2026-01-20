FROM python 3.13-slim-bookworm

WORKDIR /app

COPY services/oanda_listener/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY services/oanda_listener/src/ ./src/

CMD ["python", "src/main.py"]