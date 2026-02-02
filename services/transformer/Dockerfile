FROM python:3.13-slim-bookworm

WORKDIR /app

COPY services/transformer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DBT_PROFILES_DIR=/app

CMD ["tail", "-f", "/dev/null"]