FROM python:3.13-slim-bookworm

WORKDIR /app

COPY services/transformer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["tail", "-f", "/dev/null"]