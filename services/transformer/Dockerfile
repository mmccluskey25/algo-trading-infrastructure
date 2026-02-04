FROM python:3.13-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

COPY services/transformer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/transformer/ .

CMD ["tail", "-f", "/dev/null"]
