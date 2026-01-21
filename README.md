# Algo Trading Infrastructure
This repository contains the backend infrastructure for a containerised algorithmic trading engine. It uses a hybrid (event-driven & batch) microservices architecture to ensure fault tolerance and scalability.

This represents an architectural evolution of my previous quantitative trading platform. While the original system successfully provided analytics and interactive charting for manual execution using a more traditional stack (Pandas, MongoDB, Plotly Dash), this project is a complete ground-up rewrite designed for automated, algorithmic execution.

It transitions the logic from a monolithic desktop application to a containerised microservices infrastructure, leveraging a modern data lakehouse stack (Polars, DuckDB, dbt, Dagster). The goal is to both reimplement the robust analytics of the original platform whilst also expanding capabilities to automated, algorithmic trading all within an environment that enforces enterprise-grade data engineering standards, scalability, and fault tolerance.

## Overview
### 1. Ingestion Layer
- **Oanda Listener**: An asynchronous Python service (`aiohttp`, `asyncio`) that maintains a persistent connection to the Oanda Streaming API. Incoming ticks are pushed to a Redis queue.
- **Redis buffer**: Acts as a high-speed temporary buffer to decouple network I/O from disk I/O
- **Stream Writer**: Fault tolerant service that continuously empties the Redis buffer and writes micro-batches of raw tick data to the Landing Zone (in Parquet format).
- **Compactor-Ingestor Service**: Periodic batch job that processes the Landing Zone. Using Polars it performs basic de-duplication and file compaction. The compacted files are ingested into the Bronze layer.

### 2. Data Lakehouse Layer (WIP)
- A planned integration with dbt and DuckDB
- Bronze to Silver Pipeline: Cleans and resamples tick data to m1 OHLC candles
- Silver to Gold Pipeline: Further resampling to higher timeframes, feature engineering and signals

### 3. Trading Engine (Planned)
- **Execution Service:** A decoupled execution engine that consumes "Gold" layer signals for strategy logic while using the Redis live stream for precise, low-latency entry/exit timing. Implements the Oanda v20 REST API for order management and position tracking.
- **Backtesting Framework:** A high-performance simulation engine built on Polars. It is designed to run strategies against historical data using vectorised execution for speed. Will use a shared strategy interface with the Execution Service to ensure zero logic drift between historical testing and live deployment.

## Tech stack
- Python 3.13
- **Data Processing:** Polars, DuckDB
- **Infrastructure:** Docker, Docker Compose, Redis
- **Orchestration:** Dagster (Planned)
- **Transformation:** dbt (Data Build Tool)

## Deployment
The system is designed to be deployed on a single-node server via Docker Compose.

### Prerequisites
- Docker & Docker Compose
- Oanda API Credentials

### Configuration
Environment variables are handled via Portainer injection or a local `.env` file for development.

```bash
# Required Environment Variables
REDIS_HOST=redis # redis container name
REDIS_PORT=6379
HOST_REDIS_PATH=/mnt/user/appdata/redis
ACCOUNT_ID=xxx-xxx-xxx
API_TOKEN=your_token
INSTRUMENTS=EUR_USD,GBP_USD
OANDA_ENV=practice # or live
HOST_DATA_PATH=/mnt/user/trading_data
```