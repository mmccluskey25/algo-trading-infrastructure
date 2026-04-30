# Algo Trading Infrastructure
This repository contains the backend infrastructure for a containerised algorithmic trading engine. It uses a dual-path microservices architecture: a hot path for low-latency live trading via Redis, and a cold path for analytics and backtesting via a data lakehouse (Polars, DuckDB, dbt, Dagster).

This represents an architectural evolution of my previous quantitative trading platform. While the original system successfully provided analytics and interactive charting for manual execution using a more traditional stack (Pandas, MongoDB, Plotly Dash), this project is a complete ground-up rewrite designed for automated, algorithmic execution.

It transitions the logic from a monolithic desktop application to a containerised microservices infrastructure, leveraging a modern data lakehouse stack. The goal is to both reimplement the robust analytics of the original platform whilst also expanding capabilities to automated, algorithmic trading all within an environment that enforces enterprise-grade data engineering standards, scalability, and fault tolerance.

## Overview
### 1. Ingestion Layer
- **Oanda Listener**: An asynchronous Python service (`aiohttp`, `asyncio`) that maintains a persistent connection to the Oanda Streaming API. Incoming ticks are fanned out to two Redis queues via a pipeline; one for the cold path (stream writer) and one for the hot path (candle builder).
- **Redis**: Serves a dual role as a message queue (Lists) for passing ticks between services, and as a low-latency data store (Hashes) for live OHLC candles query-able by the trading engine.
- **Stream Writer**: Fault tolerant service that continuously drains the tick queue and writes micro-batches of raw tick data to the Bronze layer. Preserves raw tick data for analytics and data reconstruction.
- **Candle Builder**: Consumes ticks from its dedicated Redis queue and constructs 1-minute OHLC candles in real-time. Completed candles are written to Redis Hashes for the live trading path, and periodically flushed to Bronze layer parquet files for the analytics path.

### 2. Data Lakehouse Layer
- **Dagster Orchestration**: Manages the data pipeline with scheduled jobs and asset-based lineage tracking. Tick compaction runs as a daily partitioned asset. dbt models are integrated as Dagster assets with a custom translator mapping dbt folders to medallion architecture groups.
- **Bronze -> Silver Pipeline (dbt)**: The `stg_ohlc_m1` model reads directly from candle builder's Bronze parquet output. Higher timeframe models (m5, m15, H1, H4, D1) resample from the m1 model. H4 and D1 models apply New York timezone adjustments for forex market close alignment. Tick-based models (stg_oanda_ticks, int_ticks_unioned) exist independently for analytics, research, and future multi-broker union.
- **Silver -> Gold Pipeline (dbt)**: Currently session-aware analytics built in three layers:
  - **Session Candles**: Aggregates m1 candles into forex trading sessions (Tokyo, London, New York, London/NY overlap).
  - **Session Stats**: Per-session volatility metrics including ATR, normalised range, and close positioning within session range, computed via layered CTEs and window functions.
  - **Session Patterns**: Cross-session breakout detection (did this session break the previous session's high/low, including overnight breaks) and ATR-based volatility regime classification using `PERCENT_RANK`.
  - Further analytics models in progress.

### 3. Trading Engine (Planned)
- **Execution Service:** A decoupled execution engine that consumes Gold layer signals for strategy logic while reading Redis candle Hashes for low-latency market data. Will implement broker API for order management and position tracking.
- **Backtesting Framework:** A high-performance simulation engine built on Polars. Designed to run strategies against historical data using vectorised execution. Will use a shared strategy interface with the Execution Service to ensure zero logic drift between historical testing and live deployment.

## Tech stack
- Python 3.13
- **Data Processing:** Polars, DuckDB
- **Infrastructure:** Docker, Docker Compose, Redis
- **Orchestration:** Dagster
- **Transformation:** dbt (Data Build Tool)

## Deployment
The system is designed to be deployed on a single-node server via Docker Compose.

### Prerequisites
- Docker & Docker Compose
- Oanda API Credentials

### Configuration
Environment variables are handled via Portainer injection or a local `.env` file for development. See `.env.example` for the full list.

```bash
# Key variables
ACCOUNT_ID=xxx-xxx-xxx          # Oanda account
API_TOKEN=your_token            # Oanda API token
INSTRUMENTS=EUR_USD             # Comma-separated instrument list
HOST_DATA_PATH=/path/to/data   # Host mount for /data volume
HOST_REDIS_PATH=/path/to/redis # Host mount for Redis persistence
```
