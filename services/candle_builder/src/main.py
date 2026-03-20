import json
import signal
import time
from datetime import datetime
from pathlib import Path

import polars as pl
import redis

from services.shared.config import settings
from src.candle import Candle, CandleAccumulator

BRONZE_CANDLE_DIR = Path(settings.bronze_dir) / "ohlc_m1"


def connect_redis() -> redis.Redis:
    while True:
        try:
            r = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=0,
                decode_responses=True,
            )
            r.ping()
            print(f"Connected to Redis at {settings.redis_host}:{settings.redis_port}")
            return r
        except redis.ConnectionError:
            print("Redis not available, retrying in 5s...")
            time.sleep(5)


def candle_to_hash(candle: Candle) -> dict[str, str]:
    """Convert a Candle to a flat dict of strings for Redis HSET."""
    return {
        "instrument": candle.instrument,
        "bucket": candle.bucket.isoformat(),
        "bid_open": str(candle.bid_open),
        "bid_high": str(candle.bid_high),
        "bid_low": str(candle.bid_low),
        "bid_close": str(candle.bid_close),
        "ask_open": str(candle.ask_open),
        "ask_high": str(candle.ask_high),
        "ask_low": str(candle.ask_low),
        "ask_close": str(candle.ask_close),
        "mid_open": str(candle.mid_open),
        "mid_high": str(candle.mid_high),
        "mid_low": str(candle.mid_low),
        "mid_close": str(candle.mid_close),
        "tick_count": str(candle.tick_count),
    }


def candle_to_row(candle: Candle) -> dict:
    """Convert a Candle to a dict for Polars DataFrame construction."""
    return {
        "instrument": candle.instrument,
        "bucket": candle.bucket,
        "bid_open": candle.bid_open,
        "bid_high": candle.bid_high,
        "bid_low": candle.bid_low,
        "bid_close": candle.bid_close,
        "ask_open": candle.ask_open,
        "ask_high": candle.ask_high,
        "ask_low": candle.ask_low,
        "ask_close": candle.ask_close,
        "mid_open": candle.mid_open,
        "mid_high": candle.mid_high,
        "mid_low": candle.mid_low,
        "mid_close": candle.mid_close,
        "tick_count": candle.tick_count,
    }


def write_completed_candle(r: redis.Redis, candle: Candle) -> None:
    """Write a completed candle to Redis: Hash + Sorted Set index + TTL."""
    epoch = int(candle.bucket.timestamp())
    instrument = candle.instrument

    hash_key = f"candle:oanda:{instrument}:m1:{epoch}"
    index_key = f"candle_index:oanda:{instrument}:m1"

    pipe = r.pipeline(transaction=False)
    pipe.hset(hash_key, mapping=candle_to_hash(candle))
    pipe.expire(hash_key, settings.candle_redis_ttl)
    pipe.zadd(index_key, {str(epoch): epoch})
    pipe.execute()

    print(
        f"Candle complete: {instrument} {candle.bucket.isoformat()} "
        f"O={candle.mid_open:.5f} H={candle.mid_high:.5f} "
        f"L={candle.mid_low:.5f} C={candle.mid_close:.5f} "
        f"ticks={candle.tick_count}"
    )


def write_current_candle(r: redis.Redis, candle: Candle) -> None:
    """Update the in-progress candle Hash in Redis."""
    hash_key = f"candle:oanda:{candle.instrument}:m1:current"
    r.hset(hash_key, mapping=candle_to_hash(candle))


def flush_to_parquet(buffer: list[dict]) -> None:
    """Write buffered candles to a parquet file, partitioned by instrument."""
    if not buffer:
        return

    df = pl.DataFrame(buffer)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for partition_df in df.partition_by("instrument"):
        instrument = partition_df["instrument"][0]
        output_dir = BRONZE_CANDLE_DIR / instrument
        output_dir.mkdir(parents=True, exist_ok=True)

        filepath = output_dir / f"{timestamp}.parquet"
        partition_df.write_parquet(filepath, compression="zstd")
        print(f"Flushed {len(partition_df)} candles to {filepath}")


def main():
    r = connect_redis()
    accumulator = CandleAccumulator()
    parquet_buffer: list[dict] = []
    last_flush = time.monotonic()

    # Flush buffer on shutdown
    shutting_down = False

    def handle_signal(signum, frame):
        nonlocal shutting_down
        print("Shutdown signal received, flushing buffer...")
        shutting_down = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    print(f"Listening on {settings.candle_builder_queue_key}...")

    while not shutting_down:
        # Timeout lets the loop unblock periodically to check flush timer
        result = r.blpop(settings.candle_builder_queue_key, timeout=5)

        if result is not None:
            _key, raw = result
            tick = json.loads(raw)

            completed = accumulator.on_tick(tick)

            if completed is not None:
                write_completed_candle(r, completed)
                parquet_buffer.append(candle_to_row(completed))

            instrument = tick.get("instrument")
            if instrument:
                current = accumulator.current_candle(instrument)
                if current is not None:
                    write_current_candle(r, current)

        # Check if it's time to flush to parquet
        elapsed = time.monotonic() - last_flush
        if elapsed >= settings.candle_flush_interval and parquet_buffer:
            flush_to_parquet(parquet_buffer)
            parquet_buffer.clear()
            last_flush = time.monotonic()

    # Final flush on shutdown
    flush_to_parquet(parquet_buffer)
    print("Shutdown complete.")


if __name__ == "__main__":
    main()
