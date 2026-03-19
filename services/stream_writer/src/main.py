import json
import time
from datetime import datetime
from pathlib import Path

import polars as pl
import redis
from services.shared.config import settings

Path(settings.landing_dir).mkdir(parents=True, exist_ok=True)


def connect_redis():
    while True:
        try:
            r = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=0,
                decode_responses=True,
            )
            r.ping()
            print(f"Writer connected to redis at {settings.redis_host}")
            return r
        except redis.ConnectionError:
            print("Writer waiting for redis...")
            time.sleep(5)


def process_batch(r):
    try:
        q_len = r.llen(settings.queue_key)

        if q_len == 0:
            return

        print(f"Processing batch of {q_len} ticks...")

        raw_data = r.lpop(settings.queue_key, count=q_len)

        if not raw_data:
            return

        data_dicts = [json.loads(x) for x in raw_data]

        df = pl.DataFrame(data_dicts)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for partition_df in df.partition_by("instrument"):
            instrument = partition_df["instrument"][0]
            output_dir = Path(settings.landing_dir) / "ticks" / settings.broker_name / instrument
            output_dir.mkdir(parents=True, exist_ok=True)

            filepath = output_dir / f"{timestamp}.parquet"
            partition_df.write_parquet(filepath, compression="zstd")

            print(f"Saved {filepath} ({len(partition_df)} rows)")

    except Exception as e:
        print(f"Error: {e}")


def main():
    r = connect_redis()

    while True:
        time.sleep(settings.writer_batch_interval)
        process_batch(r)


if __name__ == "__main__":
    main()
