import time, json, os, redis
import polars as pl
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
QUEUE_KEY = "tick_queue:oanda"

OUTPUT_DIR = os.getenv("DATA_DIR", "./data/bronze/ticks")
BATCH_INTERVAL = os.getenv("WRITER_BATCH_INTERVAL", 60)

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

def connect_redis():
    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
            r.ping()
            print(f"Writer connected to redis at {REDIS_HOST}")
            return r
        except redis.ConnectionError:
            print("Writer waiting for redis...")
            time.sleep(5)


def process_batch(r):
    try:
        q_len = r.llen(QUEUE_KEY)
    
        if q_len == 0:
            return
        
        print(f"Processing batch of {q_len} ticks...")

        raw_data = r.lpop(QUEUE_KEY, count=q_len)

        if not raw_data:
            return
        
        data_dicts = [json.loads(x) for x in raw_data]
        
        df = pl.DataFrame(data_dicts)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") 
        filename = f"oanda_ticks_{timestamp}.parquet"
        filepath = os.path.join(OUTPUT_DIR, filename)

        df.write_parquet(filepath, compression="zstd")

        print(f"Saved {filepath} ({len(df)} rows)")

    except Exception as e:
        print("Error: {e}")
        

def main():
    r = connect_redis()

    while True:
        time.sleep(BATCH_INTERVAL)
        process_batch(r)

if __name__ == "__main__":
    main()
        