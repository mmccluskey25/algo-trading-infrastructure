import json
import time

import redis

from services.shared.config import settings


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


def main():
    r = connect_redis()
    print(f"Listening on {settings.candle_builder_queue_key}...")

    while True:
        result = r.blpop(settings.candle_builder_queue_key, timeout=0)

        if result is None:
            continue

        _key, raw = result
        tick = json.loads(raw)

        print(f"Tick: {tick.get('instrument')} @ {tick.get('time')}")


if __name__ == "__main__":
    main()
