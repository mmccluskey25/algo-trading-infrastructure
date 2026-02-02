import asyncio
import json
import os

import aiohttp
from dotenv import find_dotenv, load_dotenv
from redis.asyncio import Redis

load_dotenv(find_dotenv())

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

ACCOUNT_ID = os.getenv("ACCOUNT_ID")
API_TOKEN = os.getenv("API_TOKEN")
INSTRUMENTS = os.getenv("INSTRUMENTS")
URL = f"https://stream-fxtrade.oanda.com/v3/accounts/{ACCOUNT_ID}/pricing/stream?instruments={INSTRUMENTS}"

print(URL)


async def oanda_ingest():
    print(f"Attempting connection to Redis at {REDIS_HOST}...")
    try:
        r = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        await r.ping()
        print("Redis Connected.")
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return

    headers = {"Authorization": f"Bearer {API_TOKEN}"}

    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        print("Connecting to Oanda Stream...")
        while True:
            try:
                async with session.get(URL, timeout=0) as response:
                    # Check HTTP status
                    if response.status != 200:
                        print(f"Oanda Error: Status {response.status}")
                        error_body = await response.text()
                        print(error_body)
                        await asyncio.sleep(5)
                        continue

                    print("Stream Connected. Listening...")

                    while True:
                        line_bytes = await response.content.readline()

                        if not line_bytes:
                            print("Connection closed by server.")
                            break

                        line_str = line_bytes.decode("utf-8").strip()

                        if not line_str:  # skip empty
                            continue

                        try:
                            data = json.loads(line_str)
                            if data.get("type") == "PRICE":
                                await r.rpush("tick_queue:oanda", json.dumps(data))

                        except json.JSONDecodeError:
                            pass

            except Exception as e:
                print(f"Stream error: {e}. Reconnecting...")
                await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(oanda_ingest())
