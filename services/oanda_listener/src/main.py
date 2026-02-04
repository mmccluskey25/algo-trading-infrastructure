import asyncio
import json
import os

import aiohttp
from redis.asyncio import Redis
from services.shared.config import settings

if not settings.account_id or not settings.api_token:
    raise ValueError("Missing Oanda credentials (ACCOUNT_ID or API_TOKEN")

URL = f"https://stream-fxtrade.oanda.com/v3/accounts/{settings.account_id}/pricing/stream?instruments={settings.instruments}"
print(URL)


async def oanda_ingest():
    print(f"Attempting connection to Redis at {settings.redis_host}...")
    try:
        r = Redis(host=settings.redis_host, port=settings.redis_port, db=0)
        await r.ping()
        print("Redis Connected.")
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return

    headers = {"Authorization": f"Bearer {settings.api_token}"}

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
