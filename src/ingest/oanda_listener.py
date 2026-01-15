import aiohttp, asyncio, json, os
import redis.asyncio as redis
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379)) 

ACCOUNT_ID = os.getenv("ACCOUNT_ID")
API_TOKEN = os.getenv("API_TOKEN")
INSTRUMENTS= os.getenv("INSTRUMENTS")
URL = f"https://stream-fxtrade.oanda.com/v3/accounts/{ACCOUNT_ID}/pricing/stream?instruments={INSTRUMENTS}"

async def oanda_ingest():
    
    print("Attempting connection to Redis at {REDIS_HOST}...")
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        await r.ping()
        print("Redis Connected.")
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return
    
    headers = {"Authorization": f"Bearer {API_TOKEN}"}

    
    async with aiohttp.ClientSession(headers=headers) as session:
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

                    async for line in response.content:
                        if line:
                            decoded_line = line.decode('utf-8')
                            data = json.loads(decoded_line)

                            if data.get('type') == 'PRICE':
                                await r.rpush("tick_queue:oanda", json.dumps(data))

            except Exception as e:
                print(f"Stream error: {e}. Reconnecting...")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(oanda_ingest())
