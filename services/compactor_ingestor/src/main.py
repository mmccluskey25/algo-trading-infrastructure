import os, time
from core import compact_files
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DATA_DIR = os.getenv("DATA_DIR", "./data")
LANDING_DIR = os.getenv("LANDING_DIR", "/data/landing")
BRONZE_DIR = os.getenv("BRONZE_DIR", "/data/bronze")

DELETE_RAW = os.getenv("DELETE_RAW", "true").lower() == "true"
INTERVAL = int(os.getenv("COMPACT_INTERVAL", 240)) # default 4 hours

def main():
    print("Compactor-Ingestor scheduler started")
    print(f"Interval: {INTERVAL} minutes.")
    
    while True:
        compact_files(LANDING_DIR, BRONZE_DIR, delete_raw=DELETE_RAW)

        print("Sleeping for {INTERVAL} minutes...")
        time.sleep(INTERVAL * 60)


if __name__ == "__main__":
    main()