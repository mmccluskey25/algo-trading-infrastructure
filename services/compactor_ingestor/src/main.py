import os, glob, datetime
import polars as pl
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


DATA_DIR = os.getenv("DATA_DIR", "./data")
LANDING_DIR = os.getenv("LANDING_DIR", "/data/landing")
BRONZE_DIR = os.getenv("BRONZE_DIR", "/data/bronze")

def compact_files(date):
    date_str = date.strftime("%Y%m%d")
    files = glob.glob(f"{LANDING_DIR}/ticks{date_str}_*.parquet")

    if not files: 
        return
    
    df = pl.scan_parquet(files).sort("time").collect()

    output_path = f"{BRONZE_DIR}/ticks/{date_str}.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.write_parquet(output_path)
    print(f"Compact {len(files)} files to {output_path}")
    
    for f in files:
        os.remove(f)


# adapt code to be containerised
# fix env variables
# add "dry run" capability
# create dockerfile
