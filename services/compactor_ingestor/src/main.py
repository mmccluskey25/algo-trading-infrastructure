import os, glob, datetime
import polars as pl

LANDING_DIR = "/data/landing"
BRONZE_DIR = "/data/bronze"

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