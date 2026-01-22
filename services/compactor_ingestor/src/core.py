import os, glob
import polars as pl


def compact_files(landing_dir: str, bronze_dir: str, date_str: str, delete_raw: bool = False):

    files = glob.glob(f"{landing_dir}/{date_str}_*.parquet")

    if not files: 
        return
    
    df = pl.scan_parquet(files).sort("time").collect()

    output_path = f"{bronze_dir}/{date_str}.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.write_parquet(output_path)
    print(f"Compact {len(files)} files to {output_path}")
    
    if delete_raw:
        for f in files:
            os.remove(f)
