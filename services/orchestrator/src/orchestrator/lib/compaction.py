import glob
import os
from datetime import datetime

import polars as pl


def compact_files(
    landing_dir: str,
    bronze_dir: str,
    date_str: str = None,
    delete_raw: bool = False,
    instrument: str = None,
    broker: str = "oanda",
):
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    landing_path = f"{landing_dir}/ticks/{broker}/{instrument}"
    print(f"Looking for files in {landing_path}...")
    files = glob.glob(f"{landing_path}/{date_str}_*.parquet")

    if not files:
        print("No files found. Exiting.")
        return {
            "files_compacted": 0,
            "row_count": 0,
            "output_path": None,
            "files_deleted": 0,
            "instrument": instrument,
            "broker": broker,
        }

    print(f"Found {len(files)} files.")

    df = pl.scan_parquet(files)

    output_path = f"{bronze_dir}/ticks/{broker}/{instrument}/{date_str}.parquet"
    # Check for existing bronze parquet and merge if necessary
    if os.path.exists(output_path):
        print(f"Existing bronze file found at {output_path}. Merging...")
        try:
            history_df = pl.scan_parquet(output_path)
            df = pl.concat([history_df, df])

        except Exception as e:
            print(f"Failed: {e}")

    df = df.unique().sort("time").collect()

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write_parquet(output_path)

    print(f"Compacted {len(files)} files to {output_path}")

    if delete_raw:
        for f in files:
            os.remove(f)
        print(f"Deleted {len(files)} raw data files from Landing.")

    return {
        "files_compacted": len(files),
        "row_count": len(df),
        "output_path": output_path,
        "files_deleted": len(files) if delete_raw else 0,
        "instrument": instrument,
        "broker": broker,
    }
