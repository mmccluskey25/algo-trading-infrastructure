import os, glob
import polars as pl


def compact_files(landing_dir: str, bronze_dir: str, date_str: str, delete_raw: bool = False):
    print(f'Looking for files in {landing_dir}...')
    files = glob.glob(f"{landing_dir}/*_{date_str}_*.parquet")
    
    if not files: 
        print("No files found. Exiting.")
        return
    
    print(f"Found {len(files)} files.")
    
    df = pl.scan_parquet(files)

    output_path = f"{bronze_dir}/{date_str}.parquet"
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
