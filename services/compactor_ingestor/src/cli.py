import argparse, os
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from core import compact_files

load_dotenv(find_dotenv())

def main():
    parser = argparse.ArgumentParser(description="Manual trigger")
    
    parser.add_argument("--delete", action="store_true", help="Delete raw data files after compaction")
    parser.add_argument("--landing", default=os.getenv("LANDING_DIR", "/data/landing"))
    parser.add_argument("--bronze", default=os.getenv("BRONZE_DIR", "/data/bronze/ticks"))
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"))

    args = parser.parse_args()

    print("Manual compaction trigger started")
    compact_files(
        landing_dir=args.landing,
        bronze_dir=args.bronze,
        date_str=args.date,
        delete_raw=args.delete
    )


if __name__ == "__main__":
    main()
    