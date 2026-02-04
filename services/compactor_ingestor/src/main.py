import time
from datetime import datetime

from services.shared.config import settings

from core import compact_files


def main():
    print("Compactor-Ingestor scheduler started")
    print(f"Interval: {settings.compaction_interval_mins} minutes.")

    while True:
        compact_files(
            landing_dir=settings.landing_dir,
            bronze_dir=settings.bronze_dir,
            date_str=datetime.now().strftime("%Y%m%d"),
            delete_raw=settings.delete_after_compaction,
        )

        print(f"Sleeping for {settings.compaction_interval_mins} minutes...")
        time.sleep(settings.compaction_interval_mins * 60)


if __name__ == "__main__":
    main()
