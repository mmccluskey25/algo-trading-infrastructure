from dagster_pipes import open_dagster_pipes

from core import compact_files


def main():
    with open_dagster_pipes() as context:
        extras = context.extras
        context.log.info(f"Starting compaction for {extras['date_str']}")

        compact_files(
            landing_dir=extras["landing_dir"],
            bronze_dir=extras["bronze_dir"],
            date_str=extras["date_str"],
            delete_raw=extras["delete_after_compaction"],
        )

        context.log.info("Compaction complete.")


if __name__ == "__main__":
    main()
