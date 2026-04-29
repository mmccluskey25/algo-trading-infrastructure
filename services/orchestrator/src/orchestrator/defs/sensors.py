import dagster as dg
import os
from pathlib import Path
from orchestrator.defs.jobs import dbt_ohlc_build_job

BRONZE_OHLC_DIR = Path(os.environ["DATA_ROOT"]) / "bronze" / "ohlc_m1"

@dg.sensor(job=dbt_ohlc_build_job, minimum_interval_seconds=60)
def bronze_ohlc_sensor(context: dg.SensorEvaluationContext):
    all_files = sorted(
        str(f.name) for f in BRONZE_OHLC_DIR.rglob("*.parquet")
    )

    if not all_files:
        yield dg.SkipReason("No parquet files found in bronze OHLC directory")
        return

    cursor = context.cursor
    new_files = [f for f in all_files if f > cursor] if cursor else all_files

    if not new_files:
        yield dg.SkipReason("No new parquet files since last run")
        return

    latest = new_files[-1]
    context.update_cursor(latest)
    yield dg.RunRequest(run_key=f"dbt_ohlc_{latest}")
