import dagster as dg

from core import compact_files
from orchestrator.defs.resources import DataPathsResource

landing_ticks = dg.AssetSpec(
    key="landing_ticks",
    description="Raw tick data written by stream_writer service",
    group_name="landing",
)


@dg.asset(
    deps=[landing_ticks],
    partitions_def=dg.DailyPartitionsDefinition(start_date="20260101", fmt="%Y%m%d"),
    description="Deduplicated daily tick data in the bronze layer",
    group_name="bronze",
    kinds={"parquet"},
)
def bronze_ticks(
    context: dg.AssetExecutionContext,
    data_paths: DataPathsResource,
) -> dg.MaterializeResult:
    date_str = context.partition_key

    result = compact_files(
        landing_dir=data_paths.landing_dir,
        bronze_dir=data_paths.bronze_dir,
        date_str=date_str,
        delete_raw=data_paths.delete_after_compaction,
    )

    return dg.MaterializeResult(metadata=result)
