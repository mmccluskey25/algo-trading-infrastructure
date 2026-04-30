import dagster as dg

from orchestrator.defs.partitions import tick_partition
from orchestrator.defs.resources import DataPathsResource
from orchestrator.lib.compaction import compact_files

landing_ticks = dg.AssetSpec(
    key="landing_ticks",
    description="Raw tick data written by stream_writer service",
    group_name="landing",
    owners=["team:data-engineering"],
    tags={"domain": "forex", "cadence": "streaming"},
    metadata={
        "source_service": "stream_writer",
        "update_cadence": "Continuous during market hours",
    },
)


@dg.asset(
    deps=[landing_ticks],
    partitions_def=tick_partition,
    description="Deduplicated daily tick data in the bronze layer, partitioned by instrument",
    group_name="bronze",
    kinds={"parquet"},
    owners=["team:data-engineering"],
    tags={"domain": "forex", "cadence": "daily"},
)
def oanda_ticks(
    context: dg.AssetExecutionContext,
    data_paths: DataPathsResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    date_str = partition_key.keys_by_dimension["date"]
    instrument = partition_key.keys_by_dimension["instrument"]

    result = compact_files(
        landing_dir=data_paths.landing_dir,
        bronze_dir=data_paths.bronze_dir,
        date_str=date_str,
        delete_raw=data_paths.delete_after_compaction,
        instrument=instrument,
        broker="oanda",
    )

    return dg.MaterializeResult(metadata=result)
