from datetime import datetime

import dagster as dg

from orchestrator.defs.resources import DataPathsResource

landing_ticks = dg.AssetSpec(
    key="landing_ticks",
    description="Raw tick data written by stream_writer service",
    group_name="landing",
)


@dg.asset(
    deps=[landing_ticks],
    description="Deduplicated daily tick data in the bronze layer",
    group_name="bronze",
    kinds={"parquet"},
)
def bronze_ticks(
    context: dg.AssetExecutionContext,
    data_paths: DataPathsResource,
    pipes_client: dg.PipesSubprocessClient,
):
    date_str = datetime.now().strftime("%Y%m%d")

    pipes_client.run(
        command=["python", "services/compactor_ingestor/src/main.py"],
        extras={
            "landing_dir": data_paths.landing_dir,
            "bronze_dir": data_paths.bronze_dir,
            "date_str": date_str,
            "delete_after_compaction": data_paths.delete_after_compaction,
        },
        context=context,
    ).get_materialize_result()
