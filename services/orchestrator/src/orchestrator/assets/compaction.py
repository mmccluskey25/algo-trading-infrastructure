from datetime import datetime

from dagster import AssetExecutionContext, AssetSpec, PipesSubprocessClient, asset

from src.orchestrator.resources import DataPathsResource

landing_ticks = AssetSpec(
    key="landing_ticks",
    description="Raw tick data written by stream_writer service",
    group_name="landing",
)


@asset(
    deps=[landing_ticks],
    description="Deduplicated daily tick data in the bronze layer",
    group_name="bronze",
)
def bronze_ticks(
    context: AssetExecutionContext,
    data_paths: DataPathsResource,
    pipes_client: PipesSubprocessClient,
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
