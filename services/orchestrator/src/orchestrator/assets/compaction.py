from datetime import datetime

from dagster import AssetExecutionContext, AssetSpec, asset

landing_ticks = AssetSpec(
    key="landing_ticks",
    description="Raw tick data written by stream_writer service",
    group_name="landing",
)


# currently a stub
@asset(
    deps=[landing_ticks],
    description="Deduplicated daily tick data in the bronze layer",
    group_name="bronze",
)
def bronze_ticks(context: AssetExecutionContext):
    date_str = datetime.now().strftime("%Y%m%d")
    context.log.info(f"{date_str}")
