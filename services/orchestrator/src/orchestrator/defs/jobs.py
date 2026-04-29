import dagster as dg

from orchestrator.defs.dbt_assets import dbt_project_assets

compaction = dg.AssetSelection.assets("oanda_ticks")
compaction_job = dg.define_asset_job(name="compaction_job", selection=compaction)

dbt_ohlc_build = dg.AssetSelection.assets(dbt_project_assets)
dbt_ohlc_build_job = dg.define_asset_job(
    name="dbt_ohlc_build_job", selection=dbt_ohlc_build
)
