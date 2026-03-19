import dagster as dg

compaction = dg.AssetSelection.assets("oanda_ticks")

compaction_job = dg.define_asset_job(name="compaction_job", selection=compaction)
