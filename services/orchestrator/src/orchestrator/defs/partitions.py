import dagster as dg

daily_partition = dg.DailyPartitionsDefinition(start_date="20260101", fmt="%Y%m%d")

instrument_partition = dg.StaticPartitionsDefinition(["EUR_USD"])

tick_partition = dg.MultiPartitionsDefinition(
    {"date": daily_partition, "instrument": instrument_partition}
)
