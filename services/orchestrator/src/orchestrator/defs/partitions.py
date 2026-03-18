import dagster as dg

daily_partition = dg.DailyPartitionsDefinition(start_date="20260101", fmt="%Y%m%d")
