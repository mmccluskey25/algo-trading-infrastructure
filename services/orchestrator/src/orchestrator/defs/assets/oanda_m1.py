import dagster as dg

oanda_m1 = dg.AssetSpec(
    key="oanda_m1",
    description="m1 OHLC candles produced by candle_builder service",
    group_name="bronze",
)
