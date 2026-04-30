import dagster as dg

oanda_m1 = dg.AssetSpec(
    key="oanda_m1",
    description="m1 OHLC candles produced by candle_builder service",
    group_name="bronze",
    owners=["team:data-engineering"],
    tags={"domain": "forex", "cadence": "streaming"},
    metadata={
        "source_service": "candle_builder",
        "update_cadence": "Every 60s during market hours",
    },
)
