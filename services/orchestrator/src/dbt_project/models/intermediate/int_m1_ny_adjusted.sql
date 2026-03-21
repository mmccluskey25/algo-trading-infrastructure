select
    instrument,
    candle_open,
    open,
    high,
    low,
    close,
    bid_open,
    bid_high,
    bid_low,
    bid_close,
    ask_open,
    ask_high,
    ask_low,
    ask_close,
    (timezone('America/New_York', candle_open) - interval '{{ var("ny_close_hour") }} hours') as shifted_ny,
    (timezone('America/New_York', candle_open) - interval '{{ var("ny_close_hour") }} hours')::date as trading_date
from {{ ref('stg_ohlc_m1') }}
