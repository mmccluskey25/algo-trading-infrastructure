select
    instrument,
    bucket as candle_open,
    mid_open as open,
    mid_high as high,
    mid_low as low,
    mid_close as close,
    bid_open,
    bid_high,
    bid_low,
    bid_close,
    ask_open,
    ask_high,
    ask_low,
    ask_close,
    tick_count

from {{ source('forex', 'oanda_m1') }}
