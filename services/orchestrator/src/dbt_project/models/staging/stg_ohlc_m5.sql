select
    instrument,
    time_bucket(interval '5 minutes', candle_open) as candle_open,
    first(open order by candle_open) as open,
    max(high) as high,
    min(low) as low,
    last(close order by candle_open) as close,
    first(bid_open order by candle_open) as bid_open,
    max(bid_high) as bid_high,
    min(bid_low) as bid_low,
    last(bid_close order by candle_open) as bid_close,
    first(ask_open order by candle_open) as ask_open,
    max(ask_high) as ask_high,
    min(ask_low) as ask_low,
    last(ask_close order by candle_open) as ask_close

from {{ ref('stg_ohlc_m1') }}
group by instrument, time_bucket(interval '5 minutes', candle_open)
