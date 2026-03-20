select
    instrument,
    time_bucket(interval '1 minute', tick_time) as candle_open,
    first(mid order by tick_time) as open,
    max(mid) as high,
    min(mid) as low,
    last(mid order by tick_time) as close,
    first(bid order by tick_time) as bid_open,
    max(bid) as bid_high,
    min(bid) as bid_low,
    last(bid order by tick_time) as bid_close,
    first(ask order by tick_time) as ask_open,
    max(ask) as ask_high,
    min(ask) as ask_low,
    last(ask order by tick_time) as ask_close

from {{ ref('int_ticks_unioned') }}
group by instrument, candle_open
