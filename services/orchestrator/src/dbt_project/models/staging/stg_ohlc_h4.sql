select
    instrument,
    timezone('UTC',
        time_bucket(interval '4 hours', shifted_ny)
            + interval '{{ var("ny_close_hour") }} hours'
    ) as candle_open,
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

from {{ ref('int_ticks_ny_adjusted') }}
group by instrument, candle_open
