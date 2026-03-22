with sessions as (
    select * from (values
        ('tokyo', '00:00'::time, '09:00'::time),
        ('london', '08:00'::time, '17:00'::time),
        ('new_york', '13:00'::time, '22:00'::time),
        ('london_ny_overlap', '13:00'::time, '17:00'::time)
    ) as s(session_name, session_start, session_end)
)

select
    instrument,
    candle_open::date as trading_date,
    s.session_name,
    first(open order by candle_open) as open,
    max(high) as high,
    min(low) as low,
    last(close order by candle_open) as close
from {{ ref('stg_ohlc_m1') }} m1
join sessions s on
    m1.candle_open::time >= s.session_start and
    m1.candle_open::time < s.session_end
group by instrument, candle_open, s.session_name
