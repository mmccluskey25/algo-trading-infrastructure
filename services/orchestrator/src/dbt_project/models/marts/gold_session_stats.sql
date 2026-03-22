with base as (
    select
        instrument,
        trading_date,
        session_name,
        open, high, low, close,
        (high - low) as session_range,
        lag(close,1) over (
            partition by instrument, session_name
            order by trading_date
        ) as prev_close
    from {{ ref('gold_session_candles') }}
),
tr as (
    select
        *,
        session_range / open * 100 as session_range_pct,
        greatest(high-low, abs(high-prev_close), abs(low-prev_close)) as true_range,
        (close - low) / NULLIF(session_range, 0) as close_vs_range
    from base
)

select
    *,
    avg(true_range) over (
        partition by instrument, session_name
        order by trading_date
        rows between 4 preceding and current row
    ) as atr_5
from tr
