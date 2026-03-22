with session_order as (
    select * from (values
        ('tokyo', 1),
        ('london', 2),
        ('new_york', 3)
    ) as r(session_name, session_order)
),
base as (
    select
        instrument,
        trading_date,
        session_name,
        r.session_order,
        open, high, low, close,
        session_range,
        true_range,
        atr_5,
        close_vs_range
    from {{ ref('gold_session_stats') }} s
    join session_order r on s.session_name = r.session_name
    where session_name != 'london_ny_overlap'
),
prev as (
    select
        *,
        lag(close, 1) over (
            partition by instrument
            order by trading_date, session_order
        ) as prev_session_close,
        lag(high, 1) over (
            partition by instrument
            order by trading_date, session_order
        ) as prev_session_high,
        lag(low, 1) over (
            partition by instrument
            order by trading_date, session_order
        ) as prev_session_low
    from base
),
session_break as (
    select
        *,
        high > prev_session_high as broke_prev_high,
        low < prev_session_low as broke_prev_low
    from prev
),
vol_rank as (
    select
        *,
        percent_rank() over (
            partition by instrument, session_name
            order by atr_5
        ) as session_atr_rank
    from session_break
),
vol_regime as (
    select
        *,
        case
            when atr_rank < 0.25 then 'low'
            when atr_rank > 0.75 then 'high'
            else 'normal'
        end as session_vol_regime
    from vol_rank
)

select * from vol_regime
