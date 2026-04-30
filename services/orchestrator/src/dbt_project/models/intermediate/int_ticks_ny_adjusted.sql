select
    instrument,
    mid,
    bid,
    ask,
    tick_time,
    (timezone('America/New_York', tick_time) - interval '{{ var("ny_close_hour") }} hours') as shifted_ny,
    (timezone('America/New_York', tick_time) - interval '{{ var("ny_close_hour") }} hours')::date as trading_date
from {{ ref('int_ticks_unioned') }}
