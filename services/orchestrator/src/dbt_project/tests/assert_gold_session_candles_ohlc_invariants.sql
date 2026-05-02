select *
from {{ ref('gold_session_candles') }}
where
    high < low or
    high < open or
    high < close or
    low > open or
    low > close
