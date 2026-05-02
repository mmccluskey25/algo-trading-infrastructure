select *
from {{ ref('stg_ohlc_h4') }}
where
    high < low or
    high < open or
    high < close or
    low > open or
    low > close or
    bid_high < bid_low or
    bid_high < bid_open or
    bid_high < bid_close or
    bid_low > bid_open or
    bid_low > bid_close or
    ask_high < ask_low or
    ask_high < ask_open or
    ask_high < ask_close or
    ask_low > ask_open or
    ask_low > ask_close or
    ask_open < bid_open or
    ask_high < bid_high or
    ask_low < bid_low or
    ask_close < bid_close
