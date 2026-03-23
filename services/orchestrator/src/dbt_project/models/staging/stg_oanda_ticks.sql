{{ config(meta={'dagster_group': 'bronze'}, location=env_var('DATA_ROOT') ~ "/bronze/" ~ this.identifier ~ ".parquet") }}

select
    instrument,
    time::timestamptz as tick_time,
    bids[1].price::double as bid,
    asks[1].price::double as ask,
    (bids[1].price::double + asks[1].price::double) / 2 as mid,
    status
from {{ source('forex', 'oanda_ticks') }}
where status = 'tradeable'
