{{ config(meta={'dagster_group': 'silver'}) }}

select
    *,
    'oanda' as source
from {{ ref('stg_oanda_ticks') }}
