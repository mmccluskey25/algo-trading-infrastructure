select *
from {{ ref('stg_oanda_ticks') }}
where ask < bid
