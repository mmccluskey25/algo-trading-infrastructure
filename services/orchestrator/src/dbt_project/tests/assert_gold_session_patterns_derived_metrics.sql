select *
from {{ ref ('gold_session_patterns') }}
where
    session_atr_rank < 0 or
    session_atr_rank > 1
