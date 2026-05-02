select *
from {{ ref ('gold_session_stats') }}
where
    session_range < 0 or
    true_range < session_range or
    close_vs_range < 0 or
    close_vs_range > 1 or
    atr_5 < 0
