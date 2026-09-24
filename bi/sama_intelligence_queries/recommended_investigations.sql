select
    business_id,
    signal_order,
    signal_type as signal,
    scope_name as scope,
    recommended_next_step as recommended_investigation
from marts.sama_pilot_intelligence_signals
order by signal_order
