select
    business_id,
    signal_order,
    priority,
    signal_type as signal,
    scope_name as scope,
    impact_order_count as impact,
    confidence
from marts.sama_pilot_intelligence_signals
order by signal_order
