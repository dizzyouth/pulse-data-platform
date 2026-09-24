select distinct
    business_id,
    signal_type,
    scope_name as scope,
    limitation
from marts.sama_pilot_intelligence_signals
order by signal_type, scope_name
