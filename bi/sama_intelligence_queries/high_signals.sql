select overview.business_id, count(signals.signal_id) as active_high_signals
from marts.sama_pilot_unified_overview as overview
left join marts.sama_pilot_intelligence_signals as signals
  on signals.business_id = overview.business_id
 and signals.priority = 'HIGH'
group by overview.business_id
