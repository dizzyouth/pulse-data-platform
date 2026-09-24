select distinct on (business_id)
    business_id,
    leakage_stage,
    observed_count as largest_operational_leakage
from marts.sama_pilot_business_leakage
where is_operational_gap
order by business_id, observed_count desc, stage_order
