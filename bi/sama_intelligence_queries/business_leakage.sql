select
    business_id,
    category,
    leakage_stage,
    observed_count,
    interpretation,
    stage_order
from marts.sama_pilot_business_leakage
order by stage_order
