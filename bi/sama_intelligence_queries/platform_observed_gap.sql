select business_id, observed_count as platform_vs_observed_gap
from marts.sama_pilot_business_leakage
where leakage_stage = 'PLATFORM_VS_OBSERVED'
