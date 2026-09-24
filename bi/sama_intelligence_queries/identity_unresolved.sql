select business_id, observed_count as identity_unresolved
from marts.sama_pilot_business_leakage
where leakage_stage = 'IDENTITY_UNRESOLVED'
