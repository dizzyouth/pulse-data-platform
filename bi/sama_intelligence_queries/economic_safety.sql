select
    business_id,
    'FX_REQUIRED'::text as economic_status,
    'Cross-currency profit, contribution, margin, MER, and business ROAS remain unavailable without trusted FX.'::text
        as limitation
from marts.sama_pilot_unified_overview
