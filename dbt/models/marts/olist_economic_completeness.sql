select
    business_id,
    currency,
    economic_status,
    order_count,
    cogs_available,
    attribution_available,
    profit_calculated
from {{ source('analytics', 'olist_economic_completeness') }}
