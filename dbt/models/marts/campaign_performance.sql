select
    *,
    dense_rank() over (
        partition by business_id, platform, report_date, currency
        order by spend desc
    ) as spend_rank
from {{ source('analytics', 'campaign_performance') }}
