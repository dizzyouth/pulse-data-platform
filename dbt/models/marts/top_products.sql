select
    business_id,
    product_id,
    seller_id,
    gross_revenue,
    units_sold,
    payments_completed,
    distinct_customers,
    rank() over (partition by business_id order by gross_revenue desc) as revenue_rank
from {{ source('analytics', 'product_metrics') }}
