select
    customer_id,
    total_revenue,
    total_units_purchased,
    payments_completed,
    distinct_orders,
    rank() over (order by total_revenue desc) as revenue_rank
from {{ source('analytics', 'customer_metrics') }}
