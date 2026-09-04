select
    event_date,
    currency,
    sum(completed_orders)::bigint as completed_orders,
    sum(units_sold)::bigint as units_sold,
    sum(gross_revenue)::double precision as gross_revenue,
    (
        sum(gross_revenue) / nullif(sum(completed_orders), 0)
    )::double precision as avg_order_value
from {{ source('analytics', 'daily_sales') }}
group by event_date, currency
