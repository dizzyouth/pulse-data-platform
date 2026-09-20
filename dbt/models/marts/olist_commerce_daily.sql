select
    business_id,
    event_date,
    currency,
    orders,
    merchandise_value,
    customer_freight_charge,
    commerce_calculated_total,
    payment_total,
    payment_total - commerce_calculated_total as aggregate_payment_reconciliation_delta
from {{ source('analytics', 'olist_commerce_daily') }}
