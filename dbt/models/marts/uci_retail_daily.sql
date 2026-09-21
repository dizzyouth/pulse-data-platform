select
    business_id,
    event_date,
    currency,
    invoice_count,
    normal_invoice_count,
    cancellation_invoice_count,
    anonymous_customer_invoice_count,
    anonymous_customer_invoice_count::double precision / nullif(invoice_count, 0) as anonymous_customer_rate,
    line_count,
    positive_merchandise_value,
    cancellation_value,
    adjustment_value,
    non_merchandise_value,
    net_ledger_value,
    zero_price_line_count,
    negative_price_line_count
from {{ source('analytics', 'uci_retail_daily') }}
