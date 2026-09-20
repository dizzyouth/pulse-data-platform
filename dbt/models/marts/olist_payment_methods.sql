select business_id, payment_method, currency, payment_rows, payment_amount
from {{ source('analytics', 'olist_payment_methods') }}
