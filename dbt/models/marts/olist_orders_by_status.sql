select business_id, native_status, order_count
from {{ source('analytics', 'olist_orders_by_status') }}
