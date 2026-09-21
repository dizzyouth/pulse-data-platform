select
    business_id,
    initial_quantity,
    final_quantity,
    currency,
    count(*) as order_count,
    count(*) filter (where quantity_changed) as quantity_changed_orders,
    count(*) filter (where value_changed) as value_changed_orders,
    sum(initial_total) as initial_value,
    sum(final_total) as final_value
from {{ source('analytics', 'sama_pilot_order_facts') }}
where match_status = 'MATCHED_HIGH_CONFIDENCE'
  and cod_order_reference is not null
group by business_id, initial_quantity, final_quantity, currency
