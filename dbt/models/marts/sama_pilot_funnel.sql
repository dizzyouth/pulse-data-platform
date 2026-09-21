with identity as (
    select
        business_id,
        count(*) as initial_orders,
        count(*) filter (where match_status = 'MATCHED_HIGH_CONFIDENCE') as matched_orders,
        count(*) filter (where match_status = 'AMBIGUOUS') as ambiguous_orders,
        count(*) filter (where match_status = 'UNMATCHED') as unmatched_orders
    from {{ source('analytics', 'sama_pilot_identity_resolution') }}
    group by business_id
), lifecycle as (
    select
        business_id,
        count(*) filter (where match_status = 'MATCHED_HIGH_CONFIDENCE' and confirmed) as confirmed_leads,
        count(*) filter (where match_status = 'MATCHED_HIGH_CONFIDENCE' and cod_order_reference is not null) as final_cod_orders,
        count(*) filter (where match_status = 'MATCHED_HIGH_CONFIDENCE' and shipped) as shipped_orders,
        count(*) filter (where match_status = 'MATCHED_HIGH_CONFIDENCE' and delivered) as delivered_orders,
        count(*) filter (where match_status = 'MATCHED_HIGH_CONFIDENCE' and returned) as returned_orders,
        count(*) filter (where match_status = 'MATCHED_HIGH_CONFIDENCE' and out_of_stock) as out_of_stock_orders
    from {{ source('analytics', 'sama_pilot_order_facts') }}
    group by business_id
)
select
    identity.*,
    lifecycle.confirmed_leads,
    lifecycle.final_cod_orders,
    lifecycle.shipped_orders,
    lifecycle.delivered_orders,
    lifecycle.returned_orders,
    lifecycle.out_of_stock_orders,
    lifecycle.confirmed_leads::double precision / nullif(identity.matched_orders, 0) as confirmation_rate,
    lifecycle.delivered_orders::double precision / nullif(lifecycle.shipped_orders, 0) as delivery_rate,
    lifecycle.returned_orders::double precision / nullif(lifecycle.shipped_orders, 0) as return_rate
from identity
join lifecycle using (business_id)
