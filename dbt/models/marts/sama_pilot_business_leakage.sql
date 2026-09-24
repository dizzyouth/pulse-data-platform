with overview as (
    select *
    from {{ ref('sama_pilot_unified_overview') }}
), leakage as (
    select
        business_id,
        period_end as as_of_date,
        leakage_stage,
        category,
        observed_count::bigint as observed_count,
        denominator_count::bigint as denominator_count,
        observed_count::double precision / nullif(denominator_count, 0) as observed_rate,
        interpretation,
        is_measurement_gap,
        is_operational_gap,
        stage_order
    from overview
    cross join lateral (
        values
            (
                'PLATFORM_VS_OBSERVED', 'MEASUREMENT',
                greatest(platform_conversions - lightfunnels_orders, 0), platform_conversions,
                'Difference between platform-reported conversions and observed Lightfunnels orders; not automatically lost orders.',
                true, false, 1
            ),
            (
                'IDENTITY_UNRESOLVED', 'IDENTITY',
                ambiguous_orders + unmatched_orders, lightfunnels_orders,
                'Observed orders whose downstream identity is ambiguous or unmatched.',
                false, false, 2
            ),
            (
                'MATCHED_NOT_CONFIRMED', 'CONFIRMATION',
                greatest(matched_orders - confirmed_orders, 0), matched_orders,
                'High-confidence matched orders not observed as confirmed.',
                false, true, 3
            ),
            (
                'CONFIRMED_NOT_SHIPPED', 'FULFILLMENT',
                greatest(confirmed_orders - shipped_orders, 0), confirmed_orders,
                'Confirmed orders not observed as shipped.',
                false, true, 4
            ),
            (
                'RETURNED_AFTER_SHIPMENT', 'FULFILLMENT',
                returned_orders, shipped_orders,
                'Shipped orders observed with a returned terminal outcome.',
                false, true, 5
            ),
            (
                'SHIPPED_TERMINAL_UNRESOLVED', 'FULFILLMENT',
                greatest(shipped_orders - delivered_orders - returned_orders, 0), shipped_orders,
                'Shipped orders with neither a delivered nor returned terminal outcome in the current data.',
                false, true, 6
            )
    ) as stages(
        leakage_stage, category, observed_count, denominator_count,
        interpretation, is_measurement_gap, is_operational_gap, stage_order
    )
)
select *
from leakage
