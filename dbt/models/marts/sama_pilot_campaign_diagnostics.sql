with campaigns as (
    select
        business_id,
        campaign_id,
        campaign_name,
        spend_usd,
        platform_conversions,
        initial_orders as lightfunnels_orders,
        matched_orders,
        confirmed_orders,
        shipped_orders,
        delivered_orders,
        returned_orders
    from {{ ref('sama_pilot_tiktok_campaign_outcomes') }}
    where is_target_campaign
), business_totals as (
    select
        business_id,
        sum(spend_usd) as total_spend_usd,
        sum(initial_orders) as total_lightfunnels_orders,
        sum(matched_orders) as total_matched_orders,
        sum(confirmed_orders) as total_confirmed_orders,
        sum(shipped_orders) as total_shipped_orders,
        sum(delivered_orders) as total_delivered_orders,
        sum(returned_orders) as total_returned_orders
    from {{ ref('sama_pilot_tiktok_campaign_outcomes') }}
    where is_target_campaign
    group by business_id
), metrics as (
    select
        campaigns.*,
        campaigns.confirmed_orders::double precision / nullif(campaigns.matched_orders, 0)
            as confirmation_rate,
        campaigns.delivered_orders::double precision / nullif(campaigns.shipped_orders, 0)
            as delivery_rate,
        campaigns.returned_orders::double precision / nullif(campaigns.shipped_orders, 0)
            as return_rate,
        campaigns.spend_usd::double precision / nullif(campaigns.lightfunnels_orders, 0)
            as cost_per_lightfunnels_order_usd,
        campaigns.spend_usd::double precision / nullif(campaigns.confirmed_orders, 0)
            as cost_per_confirmed_order_usd,
        campaigns.spend_usd::double precision / nullif(campaigns.delivered_orders, 0)
            as cost_per_delivered_order_usd,
        (business_totals.total_confirmed_orders - campaigns.confirmed_orders)::double precision
            / nullif(business_totals.total_matched_orders - campaigns.matched_orders, 0)
            as peer_confirmation_rate,
        (business_totals.total_delivered_orders - campaigns.delivered_orders)::double precision
            / nullif(business_totals.total_shipped_orders - campaigns.shipped_orders, 0)
            as peer_delivery_rate,
        (business_totals.total_returned_orders - campaigns.returned_orders)::double precision
            / nullif(business_totals.total_shipped_orders - campaigns.shipped_orders, 0)
            as peer_return_rate,
        (business_totals.total_spend_usd - campaigns.spend_usd)::double precision
            / nullif(business_totals.total_lightfunnels_orders - campaigns.lightfunnels_orders, 0)
            as peer_cost_per_lightfunnels_order_usd,
        (business_totals.total_spend_usd - campaigns.spend_usd)::double precision
            / nullif(business_totals.total_delivered_orders - campaigns.delivered_orders, 0)
            as peer_cost_per_delivered_order_usd,
        business_totals.total_matched_orders - campaigns.matched_orders as peer_matched_orders,
        business_totals.total_shipped_orders - campaigns.shipped_orders as peer_shipped_orders,
        business_totals.total_lightfunnels_orders - campaigns.lightfunnels_orders
            as peer_lightfunnels_orders,
        business_totals.total_delivered_orders - campaigns.delivered_orders as peer_delivered_orders
    from campaigns
    join business_totals using (business_id)
), estimates as (
    select
        *,
        confirmation_rate - peer_confirmation_rate as confirmation_rate_delta,
        delivery_rate - peer_delivery_rate as delivery_rate_delta,
        return_rate - peer_return_rate as return_rate_delta,
        cost_per_delivered_order_usd - peer_cost_per_delivered_order_usd
            as cost_per_delivered_delta_usd,
        platform_conversions - lightfunnels_orders as platform_conversion_order_gap,
        matched_orders * peer_confirmation_rate as expected_confirmed_at_peer_rate,
        greatest(matched_orders * peer_confirmation_rate - confirmed_orders, 0)
            as confirmation_benchmark_gap_orders,
        shipped_orders * peer_delivery_rate as expected_delivered_at_peer_rate,
        greatest(shipped_orders * peer_delivery_rate - delivered_orders, 0)
            as delivery_benchmark_gap_orders,
        shipped_orders * peer_return_rate as expected_returns_at_peer_rate,
        greatest(returned_orders - shipped_orders * peer_return_rate, 0)
            as excess_returns_vs_peer,
        case when matched_orders < 30 then 'LOW'
             when matched_orders < 100 then 'MEDIUM' else 'HIGH' end
            as confirmation_sample_band,
        case when shipped_orders < 30 then 'LOW'
             when shipped_orders < 100 then 'MEDIUM' else 'HIGH' end
            as fulfillment_sample_band,
        case when lightfunnels_orders < 30 then 'LOW'
             when lightfunnels_orders < 100 then 'MEDIUM' else 'HIGH' end
            as acquisition_sample_band,
        case when delivered_orders < 30 then 'LOW'
             when delivered_orders < 100 then 'MEDIUM' else 'HIGH' end
            as cost_delivered_sample_band
    from metrics
)
select
    *,
    fulfillment_sample_band as sample_band,
    'Leave-one-out peer benchmark; benchmark gaps are observational estimates, not causal impact, forecasts, or guaranteed recoverable orders.'::text
        as benchmark_interpretation
from estimates
