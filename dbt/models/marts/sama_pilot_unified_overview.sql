with target_campaigns as (
    select *
    from {{ ref('sama_pilot_tiktok_campaign_outcomes') }}
    where is_target_campaign
), target_metrics as (
    select
        business_id,
        count(*) as target_campaigns,
        max(spend_currency) as marketing_currency,
        sum(spend_usd) as marketing_spend_usd,
        sum(impressions) as impressions,
        sum(destination_clicks) as destination_clicks,
        sum(platform_conversions) as platform_conversions,
        sum(checkouts) as checkouts,
        sum(initial_orders) as lightfunnels_orders,
        sum(matched_orders) as matched_orders,
        sum(ambiguous_orders) as ambiguous_orders,
        sum(unmatched_orders) as unmatched_orders,
        sum(confirmed_orders) as confirmed_orders,
        sum(shipped_orders) as shipped_orders,
        sum(delivered_orders) as delivered_orders,
        sum(returned_orders) as returned_orders,
        sum(out_of_stock_orders) as out_of_stock_orders
    from target_campaigns
    group by business_id
), target_costs as (
    select
        business_id,
        sum(product_cogs_usd) as product_cogs_usd,
        sum(call_center_cost_usd) as call_center_cost_usd,
        sum(logistics_cost_usd) as logistics_cost_usd,
        sum(known_operational_cost_usd) as known_operational_cost_usd
    from {{ source('analytics', 'sama_pilot_order_facts') }}
    where lower(trim(utm_source)) = 'tiktok'
      and match_status = 'MATCHED_HIGH_CONFIDENCE'
    group by business_id
)
select
    metrics.business_id,
    date '2026-03-01' as period_start,
    date '2026-07-31' as period_end,
    'Etc/GMT-1'::text as reporting_timezone,
    metrics.target_campaigns,
    metrics.marketing_currency,
    metrics.marketing_spend_usd,
    metrics.impressions,
    metrics.destination_clicks,
    metrics.platform_conversions,
    metrics.checkouts,
    metrics.lightfunnels_orders,
    metrics.matched_orders,
    metrics.ambiguous_orders,
    metrics.unmatched_orders,
    metrics.confirmed_orders,
    metrics.shipped_orders,
    metrics.delivered_orders,
    metrics.returned_orders,
    metrics.out_of_stock_orders,
    metrics.confirmed_orders::double precision / nullif(metrics.matched_orders, 0)
        as confirmation_rate,
    metrics.delivered_orders::double precision / nullif(metrics.shipped_orders, 0)
        as delivery_rate,
    metrics.returned_orders::double precision / nullif(metrics.shipped_orders, 0)
        as return_rate,
    metrics.marketing_spend_usd::double precision / nullif(metrics.lightfunnels_orders, 0)
        as cost_per_lightfunnels_order_usd,
    metrics.marketing_spend_usd::double precision / nullif(metrics.confirmed_orders, 0)
        as cost_per_confirmed_order_usd,
    metrics.marketing_spend_usd::double precision / nullif(metrics.delivered_orders, 0)
        as marketing_cost_per_delivered_order_usd,
    coalesce(costs.product_cogs_usd, 0) as product_cogs_usd,
    coalesce(costs.call_center_cost_usd, 0) as call_center_cost_usd,
    coalesce(costs.logistics_cost_usd, 0) as logistics_cost_usd,
    coalesce(costs.known_operational_cost_usd, 0) as known_operational_cost_usd,
    metrics.marketing_spend_usd + coalesce(costs.known_operational_cost_usd, 0)
        as total_known_usd_cost,
    (metrics.marketing_spend_usd + coalesce(costs.known_operational_cost_usd, 0))
        / nullif(metrics.delivered_orders, 0) as known_usd_cost_per_delivered_order
from target_metrics as metrics
left join target_costs as costs using (business_id)
