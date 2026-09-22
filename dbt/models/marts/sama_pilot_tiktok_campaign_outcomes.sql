with tiktok as (
    select
        business_id,
        campaign_id,
        max(campaign_name) as campaign_name,
        currency,
        sum(spend) as spend_usd,
        sum(impressions) as impressions,
        sum(clicks) as destination_clicks,
        sum(platform_conversions) as platform_conversions,
        sum((details_json::jsonb ->> 'checkouts_initiated')::double precision) as checkouts
    from {{ source('analytics', 'sama_pilot_tiktok_ad_daily') }}
    group by business_id, campaign_id, currency
), outcomes as (
    select
        business_id,
        campaign_id,
        max(campaign_name) as campaign_name,
        sum(initial_orders) as initial_orders,
        sum(matched_orders) as matched_orders,
        sum(ambiguous_orders) as ambiguous_orders,
        sum(unmatched_orders) as unmatched_orders,
        sum(confirmed_orders) as confirmed_orders,
        sum(shipped_orders) as shipped_orders,
        sum(delivered_orders) as delivered_orders,
        sum(returned_orders) as returned_orders,
        sum(out_of_stock_orders) as out_of_stock_orders
    from {{ source('analytics', 'sama_pilot_tiktok_order_outcomes_daily') }}
    group by business_id, campaign_id
)
select
    tiktok.business_id,
    tiktok.campaign_id,
    coalesce(outcomes.campaign_name, tiktok.campaign_name) as campaign_name,
    tiktok.currency as spend_currency,
    outcomes.campaign_id is not null as is_target_campaign,
    tiktok.spend_usd,
    tiktok.impressions,
    tiktok.destination_clicks,
    tiktok.platform_conversions,
    tiktok.checkouts,
    coalesce(outcomes.initial_orders, 0) as initial_orders,
    coalesce(outcomes.matched_orders, 0) as matched_orders,
    coalesce(outcomes.ambiguous_orders, 0) as ambiguous_orders,
    coalesce(outcomes.unmatched_orders, 0) as unmatched_orders,
    coalesce(outcomes.confirmed_orders, 0) as confirmed_orders,
    coalesce(outcomes.shipped_orders, 0) as shipped_orders,
    coalesce(outcomes.delivered_orders, 0) as delivered_orders,
    coalesce(outcomes.returned_orders, 0) as returned_orders,
    coalesce(outcomes.out_of_stock_orders, 0) as out_of_stock_orders,
    tiktok.spend_usd::double precision / nullif(tiktok.platform_conversions, 0) as platform_cpa_usd,
    tiktok.platform_conversions::double precision / nullif(tiktok.destination_clicks, 0)
        as platform_conversion_rate,
    tiktok.spend_usd::double precision / nullif(outcomes.initial_orders, 0)
        as cost_per_lightfunnels_order_usd,
    tiktok.spend_usd::double precision / nullif(outcomes.confirmed_orders, 0)
        as cost_per_confirmed_order_usd,
    tiktok.spend_usd::double precision / nullif(outcomes.shipped_orders, 0)
        as cost_per_shipped_order_usd,
    tiktok.spend_usd::double precision / nullif(outcomes.delivered_orders, 0)
        as cost_per_delivered_order_usd,
    outcomes.confirmed_orders::double precision / nullif(outcomes.matched_orders, 0)
        as confirmation_rate,
    outcomes.delivered_orders::double precision / nullif(outcomes.shipped_orders, 0)
        as delivery_rate,
    outcomes.returned_orders::double precision / nullif(outcomes.shipped_orders, 0)
        as return_rate,
    tiktok.platform_conversions - coalesce(outcomes.initial_orders, 0)
        as platform_conversion_order_gap
from tiktok
left join outcomes using (business_id, campaign_id)
