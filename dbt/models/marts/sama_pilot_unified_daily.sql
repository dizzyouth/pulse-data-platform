with target_campaigns as (
    select business_id, campaign_id
    from {{ ref('sama_pilot_tiktok_campaign_outcomes') }}
    where is_target_campaign
), marketing as (
    select
        native.business_id,
        native.report_date,
        max(native.reporting_timezone) as reporting_timezone,
        sum(native.spend_usd) as spend_usd,
        sum(native.impressions) as impressions,
        sum(native.destination_clicks) as destination_clicks,
        sum(native.platform_conversions) as platform_conversions,
        sum(native.checkouts) as checkouts
    from {{ ref('sama_pilot_tiktok_native_performance') }} as native
    inner join target_campaigns using (business_id, campaign_id)
    where native.report_date between date '2026-03-01' and date '2026-07-31'
    group by native.business_id, native.report_date
), outcomes as (
    select
        outcome.business_id,
        outcome.report_date,
        sum(outcome.initial_orders) as lightfunnels_orders,
        sum(outcome.matched_orders) as matched_orders,
        sum(outcome.ambiguous_orders) as ambiguous_orders,
        sum(outcome.unmatched_orders) as unmatched_orders,
        sum(outcome.confirmed_orders) as confirmed_orders,
        sum(outcome.shipped_orders) as shipped_orders,
        sum(outcome.delivered_orders) as delivered_orders,
        sum(outcome.returned_orders) as returned_orders,
        sum(outcome.out_of_stock_orders) as out_of_stock_orders
    from {{ source('analytics', 'sama_pilot_tiktok_order_outcomes_daily') }} as outcome
    inner join target_campaigns using (business_id, campaign_id)
    where outcome.report_date between date '2026-03-01' and date '2026-07-31'
    group by outcome.business_id, outcome.report_date
), combined as (
    select
        coalesce(marketing.business_id, outcomes.business_id) as business_id,
        coalesce(marketing.report_date, outcomes.report_date) as report_date,
        coalesce(marketing.reporting_timezone, 'Etc/GMT-1') as reporting_timezone,
        coalesce(marketing.spend_usd, 0) as spend_usd,
        coalesce(marketing.impressions, 0) as impressions,
        coalesce(marketing.destination_clicks, 0) as destination_clicks,
        coalesce(marketing.platform_conversions, 0) as platform_conversions,
        coalesce(marketing.checkouts, 0) as checkouts,
        coalesce(outcomes.lightfunnels_orders, 0) as lightfunnels_orders,
        coalesce(outcomes.matched_orders, 0) as matched_orders,
        coalesce(outcomes.ambiguous_orders, 0) as ambiguous_orders,
        coalesce(outcomes.unmatched_orders, 0) as unmatched_orders,
        coalesce(outcomes.confirmed_orders, 0) as confirmed_orders,
        coalesce(outcomes.shipped_orders, 0) as shipped_orders,
        coalesce(outcomes.delivered_orders, 0) as delivered_orders,
        coalesce(outcomes.returned_orders, 0) as returned_orders,
        coalesce(outcomes.out_of_stock_orders, 0) as out_of_stock_orders
    from marketing
    full outer join outcomes using (business_id, report_date)
)
select
    *,
    confirmed_orders::double precision / nullif(matched_orders, 0) as confirmation_rate,
    delivered_orders::double precision / nullif(shipped_orders, 0) as delivery_rate,
    returned_orders::double precision / nullif(shipped_orders, 0) as return_rate,
    spend_usd::double precision / nullif(lightfunnels_orders, 0) as marketing_cost_per_order_usd,
    spend_usd::double precision / nullif(confirmed_orders, 0) as marketing_cost_per_confirmed_usd,
    spend_usd::double precision / nullif(delivered_orders, 0) as marketing_cost_per_delivered_usd
from combined
