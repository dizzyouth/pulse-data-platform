{% test nonnegative(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}

{% test contribution_formula(model) %}
select *
from {{ model }}
where cogs_complete
  and (
    contribution_before_marketing is null
    or abs(contribution_before_marketing
           - (recognized_economic_value - variable_operational_cost)) > 0.00000001
  )
{% endtest %}

{% test unique_combination(model, column_names) %}
select {{ column_names | join(', ') }}
from {{ model }}
group by {{ column_names | join(', ') }}
having count(*) > 1
{% endtest %}

{% test between_zero_and_one(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 1)
{% endtest %}

{% test expression_is_true(model, expression) %}
select *
from {{ model }}
where not ({{ expression }})
{% endtest %}

{% test date_between(model, column_name, start_date, end_date) %}
select *
from {{ model }}
where {{ column_name }} < date '{{ start_date }}'
   or {{ column_name }} > date '{{ end_date }}'
{% endtest %}

{% test sama_unified_overview_reconciles(model) %}
with expected as (
    select
        business_id,
        count(*) as target_campaigns,
        sum(spend_usd) as marketing_spend_usd,
        sum(initial_orders) as lightfunnels_orders,
        sum(matched_orders) as matched_orders,
        sum(ambiguous_orders) as ambiguous_orders,
        sum(unmatched_orders) as unmatched_orders,
        sum(confirmed_orders) as confirmed_orders,
        sum(shipped_orders) as shipped_orders,
        sum(delivered_orders) as delivered_orders,
        sum(returned_orders) as returned_orders,
        sum(out_of_stock_orders) as out_of_stock_orders
    from {{ ref('sama_pilot_tiktok_campaign_outcomes') }}
    where is_target_campaign
    group by business_id
)
select actual.*
from {{ model }} as actual
join expected using (business_id)
where actual.target_campaigns <> expected.target_campaigns
   or abs(actual.marketing_spend_usd - expected.marketing_spend_usd) > 0.000001
   or actual.lightfunnels_orders <> expected.lightfunnels_orders
   or actual.matched_orders <> expected.matched_orders
   or actual.ambiguous_orders <> expected.ambiguous_orders
   or actual.unmatched_orders <> expected.unmatched_orders
   or actual.confirmed_orders <> expected.confirmed_orders
   or actual.shipped_orders <> expected.shipped_orders
   or actual.delivered_orders <> expected.delivered_orders
   or actual.returned_orders <> expected.returned_orders
   or actual.out_of_stock_orders <> expected.out_of_stock_orders
{% endtest %}

{% test sama_unified_daily_reconciles(model) %}
with daily as (
    select
        business_id,
        sum(spend_usd) as marketing_spend_usd,
        sum(lightfunnels_orders) as lightfunnels_orders,
        sum(matched_orders) as matched_orders,
        sum(ambiguous_orders) as ambiguous_orders,
        sum(unmatched_orders) as unmatched_orders,
        sum(confirmed_orders) as confirmed_orders,
        sum(shipped_orders) as shipped_orders,
        sum(delivered_orders) as delivered_orders,
        sum(returned_orders) as returned_orders,
        sum(out_of_stock_orders) as out_of_stock_orders
    from {{ model }}
    group by business_id
)
select daily.*
from daily
join {{ ref('sama_pilot_unified_overview') }} as overview using (business_id)
where abs(daily.marketing_spend_usd - overview.marketing_spend_usd) > 0.000001
   or daily.lightfunnels_orders <> overview.lightfunnels_orders
   or daily.matched_orders <> overview.matched_orders
   or daily.ambiguous_orders <> overview.ambiguous_orders
   or daily.unmatched_orders <> overview.unmatched_orders
   or daily.confirmed_orders <> overview.confirmed_orders
   or daily.shipped_orders <> overview.shipped_orders
   or daily.delivered_orders <> overview.delivered_orders
   or daily.returned_orders <> overview.returned_orders
   or daily.out_of_stock_orders <> overview.out_of_stock_orders
{% endtest %}

{% test sama_unified_fact_cohort_reconciles(model) %}
with expected as (
    select
        business_id,
        count(*) as lightfunnels_orders,
        count(*) filter (where match_status = 'MATCHED_HIGH_CONFIDENCE') as matched_orders
    from {{ source('analytics', 'sama_pilot_order_facts') }}
    where lower(trim(utm_source)) = 'tiktok'
    group by business_id
)
select actual.*
from {{ model }} as actual
join expected using (business_id)
where actual.lightfunnels_orders <> expected.lightfunnels_orders
   or actual.matched_orders <> expected.matched_orders
{% endtest %}

{% test sama_unified_native_reconciles(model) %}
with economics as (
    select
        business_id,
        sum(product_cogs_usd) as product_cogs_usd,
        sum(call_center_cost_usd) as call_center_cost_usd,
        sum(logistics_cost_usd) as logistics_cost_usd,
        sum(known_operational_cost_usd) as known_operational_cost_usd
    from {{ model }}
    group by business_id
)
select economics.*
from economics
join {{ ref('sama_pilot_unified_overview') }} as overview using (business_id)
where abs(economics.product_cogs_usd - overview.product_cogs_usd) > 0.000001
   or abs(economics.call_center_cost_usd - overview.call_center_cost_usd) > 0.000001
   or abs(economics.logistics_cost_usd - overview.logistics_cost_usd) > 0.000001
   or abs(economics.known_operational_cost_usd - overview.known_operational_cost_usd) > 0.000001
{% endtest %}

{% test uci_daily_signed_reconciliation(model) %}
select *
from {{ model }}
where abs(net_ledger_value - (
    positive_merchandise_value + cancellation_value + adjustment_value + non_merchandise_value
)) > 0.000001
{% endtest %}

{% test uci_invoice_signed_reconciliation(model) %}
select *
from {{ model }}
where abs(net_ledger_value - (
    positive_merchandise_value + cancellation_value + adjustment_value + non_merchandise_value
)) > 0.000001
{% endtest %}
