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

{% test sama_business_leakage_reconciles(model) %}
with expected as (
    select business_id, stage, expected_count
    from {{ ref('sama_pilot_unified_overview') }}
    cross join lateral (
        values
            ('PLATFORM_VS_OBSERVED', greatest(platform_conversions - lightfunnels_orders, 0)),
            ('IDENTITY_UNRESOLVED', ambiguous_orders + unmatched_orders),
            ('MATCHED_NOT_CONFIRMED', greatest(matched_orders - confirmed_orders, 0)),
            ('CONFIRMED_NOT_SHIPPED', greatest(confirmed_orders - shipped_orders, 0)),
            ('RETURNED_AFTER_SHIPMENT', returned_orders),
            ('SHIPPED_TERMINAL_UNRESOLVED', greatest(shipped_orders - delivered_orders - returned_orders, 0))
    ) as stages(stage, expected_count)
)
select actual.*
from {{ model }} as actual
full outer join expected
  on actual.business_id = expected.business_id
 and actual.leakage_stage = expected.stage
where actual.business_id is null
   or expected.business_id is null
   or actual.observed_count <> expected.expected_count
{% endtest %}

{% test sama_campaign_diagnostics_reconciles(model) %}
with source_campaigns as (
    select *
    from {{ ref('sama_pilot_tiktok_campaign_outcomes') }}
    where is_target_campaign
), expected as (
    select
        current.business_id,
        current.campaign_id,
        sum(peer.confirmed_orders)::double precision / nullif(sum(peer.matched_orders), 0)
            as peer_confirmation_rate,
        sum(peer.delivered_orders)::double precision / nullif(sum(peer.shipped_orders), 0)
            as peer_delivery_rate,
        sum(peer.returned_orders)::double precision / nullif(sum(peer.shipped_orders), 0)
            as peer_return_rate,
        sum(peer.spend_usd)::double precision / nullif(sum(peer.initial_orders), 0)
            as peer_cost_per_lightfunnels_order_usd,
        sum(peer.spend_usd)::double precision / nullif(sum(peer.delivered_orders), 0)
            as peer_cost_per_delivered_order_usd
    from source_campaigns as current
    join source_campaigns as peer
      on peer.business_id = current.business_id
     and peer.campaign_id <> current.campaign_id
    group by current.business_id, current.campaign_id
)
select actual.*
from {{ model }} as actual
full outer join expected using (business_id, campaign_id)
where actual.business_id is null
   or expected.business_id is null
   or abs(actual.peer_confirmation_rate - expected.peer_confirmation_rate) > 0.000000001
   or abs(actual.peer_delivery_rate - expected.peer_delivery_rate) > 0.000000001
   or abs(actual.peer_return_rate - expected.peer_return_rate) > 0.000000001
   or abs(actual.peer_cost_per_lightfunnels_order_usd - expected.peer_cost_per_lightfunnels_order_usd) > 0.000001
   or abs(actual.peer_cost_per_delivered_order_usd - expected.peer_cost_per_delivered_order_usd) > 0.000001
{% endtest %}

{% test sama_intelligence_signals_valid(model) %}
select signals.*
from {{ model }} as signals
left join {{ ref('sama_pilot_tiktok_campaign_outcomes') }} as campaigns
  on signals.scope_type = 'CAMPAIGN'
 and signals.business_id = campaigns.business_id
 and signals.scope_id = campaigns.campaign_id
 and campaigns.is_target_campaign
where signals.evidence_summary = ''
   or signals.recommended_next_step = ''
   or signals.limitation = ''
   or signals.causal_claim
   or (signals.confidence = 'LOW' and signals.priority = 'HIGH')
   or (signals.scope_type = 'CAMPAIGN' and campaigns.campaign_id is null)
   or lower(signals.evidence_summary || ' ' || signals.why_it_matters || ' ' ||
            signals.recommended_next_step || ' ' || signals.limitation)
      ~ '(phone|email|street address|tracking number)'
   or lower(signals.evidence_summary || ' ' || signals.why_it_matters || ' ' ||
            signals.recommended_next_step)
      ~ '(net profit|business roas|contribution margin|\bmer\b)'
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
