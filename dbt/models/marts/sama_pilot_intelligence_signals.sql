with overview as (
    select * from {{ ref('sama_pilot_unified_overview') }}
), leakage as (
    select * from {{ ref('sama_pilot_business_leakage') }}
), business_candidates as (
    select
        concat(business_id, '|BUSINESS|RETURN_PRESSURE') as signal_id,
        business_id,
        as_of_date,
        'BUSINESS'::text as scope_type,
        business_id::text as scope_id,
        business_id::text as scope_name,
        'RETURN_PRESSURE'::text as signal_type,
        'FULFILLMENT'::text as signal_category,
        'returned_orders'::text as metric_name,
        returned_orders::double precision as observed_value,
        delivered_orders::double precision as baseline_value,
        (returned_orders - delivered_orders)::double precision as absolute_gap,
        (returned_orders - delivered_orders)::double precision / nullif(delivered_orders, 0)
            as relative_gap,
        shipped_orders::bigint as sample_size,
        returned_orders::double precision as impact_order_count,
        concat('More shipped orders were returned than delivered: ', returned_orders,
               ' returned vs ', delivered_orders, ' delivered; return rate ',
               round(return_rate::numeric * 100, 2), '%.') as evidence_summary,
        'Returns are the largest observed downstream volume leakage.'::text as why_it_matters,
        'Investigate fulfillment and return drivers before scaling acquisition.'::text
            as recommended_next_step,
        'Current analytical data does not identify a causal return reason or establish a causal link to creative, fulfillment, or customer quality.'::text
            as limitation,
        case when shipped_orders < 30 then 'LOW'
             when shipped_orders < 100 then 'MEDIUM' else 'HIGH' end as confidence,
        false as causal_claim
    from overview
    join leakage using (business_id)
    where leakage_stage = 'RETURNED_AFTER_SHIPMENT'
      and returned_orders > delivered_orders

    union all

    select
        concat(business_id, '|BUSINESS|CONFIRMATION_LEAKAGE'), business_id, as_of_date,
        'BUSINESS', business_id, business_id, 'CONFIRMATION_LEAKAGE', 'CONFIRMATION',
        'matched_not_confirmed_orders', observed_count::double precision, 0::double precision,
        observed_count::double precision, null::double precision, denominator_count,
        observed_count::double precision,
        concat(observed_count, ' of ', denominator_count,
               ' high-confidence matched orders were not observed as confirmed.'),
        'The observed confirmation gap affects a material share of matched order intent.',
        'Review confirmation operations and matched-order disposition.',
        'Current data identifies the stage where the gap appears but does not prove why an order was not confirmed.',
        case when denominator_count < 30 then 'LOW'
             when denominator_count < 100 then 'MEDIUM' else 'HIGH' end,
        false
    from leakage
    where leakage_stage = 'MATCHED_NOT_CONFIRMED' and observed_count > 0

    union all

    select
        concat(business_id, '|BUSINESS|PLATFORM_OBSERVED_GAP'), business_id, period_end,
        'BUSINESS', business_id, business_id, 'PLATFORM_OBSERVED_GAP', 'MEASUREMENT',
        'platform_conversions', platform_conversions::double precision,
        lightfunnels_orders::double precision,
        (platform_conversions - lightfunnels_orders)::double precision,
        (platform_conversions - lightfunnels_orders)::double precision
            / nullif(lightfunnels_orders, 0), lightfunnels_orders,
        greatest(platform_conversions - lightfunnels_orders, 0)::double precision,
        concat(platform_conversions, ' platform-reported conversions vs ', lightfunnels_orders,
               ' observed Lightfunnels orders; difference ',
               platform_conversions - lightfunnels_orders, '.'),
        'Platform and observed-order measures use different semantics and should be reconciled before interpretation.',
        'Review event attribution and platform-versus-observed measurement semantics.',
        'The difference is a measurement gap, not automatically lost orders, fraud, or a tracking failure.',
        case when lightfunnels_orders < 30 then 'LOW'
             when lightfunnels_orders < 100 then 'MEDIUM' else 'HIGH' end,
        false
    from overview
    where platform_conversions > lightfunnels_orders

    union all

    select
        concat(business_id, '|BUSINESS|IDENTITY_RESOLUTION_GAP'), business_id, as_of_date,
        'BUSINESS', business_id, business_id, 'IDENTITY_RESOLUTION_GAP', 'IDENTITY',
        'unresolved_identity_orders', observed_count::double precision, 0::double precision,
        observed_count::double precision, null::double precision, denominator_count,
        observed_count::double precision,
        concat(observed_count, ' observed orders have ambiguous or unmatched downstream identity.'),
        'Unresolved identity limits confidence in downstream campaign outcome linkage.',
        'Review sanitized identity-resolution inputs and ambiguous match evidence.',
        'Current data does not establish a unique downstream identity for these records and exposes no raw PII.',
        case when denominator_count < 30 then 'LOW'
             when denominator_count < 100 then 'MEDIUM' else 'HIGH' end,
        false
    from leakage
    where leakage_stage = 'IDENTITY_UNRESOLVED' and observed_count > 0
), campaign_candidates as (
    select
        concat(business_id, '|CAMPAIGN|', campaign_id, '|DELIVERY_GAP') as signal_id,
        business_id,
        (select max(period_end) from overview where overview.business_id = diagnostics.business_id)
            as as_of_date,
        'CAMPAIGN'::text as scope_type,
        campaign_id::text as scope_id,
        campaign_name::text as scope_name,
        'CAMPAIGN_DELIVERY_GAP'::text as signal_type,
        'FULFILLMENT'::text as signal_category,
        'delivery_rate'::text as metric_name,
        delivery_rate as observed_value,
        peer_delivery_rate as baseline_value,
        peer_delivery_rate - delivery_rate as absolute_gap,
        (peer_delivery_rate - delivery_rate) / nullif(peer_delivery_rate, 0) as relative_gap,
        shipped_orders::bigint as sample_size,
        delivery_benchmark_gap_orders as impact_order_count,
        concat('Observed delivery rate ', round(delivery_rate::numeric * 100, 2),
               '% vs leave-one-out peer rate ', round(peer_delivery_rate::numeric * 100, 2),
               '%; benchmark gap ', round(delivery_benchmark_gap_orders::numeric, 2), ' orders.'),
        'This campaign differs materially from the other target campaigns on observed downstream delivery.',
        'Review this campaign''s downstream order quality before increasing spend.',
        'The benchmark gap is observational, not causal impact, a forecast, or guaranteed recoverable orders.',
        fulfillment_sample_band as confidence,
        false as causal_claim
    from {{ ref('sama_pilot_campaign_diagnostics') }} as diagnostics
    where fulfillment_sample_band in ('MEDIUM', 'HIGH')
      and peer_delivery_rate - delivery_rate >= 0.10
      and delivery_benchmark_gap_orders > 0

    union all

    select
        concat(business_id, '|CAMPAIGN|', campaign_id, '|RETURN_PRESSURE'), business_id,
        (select max(period_end) from overview where overview.business_id = diagnostics.business_id),
        'CAMPAIGN', campaign_id, campaign_name, 'CAMPAIGN_RETURN_PRESSURE', 'FULFILLMENT',
        'return_rate', return_rate, peer_return_rate, return_rate - peer_return_rate,
        (return_rate - peer_return_rate) / nullif(peer_return_rate, 0), shipped_orders,
        excess_returns_vs_peer,
        concat('Observed return rate ', round(return_rate::numeric * 100, 2),
               '% vs leave-one-out peer rate ', round(peer_return_rate::numeric * 100, 2),
               '%; excess-vs-peer benchmark gap ', round(excess_returns_vs_peer::numeric, 2), ' orders.'),
        'This campaign differs materially from peers on observed return outcomes.',
        'Review downstream order and fulfillment evidence associated with this campaign.',
        'Current data cannot establish a causal return reason or attribute the difference to creative, fulfillment, or customer quality.',
        fulfillment_sample_band, false
    from {{ ref('sama_pilot_campaign_diagnostics') }} as diagnostics
    where fulfillment_sample_band in ('MEDIUM', 'HIGH')
      and return_rate - peer_return_rate >= 0.10
      and excess_returns_vs_peer > 0

    union all

    select
        concat(business_id, '|CAMPAIGN|', campaign_id, '|ACQUISITION_COST_GAP'), business_id,
        (select max(period_end) from overview where overview.business_id = diagnostics.business_id),
        'CAMPAIGN', campaign_id, campaign_name, 'CAMPAIGN_ACQUISITION_COST_GAP', 'ACQUISITION',
        'cost_per_lightfunnels_order_usd', cost_per_lightfunnels_order_usd,
        peer_cost_per_lightfunnels_order_usd,
        cost_per_lightfunnels_order_usd - peer_cost_per_lightfunnels_order_usd,
        (cost_per_lightfunnels_order_usd - peer_cost_per_lightfunnels_order_usd)
            / nullif(peer_cost_per_lightfunnels_order_usd, 0), lightfunnels_orders,
        null::double precision,
        concat('Observed cost per Lightfunnels order $',
               round(cost_per_lightfunnels_order_usd::numeric, 2),
               ' vs leave-one-out peer cost $',
               round(peer_cost_per_lightfunnels_order_usd::numeric, 2), '.'),
        'Acquisition cost is materially above the other target campaigns before downstream-quality interpretation.',
        'Review acquisition efficiency and measurement inputs separately from downstream fulfillment quality.',
        'This peer comparison is not profit, contribution, business ROAS, causal impact, or a budget instruction.',
        acquisition_sample_band, false
    from {{ ref('sama_pilot_campaign_diagnostics') }} as diagnostics
    where acquisition_sample_band in ('MEDIUM', 'HIGH')
      and cost_per_lightfunnels_order_usd >= peer_cost_per_lightfunnels_order_usd * 1.25
), candidates as (
    select * from business_candidates
    union all
    select * from campaign_candidates
), prioritized as (
    select
        *,
        case
            when signal_category = 'MEASUREMENT' then 'LOW'
            when confidence = 'LOW' then 'LOW'
            when coalesce(impact_order_count, 0) >= 100 then 'HIGH'
            when coalesce(impact_order_count, 0) >= 25 or coalesce(relative_gap, 0) >= 0.20
                then 'MEDIUM'
            else 'LOW'
        end as priority
    from candidates
)
select
    row_number() over (
        order by
            case priority when 'HIGH' then 1 when 'MEDIUM' then 2 else 3 end,
            impact_order_count desc nulls last,
            absolute_gap desc nulls last,
            signal_type,
            scope_type,
            scope_id
    ) as signal_order,
    signal_id,
    business_id,
    as_of_date,
    scope_type,
    scope_id,
    scope_name,
    signal_type,
    signal_category,
    priority,
    confidence,
    metric_name,
    observed_value,
    baseline_value,
    absolute_gap,
    relative_gap,
    sample_size,
    impact_order_count,
    evidence_summary,
    why_it_matters,
    recommended_next_step,
    limitation,
    causal_claim
from prioritized
