select
    business_id,
    coalesce(currency, 'UNSPECIFIED') as currency,
    economic_status,
    count(*) filter (where delivered) as delivered_orders,
    sum(cash_collected) as collected_native_currency,
    sum(cod_fee_native) as cod_fee_native_currency,
    sum(cash_collected - cod_fee_native) as cash_after_cod_fee_native_currency,
    sum(product_cogs_usd) as product_cogs_usd,
    sum(call_center_cost_usd) as call_center_cost_usd,
    sum(logistics_cost_usd) as logistics_cost_usd,
    sum(known_operational_cost_usd) as known_operational_cost_usd
from {{ source('analytics', 'sama_pilot_order_facts') }}
where lower(trim(utm_source)) = 'tiktok'
  and match_status = 'MATCHED_HIGH_CONFIDENCE'
group by business_id, coalesce(currency, 'UNSPECIFIED'), economic_status
