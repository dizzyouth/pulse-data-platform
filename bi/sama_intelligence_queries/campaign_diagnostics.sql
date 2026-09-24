select
    business_id,
    campaign_name,
    spend_usd,
    delivery_rate,
    peer_delivery_rate,
    return_rate,
    peer_return_rate,
    cost_per_delivered_order_usd,
    peer_cost_per_delivered_order_usd,
    delivery_benchmark_gap_orders,
    excess_returns_vs_peer,
    fulfillment_sample_band as sample_band
from marts.sama_pilot_campaign_diagnostics
order by delivery_benchmark_gap_orders desc, excess_returns_vs_peer desc, campaign_name
