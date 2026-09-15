SELECT business_id, order_source_id, order_id, order_created_date,
       reporting_timezone, currency, economic_status,
       delivered_order_value, cash_expected, cash_collected,
       gross_collected, net_remitted,
       product_cogs, shipping_cost, cod_fee, fulfillment_fee,
       return_fee, variable_operational_cost,
       contribution_before_marketing, contribution_after_marketing,
       CASE WHEN gross_collected > net_remitted
            THEN gross_collected - net_remitted ELSE 0 END AS remittance_gap,
       CASE WHEN delivered AND net_remitted = 0 THEN 1 ELSE 0 END AS delivered_but_unremitted
FROM {{ source('analytics', 'order_economics') }}
WHERE payment_type = 'cod'
