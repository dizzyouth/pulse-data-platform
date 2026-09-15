SELECT business_id, marketing_date, platform, campaign_id, marketing_currency,
       commerce_currency, campaign_spend, platform_conversion_value, platform_roas,
       attributed_orders, attributed_delivered_orders, attributed_delivered_value,
       attributed_cash_collected, attributed_contribution_before_marketing,
       delivered_value_roas, cash_roas, contribution_roas, currency_compatible
FROM marts.campaign_economics
ORDER BY marketing_date, business_id, platform, campaign_id;
