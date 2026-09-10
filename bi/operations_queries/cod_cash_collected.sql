SELECT business_id, collection_date, provider, currency, collection_count,
       cash_expected, cash_collected, cash_collection_rate
FROM marts.cod_performance
ORDER BY collection_date
