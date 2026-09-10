SELECT business_id, collection_date, provider, currency, collection_count,
       cash_expected, cash_collected, cash_collection_rate
FROM {{ source('analytics', 'cod_collection_performance') }}
