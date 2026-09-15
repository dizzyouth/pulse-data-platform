SELECT *
FROM {{ source('analytics', 'attributed_campaign_economics') }}
