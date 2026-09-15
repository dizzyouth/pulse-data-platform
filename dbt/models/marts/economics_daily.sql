SELECT *
FROM {{ source('analytics', 'business_economics_daily') }}
