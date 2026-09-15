SELECT *
FROM {{ source('analytics', 'order_economics') }}
