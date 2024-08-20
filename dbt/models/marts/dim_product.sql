WITH product_name AS (
  SELECT 
    *
  FROM {{ ref("stg__product_name") }}
)

SELECT * FROM product_name