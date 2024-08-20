WITH user_order AS (
  SELECT 
    *
  FROM {{ ref("stg__user_behaviour") }}
  WHERE collection = "checkout_success"
),
order_with_alloy AS (
  SELECT
    *
  FROM user_order
  WHERE LOWER(option_label) = "alloy"
),
order_with_other AS (
  SELECT
    *
  FROM user_order
  WHERE LOWER(option_label) != "alloy"
),
final AS (
  SELECT
    owa.order_key,
    owa.store_key,
    owa.order_date_key,
    owa.product_key,
    owa.country_key,
    COALESCE(owa.value_key, 0) AS alloy_key,
    COALESCE(owo.value_key, 0) AS stone_key,
    owa.order_quantity,
    owa.unit_price,
    owa.currency,
    owa.line_total,
  FROM order_with_alloy AS owa
  LEFT JOIN order_with_other AS owo ON owa.order_key = owo.order_key 
  AND owa.product_key = owo.product_key
)

SELECT 
  *
FROM final