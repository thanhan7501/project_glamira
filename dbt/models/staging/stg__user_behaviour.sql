WITH user_behaviour AS (
	SELECT 
		order_id,
    store_id,
    time_stamp,
    ip,
    collection,
    cart_products_unnest.product_id AS cart_product_id,
    cart_products_unnest.amount AS cart_amount,
    cart_products_unnest.price AS cart_price,
    cart_products_unnest.currency AS cart_currency,
    cart_products_unnest.option AS cart_option
	FROM `mimetic-slate-428113-q6.glamira_raw.user_behaviour`,
  UNNEST(cart_products) AS cart_products_unnest
),
rename AS (
  SELECT
    CAST(REPLACE(CAST(order_id AS STRING),".0",'') AS INT64) AS order_key,
    store_id AS store_key,
    UNIX_DATE(DATE(TIMESTAMP_SECONDS(time_stamp))) AS order_date_key,
    cart_product_id AS product_key,
    ip AS country_key,
    CAST(cart_amount AS INT64) AS order_quantity,
    CASE 
      WHEN REGEXP_CONTAINS(cart_price, r'^[0-9]{1,3}(,[0-9]{3})*\.[0-9]{2}$') THEN SAFE_CAST(REPLACE(cart_price,',', '') AS FLOAT64)
    ELSE SAFE_CAST(REGEXP_REPLACE(REPLACE(REPLACE(cart_price,"'",''),'.','') , r"[٫,]", '.') AS FLOAT64) --invalid value is 1,88.00 and 1'88,00 60,00
      END AS unit_price,
    cart_currency AS currency,
    JSON_QUERY_ARRAY(cart_option) AS option,
    collection,
  FROM user_behaviour
),
unnest_option AS (
  SELECT
    order_key,
    store_key,
    order_date_key,
    product_key,
    country_key,
    currency,
    option_unnest AS option,
    collection,
    order_quantity,
    COALESCE(unit_price, 0) as unit_price,
    order_quantity * unit_price AS line_total
  FROM rename,
  UNNEST(option) AS option_unnest
),
final AS (
  SELECT
    order_key,
    store_key,
    order_date_key,
    product_key,
    country_key,
    currency,
    CAST(JSON_VALUE(option.option_id) AS INT64) AS option_key,
    JSON_VALUE(option.option_label) AS option_label,
    CAST(JSON_VALUE(option.value_id) AS INT64) AS value_key,
    JSON_VALUE(option.value_label) AS value_label,
    collection,
    order_quantity,
    unit_price,
    line_total
  FROM unnest_option
)

SELECT * FROM final