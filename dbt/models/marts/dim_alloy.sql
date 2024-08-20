WITH option AS (
  SELECT 
    *
  FROM {{ ref("stg__user_behaviour") }}
  WHERE LOWER(option_label) = "alloy"
),
final AS (
  SELECT 
    DISTINCT value_key AS alloy_key, 
    value_label AS alloy_name,
  FROM option
)

SELECT * FROM final