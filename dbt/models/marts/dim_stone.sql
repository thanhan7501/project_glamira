WITH option AS (
  SELECT 
    *
  FROM {{ ref("stg__user_behaviour") }}
  WHERE LOWER(option_label) != "alloy"
),
final AS (
  SELECT 
    DISTINCT value_key AS stone_key,
    value_label AS stone_name,
  FROM option
)

SELECT * FROM final