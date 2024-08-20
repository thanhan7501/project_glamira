WITH location AS (
  SELECT 
    *
  FROM {{ ref("stg__location") }}
)

SELECT * FROM location