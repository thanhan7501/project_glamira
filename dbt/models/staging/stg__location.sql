WITH location AS (
	SELECT 
		*
	FROM `mimetic-slate-428113-q6.glamira_raw.location`
),
final AS (
  SELECT
    ip as country_key,
    country_short,
    country_long,
    region,
    city,
    latitude,
    longitude,
    zipcode,
    timezone
  FROM location
)

SELECT * FROM final