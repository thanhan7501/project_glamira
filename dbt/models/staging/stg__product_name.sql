WITH product AS (
	SELECT 
		*
	FROM `mimetic-slate-428113-q6.glamira_raw.product_name`
),
final AS (
	SELECT
		product_id AS product_key,
    product_name
	FROM product
)

SELECT * FROM final