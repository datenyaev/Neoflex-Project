INSERT INTO rd.product
SELECT 
	product_rk, 
	product_name, 
	CAST(effective_from_date AS DATE), 
	CAST(effective_to_date AS DATE)
FROM load.product_info
WHERE CAST(effective_from_date AS DATE) NOT IN (
    SELECT distinct effective_from_date
    FROM rd.product
);
