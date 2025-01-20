INSERT INTO rd.deal_info
SELECT 
	deal_rk, 
	deal_num, 
	deal_name, 
	deal_sum, 
	client_rk, 
	account_rk, 
	agreement_rk, 
	CAST(deal_start_date AS DATE), 
	department_rk, 
	product_rk, 
	deal_type_cd, 
	CAST(effective_from_date AS DATE), 
	CAST(effective_to_date AS DATE)
FROM load.deal_info
WHERE CAST(effective_from_date AS DATE) NOT IN (
    SELECT distinct effective_from_date
    FROM rd.deal_info
);
