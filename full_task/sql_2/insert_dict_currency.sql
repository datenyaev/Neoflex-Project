INSERT INTO dm.dict_currency (currency_cd, currency_name, effective_from_date, effective_to_date)
SELECT CAST(lc.currency_cd AS TEXT), 
       lc.currency_name, 
       CAST(lc.effective_from_date AS DATE), 
       CAST(lc.effective_to_date AS DATE)
FROM load.dict_currency lc
WHERE NOT EXISTS (
    SELECT 1
    FROM dm.dict_currency dc
    WHERE dc.currency_cd = CAST(lc.currency_cd AS TEXT) 
    AND dc.effective_from_date = CAST(lc.effective_from_date AS DATE)
    AND dc.effective_to_date = CAST(lc.effective_to_date AS DATE)
);
