truncate table dm.dm_account_balance_f;

INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
SELECT 
    b.on_date, 
    b.account_rk, 
    b.balance_out, 
    b.balance_out * COALESCE(er.reduced_cource, 1)
FROM 
    ds.ft_balance_f b
LEFT JOIN 
    ds.md_exchange_rate_d er
ON 
    b.currency_rk = er.currency_rk and b.on_date between er.data_actual_date and er.data_actual_end_date
WHERE 
    b.on_date = '2017-12-31';
	
DO $$
DECLARE
    day DATE := '2018-01-01';
BEGIN
    WHILE day <= '2018-01-31' LOOP
        CALL ds.fill_account_balance_f(day);
        day := day + INTERVAL '1 day';
    END LOOP;
END $$;