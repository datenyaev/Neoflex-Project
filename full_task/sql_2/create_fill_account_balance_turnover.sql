CREATE OR REPLACE PROCEDURE dm.fill_account_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Очистка витрины
    TRUNCATE TABLE dm.account_balance_turnover;

    -- Перезагрузка данных с корректировками по account_in_sum и account_out_sum
    INSERT INTO dm.account_balance_turnover (
        account_rk,
        currency_name,
        department_rk,
        effective_date,
        account_in_sum,
        account_out_sum
    )
    SELECT a.account_rk,
           COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
           a.department_rk,
           ab.effective_date,
           -- Корректировка account_in_sum на основе предыдущего дня
           CASE 
               WHEN ab.account_in_sum <> ab_previous.account_out_sum THEN ab_previous.account_out_sum 
               ELSE ab.account_in_sum 
           END AS account_in_sum,
           -- Корректировка account_out_sum на основе следующего дня
           CASE 
               WHEN ab.account_in_sum <> ab_next.account_in_sum THEN ab_next.account_in_sum
               ELSE ab.account_out_sum 
           END AS account_out_sum
    FROM rd.account a
    LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
    LEFT JOIN rd.account_balance ab_previous ON a.account_rk = ab_previous.account_rk
        AND ab.effective_date = ab_previous.effective_date + INTERVAL '1 day'
    LEFT JOIN rd.account_balance ab_next ON a.account_rk = ab_next.account_rk
        AND ab.effective_date = ab_next.effective_date - INTERVAL '1 day'
    LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
END;
$$;
