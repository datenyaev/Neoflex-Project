-- PROCEDURE: ds.fill_account_turnover_f(date)

-- DROP PROCEDURE IF EXISTS ds.fill_account_turnover_f(date);

CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(
	IN i_ondate date)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    inserted_rows INTEGER;
BEGIN
    -- Удаление записей за дату расчета
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;

    -- Вставка новых данных
    INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    SELECT 
        i_OnDate,
        a.account_rk,
        COALESCE(SUM(p.credit_amount), 0) AS credit_amount,
        COALESCE(SUM(p.credit_amount * COALESCE(er.reduced_cource, 1)), 0) AS credit_amount_rub,
        COALESCE(SUM(p.debet_amount), 0) AS debet_amount,
        COALESCE(SUM(p.debet_amount * COALESCE(er.reduced_cource, 1)), 0) AS debet_amount_rub
    FROM 
        ds.md_account_d a
    LEFT JOIN 
        ds.ft_posting_f p
        ON p.oper_date = i_OnDate AND p.credit_account_rk = a.account_rk
	LEFT JOIN 
        ds.ft_posting_f p
        ON p.oper_date = i_OnDate AND p.debet_account_rk = a.account_rk
    LEFT JOIN 
        ds.md_exchange_rate_d er
        ON a.currency_rk = er.currency_rk AND i_OnDate BETWEEN er.data_actual_date AND er.data_actual_end_date
    WHERE 
        i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date
    GROUP BY 
        i_OnDate, a.account_rk
    HAVING 
        SUM(p.credit_amount) > 0 OR SUM(p.debet_amount) > 0;

    GET DIAGNOSTICS inserted_rows = ROW_COUNT;

    -- Логирование завершения выполнения
    INSERT INTO logs.dm_log_table (process_name, start_time, end_time, description, rows_inserted)
    VALUES ('fill_account_turnover_f', NOW() - INTERVAL '5 seconds', NOW(), 'Завершен расчет витрины оборотов за ' || i_OnDate, inserted_rows);
END;
$BODY$;
ALTER PROCEDURE ds.fill_account_turnover_f(date)
    OWNER TO postgres;
