-- PROCEDURE: ds.fill_account_balance_f(date)

-- DROP PROCEDURE IF EXISTS ds.fill_account_balance_f(date);

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(
	IN i_ondate date)
LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    prev_date DATE := i_OnDate - INTERVAL '1 day';
    inserted_rows INTEGER;
BEGIN
    -- Удаление записей за дату расчета
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

    -- Вставка новых данных
    INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT 
        i_OnDate, 
        a.account_rk,
        CASE a.char_type
            WHEN 'Рђ' THEN COALESCE(b.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            WHEN 'Рџ' THEN COALESCE(b.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
        END AS balance_out,
        CASE a.char_type
            WHEN 'Рђ' THEN COALESCE(b.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
            WHEN 'Рџ' THEN COALESCE(b.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM 
        ds.md_account_d a
    LEFT JOIN 
        dm.dm_account_balance_f b
    ON 
        b.on_date = prev_date AND b.account_rk = a.account_rk
    LEFT JOIN 
        dm.dm_account_turnover_f t
    ON 
        t.on_date = i_OnDate AND t.account_rk = a.account_rk
    WHERE 
        i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date;

    GET DIAGNOSTICS inserted_rows = ROW_COUNT;

    -- Логирование завершения выполнения
    INSERT INTO logs.dm_log_table (process_name, start_time, end_time, description, rows_inserted)
    VALUES ('fill_account_balance_f', NOW() - INTERVAL '5 seconds', NOW(), 'Завершен расчет витрины остатков за ' || i_OnDate, inserted_rows);
END;
$BODY$;
ALTER PROCEDURE ds.fill_account_balance_f(date)
    OWNER TO postgres;
