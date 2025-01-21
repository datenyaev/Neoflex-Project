CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    sstart_date DATE := i_OnDate - INTERVAL '1 month';
    eend_date DATE := i_OnDate - INTERVAL '1 day';
    inserted_rows INTEGER;
BEGIN
    -- Удаление записей за дату расчета
    DELETE FROM dm.dm_f101_round_f WHERE from_date = sstart_date AND to_date = eend_date;

    -- Вставка новых данных
    INSERT INTO dm.dm_f101_round_f (from_date, to_date, chapter, ledger_account, characteristic, 
                                    balance_in_rub, balance_in_val, balance_in_total,
                                    turn_deb_rub, turn_deb_val, turn_deb_total,
                                    turn_cre_rub, turn_cre_val, turn_cre_total,
                                    balance_out_rub, balance_out_val, balance_out_total)
    SELECT
        sstart_date,
        eend_date,
        l.chapter,
        LEFT(a.account_number, 5) AS ledger_account,
        a.char_type AS characteristic,
        COALESCE(SUM(CASE WHEN a.currency_rk IN (810, 643) THEN b_prev.balance_out_rub ELSE 0 END), 0) AS balance_in_rub,
        COALESCE(SUM(CASE WHEN a.currency_rk NOT IN (810, 643) THEN b_prev.balance_out_rub ELSE 0 END), 0) AS balance_in_val,
        COALESCE(SUM(b_prev.balance_out_rub), 0) AS balance_in_total,
        COALESCE(SUM(CASE WHEN a.currency_rk IN (810, 643) THEN t.debet_amount_rub ELSE 0 END), 0) AS turn_deb_rub,
        COALESCE(SUM(CASE WHEN a.currency_rk NOT IN (810, 643) THEN t.debet_amount_rub ELSE 0 END), 0) AS turn_deb_val,
        COALESCE(SUM(t.debet_amount_rub), 0) AS turn_deb_total,
        COALESCE(SUM(CASE WHEN a.currency_rk IN (810, 643) THEN t.credit_amount_rub ELSE 0 END), 0) AS turn_cre_rub,
        COALESCE(SUM(CASE WHEN a.currency_rk NOT IN (810, 643) THEN t.credit_amount_rub ELSE 0 END), 0) AS turn_cre_val,
        COALESCE(SUM(t.credit_amount_rub), 0) AS turn_cre_total,
        COALESCE(SUM(CASE WHEN a.currency_rk IN (810, 643) THEN b_last.balance_out_rub ELSE 0 END), 0) AS balance_out_rub,
        COALESCE(SUM(CASE WHEN a.currency_rk NOT IN (810, 643) THEN b_last.balance_out_rub ELSE 0 END), 0) AS balance_out_val,
        COALESCE(SUM(b_last.balance_out_rub), 0) AS balance_out_total
    FROM
        ds.md_account_d a
    LEFT JOIN
        dm.dm_account_balance_f b_prev ON b_prev.on_date = sstart_date - INTERVAL '1 day' AND b_prev.account_rk = a.account_rk
    LEFT JOIN
        dm.dm_account_balance_f b_last ON b_last.on_date = eend_date AND b_last.account_rk = a.account_rk
    LEFT JOIN
        dm.dm_account_turnover_f t ON t.on_date BETWEEN sstart_date AND eend_date AND t.account_rk = a.account_rk
    LEFT JOIN
        ds.md_ledger_account_s l ON l.ledger_account = CAST(LEFT(a.account_number, 5) AS INT)
    WHERE
        sstart_date BETWEEN a.data_actual_date AND a.data_actual_end_date
    GROUP BY
        l.chapter, LEFT(a.account_number, 5), a.char_type;

    GET DIAGNOSTICS inserted_rows = ROW_COUNT;

    -- Логирование завершения выполнения
    INSERT INTO logs.dm_log_table (process_name, start_time, end_time, description, rows_inserted)
    VALUES ('fill_f101_round_f', NOW() - INTERVAL '5 seconds' , NOW(), 'Завершен расчет витрины 101 формы за ' || sstart_date || ' - ' || eend_date, inserted_rows);
END;
$$;