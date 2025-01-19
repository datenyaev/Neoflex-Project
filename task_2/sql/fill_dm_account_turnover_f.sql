DO $$
DECLARE
    day DATE := '2018-01-01';
BEGIN
    WHILE day <= '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(day);
        day := day + INTERVAL '1 day';
    END LOOP;
END $$;