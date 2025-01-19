CREATE OR REPLACE FUNCTION ds.upsert_table(table_name text, temp_table text)
RETURNS integer AS $$
DECLARE
    rows_affected integer;
BEGIN
    -- Удаление данных для таблицы FT_POSTING_F перед загрузкой
    IF table_name = 'ft_posting_f' THEN
        EXECUTE 'DELETE FROM ds.ft_posting_f';
        -- Выполнение UPSERT для таблицы FT_POSTING_F
        EXECUTE format(
            'WITH upsert AS (
                INSERT INTO ds.%I ("oper_date", "credit_account_rk", "debet_account_rk", "credit_amount", "debet_amount")
                SELECT 
                    CAST("OPER_DATE" AS DATE) AS "OPER_DATE",
                    "CREDIT_ACCOUNT_RK",
                    "DEBET_ACCOUNT_RK",
                    "CREDIT_AMOUNT",
                    "DEBET_AMOUNT"
                FROM ds.%I
                RETURNING 1
            )
            SELECT COUNT(*) FROM upsert;',
            table_name, temp_table
        ) INTO rows_affected;
    ELSIF table_name = 'ft_balance_f' THEN
        -- UPSERT для таблицы FT_BALANCE_F
        EXECUTE format(
            'WITH upsert AS (
                INSERT INTO ds.%I ("on_date", "account_rk", "currency_rk", "balance_out")
                SELECT 
                    CAST("ON_DATE" AS DATE) AS "ON_DATE",
                    "ACCOUNT_RK",
                    "CURRENCY_RK",
                    "BALANCE_OUT"
                FROM ds.%I
                ON CONFLICT ("on_date", "account_rk") DO UPDATE
                SET "balance_out" = EXCLUDED."balance_out"
                RETURNING 1
            )
            SELECT COUNT(*) FROM upsert;',
            table_name, temp_table
        ) INTO rows_affected;
    ELSIF table_name = 'md_account_d' THEN
        -- UPSERT для таблицы MD_ACCOUNT_D
        EXECUTE format(
            'WITH upsert AS (
                INSERT INTO ds.%I ("data_actual_date", "data_actual_end_date", "account_rk", "account_number", "char_type", "currency_rk", "currency_code")
                SELECT 
                    CAST("DATA_ACTUAL_DATE" AS DATE),
                    CAST("DATA_ACTUAL_END_DATE" AS DATE),
                    "ACCOUNT_RK",
                    "ACCOUNT_NUMBER",
                    "CHAR_TYPE",
                    "CURRENCY_RK",
                    "CURRENCY_CODE"
                FROM ds.%I
                ON CONFLICT ("data_actual_date", "account_rk") DO UPDATE
                SET "account_number" = EXCLUDED."account_number",
                    "char_type" = EXCLUDED."char_type",
                    "currency_rk" = EXCLUDED."currency_rk",
                    "currency_code" = EXCLUDED."currency_code"
                RETURNING 1
            )
            SELECT COUNT(*) FROM upsert;',
            table_name, temp_table
        ) INTO rows_affected;
    ELSIF table_name = 'md_currency_d' THEN
        -- UPSERT для таблицы MD_CURRENCY_D
        EXECUTE format(
            'WITH upsert AS (
                INSERT INTO ds.%I ("currency_rk", "data_actual_date", "data_actual_end_date", "currency_code", "code_iso_char")
                SELECT 
                    "CURRENCY_RK",
                    CAST("DATA_ACTUAL_DATE" AS DATE),
                    CAST("DATA_ACTUAL_END_DATE" AS DATE),
                    "CURRENCY_CODE",
                    "CODE_ISO_CHAR"
                FROM ds.%I
                ON CONFLICT ("currency_rk", "data_actual_date") DO UPDATE
                SET "currency_code" = EXCLUDED."currency_code",
                    "code_iso_char" = EXCLUDED."code_iso_char"
                RETURNING 1
            )
            SELECT COUNT(*) FROM upsert;',
            table_name, temp_table
        ) INTO rows_affected;
    ELSIF table_name = 'md_exchange_rate_d' THEN
        -- UPSERT для таблицы MD_EXCHANGE_RATE_D
        EXECUTE format(
            'WITH upsert AS (
                INSERT INTO ds.%I ("data_actual_date", "data_actual_end_date", "currency_rk", "reduced_cource", "code_iso_num")
                SELECT DISTINCT
                    CAST("DATA_ACTUAL_DATE" AS DATE),
                    CAST("DATA_ACTUAL_END_DATE" AS DATE),
                    "CURRENCY_RK",
                    "REDUCED_COURCE",
                    "CODE_ISO_NUM"
                FROM ds.%I
				ON CONFLICT ("data_actual_date", "currency_rk") DO UPDATE
    			SET "reduced_cource" = EXCLUDED."reduced_cource",
        			"code_iso_num" = EXCLUDED."code_iso_num"
                RETURNING 1
            )
            SELECT COUNT(*) FROM upsert;',
            table_name, temp_table
        ) INTO rows_affected;
    ELSIF table_name = 'md_ledger_account_s' THEN
        -- UPSERT для таблицы MD_LEDGER_ACCOUNT_S
        EXECUTE format(
            'WITH upsert AS (
                INSERT INTO ds.%I ("chapter", "chapter_name", "section_number", "section_name", "subsection_name", "ledger1_account", "ledger1_account_name", "ledger_account", "ledger_account_name", "characteristic", "start_date", "end_date")
                SELECT 
                    "CHAPTER",
                    "CHAPTER_NAME",
                    "SECTION_NUMBER",
                    "SECTION_NAME",
                    "SUBSECTION_NAME",
                    "LEDGER1_ACCOUNT",
                    "LEDGER1_ACCOUNT_NAME",
                    "LEDGER_ACCOUNT",
                    "LEDGER_ACCOUNT_NAME",
                    "CHARACTERISTIC",
                    --"IS_RESIDENT",
                    --"IS_RESERVE",
                    --"IS_RESERVED",
                    --"IS_LOAN",
                    --"IS_RESERVED_ASSETS",
                    --"IS_OVERDUE",
                    --"IS_INTEREST",
                    --"PAIR_ACCOUNT",
                    CAST("START_DATE" AS DATE),
                    CAST("END_DATE" AS DATE)
                    --"IS_RUB_ONLY",
                    --"MIN_TERM",
                    --"MIN_TERM_MEASURE",
                    --"MAX_TERM",
                    --"MAX_TERM_MEASURE",
                    --"LEDGER_ACC_FULL_NAME_TRANSLIT",
                    --"IS_REVALUATION",
                    --"IS_CORRECT"
                FROM ds.%I
                ON CONFLICT ("ledger_account", "start_date") DO UPDATE
                SET "chapter" = EXCLUDED."chapter",
			    "chapter_name" = EXCLUDED."chapter_name",
			    "section_number" = EXCLUDED."section_number",
			    "section_name" = EXCLUDED."section_name",
			    "subsection_name" = EXCLUDED."subsection_name",
			    "ledger1_account" = EXCLUDED."ledger1_account",
			    "ledger1_account_name" = EXCLUDED."ledger1_account_name",
			    "ledger_account_name" = EXCLUDED."ledger_account_name",
			    "characteristic" = EXCLUDED."characteristic",
			    "end_date" = EXCLUDED."end_date"
			   

                RETURNING 1
            )
            SELECT COUNT(*) FROM upsert;',
            table_name, temp_table
        ) INTO rows_affected;
    ELSE
        RAISE EXCEPTION 'Unsupported table: %', table_name;
    END IF;

    RETURN rows_affected;
END;
$$ LANGUAGE plpgsql;
