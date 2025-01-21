WITH account_balance_with_next AS (
        SELECT
            ab1.account_rk,
            ab1.effective_date AS current_date,
            ab1.account_in_sum,
            ab1.account_out_sum,
            ab2.account_in_sum AS next_account_in_sum,
            ab2.effective_date AS next_date
        FROM rd.account_balance ab1
        LEFT JOIN rd.account_balance ab2 
            ON ab1.account_rk = ab2.account_rk
            AND ab1.effective_date = ab2.effective_date - INTERVAL '1 day'
    )
    UPDATE rd.account_balance ab
    SET account_out_sum = COALESCE(
        (SELECT next_account_in_sum 
         FROM account_balance_with_next 
         WHERE account_balance_with_next.account_rk = ab.account_rk
           AND account_balance_with_next.current_date = ab.effective_date), 
        ab.account_out_sum
    )
    WHERE EXISTS (
        SELECT 1
        FROM account_balance_with_next
        WHERE account_balance_with_next.account_rk = ab.account_rk
          AND account_balance_with_next.current_date = ab.effective_date
    );