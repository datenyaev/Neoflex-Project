WITH account_balance_with_previous AS (
    SELECT
        ab1.account_rk,
        ab1.effective_date AS current_date,
        ab1.account_in_sum,
        ab1.account_out_sum,
        ab2.account_out_sum AS previous_account_out_sum,
        ab2.effective_date AS previous_date
    FROM rd.account_balance ab1
    LEFT JOIN rd.account_balance ab2 
        ON ab1.account_rk = ab2.account_rk
        AND ab1.effective_date = ab2.effective_date + INTERVAL '1 day'
)
UPDATE rd.account_balance ab
SET account_in_sum = COALESCE(
    (SELECT previous_account_out_sum 
     FROM account_balance_with_previous 
     WHERE account_balance_with_previous.account_rk = ab.account_rk
       AND account_balance_with_previous.current_date = ab.effective_date), 
    ab.account_in_sum
)
WHERE EXISTS (
    SELECT 1
    FROM account_balance_with_previous
    WHERE account_balance_with_previous.account_rk = ab.account_rk
      AND account_balance_with_previous.current_date = ab.effective_date
);
