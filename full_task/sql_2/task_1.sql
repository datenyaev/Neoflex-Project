-- выводим дубли
WITH duplicate_clients AS (
    SELECT 
        client_rk, 
        effective_from_date, 
        COUNT(*) AS duplicate_count
    FROM 
        dm.client
    GROUP BY 
        client_rk, effective_from_date
    HAVING 
        COUNT(*) > 1
)
SELECT 
    c.client_rk, 
    c.effective_from_date, 
    c.client_id,
    c.counterparty_type_cd,
    c.black_list_flag,
    c.client_open_dttm,
    c.bankruptcy_rk
FROM 
    dm.client c
JOIN 
    duplicate_clients d 
    ON c.client_rk = d.client_rk 
    AND c.effective_from_date = d.effective_from_date;

-- удаляем дубли
WITH cte AS (
    SELECT 
        MIN(ctid) AS min_ctid
    FROM 
        dm.client
    GROUP BY 
        client_rk, effective_from_date
)
DELETE FROM 
    dm.client
WHERE 
    ctid NOT IN (SELECT min_ctid FROM cte);
