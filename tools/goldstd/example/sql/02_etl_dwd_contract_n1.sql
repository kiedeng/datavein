INSERT OVERWRITE TABLE dwd.dwd_contract_detail PARTITION (dt = '2026-07-25')
SELECT c.contract_no, c.apply_id, c.cust_no, c.contract_amt,
       CASE WHEN c.end_date >= '2026-07-25' THEN 1 ELSE 0 END AS is_active
FROM ods.ods_t_contract c
JOIN dwd.dwd_apply_detail a
  ON c.apply_id = a.apply_id AND a.dt = '2026-07-25'
WHERE c.dt = '2026-07-25'
