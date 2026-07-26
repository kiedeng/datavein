INSERT OVERWRITE TABLE dws.dws_cust_credit_summary PARTITION (dt = '2026-07-25')
SELECT cust_no, org_no,
       SUM(credit_amt)  AS total_credit_amt,
       SUM(repay_amt)   AS total_repay_amt,
       SUM(cnt)         AS contract_cnt
FROM (
  SELECT cust_no, org_no, total_credit_amt AS credit_amt,
         total_repay_amt AS repay_amt, contract_cnt AS cnt
  FROM dws.dws_cust_credit_summary
  WHERE dt = '2026-07-24'
  UNION ALL
  SELECT c.cust_no, u.org_no, a.apply_amt, 0, 1
  FROM dwd.dwd_contract_detail c
  JOIN dwd.dwd_apply_detail a ON c.apply_id = a.apply_id AND a.dt = '2026-07-25'
  JOIN ods.ods_t_cust u ON u.cust_no = c.cust_no AND u.dt = '2026-07-25'
  WHERE c.dt = '2026-07-25'
) m
GROUP BY cust_no, org_no
