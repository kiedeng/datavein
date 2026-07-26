INSERT OVERWRITE TABLE tmp_credit.tmp_repay_dedup
SELECT repay_id, contract_no, repay_amt, repay_date, repay_type
FROM (
  SELECT repay_id, contract_no, repay_amt, repay_date, repay_type,
         row_number() OVER (PARTITION BY repay_id ORDER BY repay_date DESC) AS rn
  FROM ods.ods_t_repay WHERE dt = '2026-07-25'
) t WHERE rn = 1
