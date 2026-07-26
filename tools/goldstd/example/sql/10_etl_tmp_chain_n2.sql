INSERT OVERWRITE TABLE dwd.dwd_repay_detail PARTITION (dt = '2026-07-25')
SELECT repay_id, contract_no, repay_amt, repay_date,
       CASE WHEN repay_type = 'PRE' THEN 1 ELSE 0 END AS is_prepay
FROM tmp_credit.tmp_repay_dedup
