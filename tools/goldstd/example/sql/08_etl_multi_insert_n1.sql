FROM dwd.dwd_repay_detail r
INSERT OVERWRITE TABLE dwd.dwd_repay_agg PARTITION (dt = '2026-07-25')
SELECT r.contract_no, SUM(r.repay_amt) WHERE r.dt = '2026-07-25' GROUP BY r.contract_no
INSERT OVERWRITE TABLE dwd.dwd_repay_prepay PARTITION (dt = '2026-07-25')
SELECT r.repay_id, r.contract_no, r.repay_amt WHERE r.dt = '2026-07-25' AND r.is_prepay = 1
