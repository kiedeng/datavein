INSERT OVERWRITE TABLE dwd.dwd_apply_detail PARTITION (dt = '2026-07-25')
SELECT apply_id, cust_no, prod_cd, apply_amt, apply_status,
       to_date(apply_time) AS biz_date
FROM (
  SELECT apply_id, cust_no, prod_cd, apply_amt, apply_status, apply_time,
         row_number() OVER (PARTITION BY apply_id ORDER BY apply_time DESC) AS rn
  FROM ods.ods_t_apply
  WHERE dt = '2026-07-25'
) t
WHERE rn = 1
