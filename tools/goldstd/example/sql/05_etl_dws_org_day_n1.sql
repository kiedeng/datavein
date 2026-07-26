INSERT OVERWRITE TABLE dws.dws_org_credit_day PARTITION (dt = '2026-07-25')
SELECT /*+ BROADCAST(o) */
       u.org_no, o.org_name,
       SUM(a.apply_amt) AS credit_amt,
       COUNT(1)         AS apply_cnt
FROM dwd.dwd_apply_detail a
JOIN ods.ods_t_cust u ON u.cust_no = a.cust_no AND u.dt = '2026-07-25'
JOIN dim.dim_org o    ON o.org_no = u.org_no
WHERE a.dt = '2026-07-25' AND a.apply_status = 'APPROVED'
GROUP BY u.org_no, o.org_name
DISTRIBUTE BY org_no
