INSERT OVERWRITE TABLE ads.ads_apply_cube PARTITION (dt = '2026-07-25')
SELECT u.org_no, a.apply_status, COUNT(1) AS apply_cnt
FROM dwd.dwd_apply_detail a
JOIN ods.ods_t_cust u ON u.cust_no = a.cust_no AND u.dt = '2026-07-25'
WHERE a.dt = '2026-07-25'
GROUP BY u.org_no, a.apply_status
GROUPING SETS ((u.org_no, a.apply_status), (u.org_no), ())
