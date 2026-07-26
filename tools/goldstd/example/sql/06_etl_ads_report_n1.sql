INSERT OVERWRITE TABLE ads.ads_credit_report PARTITION (dt = '2026-07-25')
SELECT org_no, org_name, credit_amt,
       RANK() OVER (ORDER BY credit_amt DESC) AS credit_rank
FROM dws.dws_org_credit_day
WHERE dt = '2026-07-25'
