CREATE VIEW ads.v_org_credit AS
SELECT org_no, org_name, credit_amt
FROM ads.ads_credit_report
WHERE dt = '2026-07-25'
