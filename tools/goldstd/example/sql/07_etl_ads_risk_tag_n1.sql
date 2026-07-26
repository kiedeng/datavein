INSERT OVERWRITE TABLE ads.ads_cust_risk_tag PARTITION (dt = '2026-07-25')
SELECT cust_no, tag AS risk_tag
FROM ods.ods_t_cust
LATERAL VIEW explode(split(risk_tags, ',')) x AS tag
WHERE dt = '2026-07-25'
