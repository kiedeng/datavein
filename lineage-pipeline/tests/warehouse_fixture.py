"""模拟信贷数仓:表结构 + 真实风格 Hive/Spark ETL SQL 语料库。

用途:① 解析核心语料库测试(金标准用例集,13 章);② E2E 集成测试的种子数据。
SQL 均为渲染后版本(制度要求动态拼接归档渲染结果,4.2),分区列恒排 schema 最后
(与 metadata_sync 的快照顺序一致)。
"""

# {db: {table: {col: type}}},列序即物理序
SCHEMA = {
    "ods": {
        "ods_t_apply": {"apply_id": "bigint", "cust_no": "string", "prod_cd": "string",
                        "apply_amt": "decimal(18,2)", "apply_status": "string",
                        "apply_time": "string", "dt": "string"},
        "ods_t_contract": {"contract_no": "string", "apply_id": "bigint",
                           "cust_no": "string", "contract_amt": "decimal(18,2)",
                           "start_date": "string", "end_date": "string", "dt": "string"},
        "ods_t_repay": {"repay_id": "bigint", "contract_no": "string",
                        "repay_amt": "decimal(18,2)", "repay_date": "string",
                        "repay_type": "string", "dt": "string"},
        "ods_t_cust": {"cust_no": "string", "cust_name": "string", "id_card": "string",
                       "mobile": "string", "org_no": "string", "risk_tags": "string",
                       "dt": "string"},
    },
    "dim": {
        "dim_org": {"org_no": "string", "org_name": "string", "parent_org_no": "string"},
    },
    "dwd": {
        "dwd_apply_detail": {"apply_id": "bigint", "cust_no": "string", "prod_cd": "string",
                             "apply_amt": "decimal(18,2)", "apply_status": "string",
                             "apply_date": "string", "dt": "string"},
        "dwd_apply_snapshot": {"apply_id": "bigint", "cust_no": "string", "prod_cd": "string",
                               "apply_amt": "decimal(18,2)", "apply_status": "string",
                               "apply_time": "string", "dt": "string"},
        "dwd_contract_detail": {"contract_no": "string", "apply_id": "bigint",
                                "cust_no": "string", "contract_amt": "decimal(18,2)",
                                "is_active": "tinyint", "dt": "string"},
        "dwd_repay_detail": {"repay_id": "bigint", "contract_no": "string",
                             "repay_amt": "decimal(18,2)", "repay_date": "string",
                             "is_prepay": "tinyint", "dt": "string"},
    },
    "tmp_credit": {
        "tmp_repay_dedup": {"repay_id": "bigint", "contract_no": "string",
                            "repay_amt": "decimal(18,2)", "repay_date": "string",
                            "repay_type": "string"},
    },
    "dws": {
        "dws_cust_credit_summary": {"cust_no": "string", "org_no": "string",
                                    "total_credit_amt": "decimal(18,2)",
                                    "total_repay_amt": "decimal(18,2)",
                                    "contract_cnt": "bigint", "dt": "string"},
        "dws_org_credit_day": {"org_no": "string", "org_name": "string",
                               "credit_amt": "decimal(18,2)", "apply_cnt": "bigint",
                               "dt": "string"},
    },
    "ads": {
        "ads_credit_report": {"org_no": "string", "org_name": "string",
                              "credit_amt": "decimal(18,2)", "credit_rank": "bigint",
                              "dt": "string"},
        "ads_cust_risk_tag": {"cust_no": "string", "risk_tag": "string", "dt": "string"},
        "ads_apply_cube": {"org_no": "string", "apply_status": "string",
                           "apply_cnt": "bigint", "dt": "string"},
    },
}

# 语料库:每条为一个调度任务 SQL 节点
# col_checks: (目标列, 期望来源表, 期望来源列, 表达式应包含的子串或None)
CORPUS = [
    dict(  # T1 窗口去重 + CTE 子查询 + 静态分区(SELECT 别名与目标列名不同,验证位置映射)
        task_id="etl_dwd_apply", node_seq=1, dialect="hive",
        target="dwd.dwd_apply_detail",
        sources={"ods.ods_t_apply"}, status="success",
        col_checks=[("apply_date", "ods.ods_t_apply", "apply_time", "TO_DATE")],
        sql="""
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
"""),
    dict(  # T2 双表 JOIN + CASE WHEN + 静态分区
        task_id="etl_dwd_contract", node_seq=1, dialect="hive",
        target="dwd.dwd_contract_detail",
        sources={"ods.ods_t_contract", "dwd.dwd_apply_detail"}, status="success",
        col_checks=[("is_active", "ods.ods_t_contract", "end_date", "CASE")],
        sql="""
INSERT OVERWRITE TABLE dwd.dwd_contract_detail PARTITION (dt = '2026-07-25')
SELECT c.contract_no, c.apply_id, c.cust_no, c.contract_amt,
       CASE WHEN c.end_date >= '2026-07-25' THEN 1 ELSE 0 END AS is_active
FROM ods.ods_t_contract c
JOIN dwd.dwd_apply_detail a
  ON c.apply_id = a.apply_id AND a.dt = '2026-07-25'
WHERE c.dt = '2026-07-25'
"""),
    dict(  # T3 简单清洗(与 T9b 构成"多任务写同一目标表")
        task_id="etl_dwd_repay", node_seq=1, dialect="hive",
        target="dwd.dwd_repay_detail",
        sources={"ods.ods_t_repay"}, status="success",
        col_checks=[("is_prepay", "ods.ods_t_repay", "repay_type", "CASE")],
        sql="""
INSERT OVERWRITE TABLE dwd.dwd_repay_detail PARTITION (dt = '2026-07-25')
SELECT repay_id, contract_no, repay_amt, repay_date,
       CASE WHEN repay_type = 'PRE' THEN 1 ELSE 0 END AS is_prepay
FROM ods.ods_t_repay
WHERE dt = '2026-07-25'
"""),
    dict(  # T4 自依赖增量滚动 + UNION ALL 穿透 + 多表 JOIN
        task_id="etl_dws_cust_summary", node_seq=1, dialect="hive",
        target="dws.dws_cust_credit_summary",
        sources={"dws.dws_cust_credit_summary", "dwd.dwd_contract_detail",
                 "dwd.dwd_apply_detail", "ods.ods_t_cust"},
        status="success", self_loop_src="dws.dws_cust_credit_summary",
        col_checks=[("total_credit_amt", "dwd.dwd_apply_detail", "apply_amt", "SUM"),
                    ("total_credit_amt", "dws.dws_cust_credit_summary",
                     "total_credit_amt", None)],
        sql="""
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
"""),
    dict(  # T5 Spark:BROADCAST hint + 聚合 + DISTRIBUTE BY
        task_id="etl_dws_org_day", node_seq=1, dialect="spark",
        target="dws.dws_org_credit_day",
        sources={"dwd.dwd_apply_detail", "ods.ods_t_cust", "dim.dim_org"},
        status="success",
        col_checks=[("credit_amt", "dwd.dwd_apply_detail", "apply_amt", "SUM")],
        sql="""
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
"""),
    dict(  # T6 Spark:窗口函数 RANK
        task_id="etl_ads_report", node_seq=1, dialect="spark",
        target="ads.ads_credit_report",
        sources={"dws.dws_org_credit_day"}, status="success",
        col_checks=[("credit_rank", "dws.dws_org_credit_day", "credit_amt", "RANK")],
        sql="""
INSERT OVERWRITE TABLE ads.ads_credit_report PARTITION (dt = '2026-07-25')
SELECT org_no, org_name, credit_amt,
       RANK() OVER (ORDER BY credit_amt DESC) AS credit_rank
FROM dws.dws_org_credit_day
WHERE dt = '2026-07-25'
"""),
    dict(  # T7 Hive:LATERAL VIEW explode(行转列)
        task_id="etl_ads_risk_tag", node_seq=1, dialect="hive",
        target="ads.ads_cust_risk_tag",
        sources={"ods.ods_t_cust"}, status="success",
        col_checks=[("risk_tag", "ods.ods_t_cust", "risk_tags", None)],
        sql="""
INSERT OVERWRITE TABLE ads.ads_cust_risk_tag PARTITION (dt = '2026-07-25')
SELECT cust_no, tag AS risk_tag
FROM ods.ods_t_cust
LATERAL VIEW explode(split(risk_tags, ',')) x AS tag
WHERE dt = '2026-07-25'
"""),
    dict(  # T8 Hive 多表插入:sqlglot 不支持,应失败并带专用原因码(登记规范要求拆分)
        task_id="etl_multi_insert", node_seq=1, dialect="hive",
        target=None, sources=set(), status="failed",
        reason="multi_insert_unsupported",
        sql="""
FROM dwd.dwd_repay_detail r
INSERT OVERWRITE TABLE dwd.dwd_repay_agg PARTITION (dt = '2026-07-25')
SELECT r.contract_no, SUM(r.repay_amt) WHERE r.dt = '2026-07-25' GROUP BY r.contract_no
INSERT OVERWRITE TABLE dwd.dwd_repay_prepay PARTITION (dt = '2026-07-25')
SELECT r.repay_id, r.contract_no, r.repay_amt WHERE r.dt = '2026-07-25' AND r.is_prepay = 1
"""),
    dict(  # T9a 同任务临时表:第一段写 tmp
        task_id="etl_tmp_chain", node_seq=1, dialect="hive",
        target="tmp_credit.tmp_repay_dedup",
        sources={"ods.ods_t_repay"}, status="success", col_checks=[],
        sql="""
INSERT OVERWRITE TABLE tmp_credit.tmp_repay_dedup
SELECT repay_id, contract_no, repay_amt, repay_date, repay_type
FROM (
  SELECT repay_id, contract_no, repay_amt, repay_date, repay_type,
         row_number() OVER (PARTITION BY repay_id ORDER BY repay_date DESC) AS rn
  FROM ods.ods_t_repay WHERE dt = '2026-07-25'
) t WHERE rn = 1
"""),
    dict(  # T9b 第二段 tmp→正式表(与 T3 构成多任务写同表,边按 sql_id 共存)
        task_id="etl_tmp_chain", node_seq=2, dialect="hive",
        target="dwd.dwd_repay_detail",
        sources={"tmp_credit.tmp_repay_dedup"}, status="success",
        col_checks=[("is_prepay", "tmp_credit.tmp_repay_dedup", "repay_type", "CASE")],
        sql="""
INSERT OVERWRITE TABLE dwd.dwd_repay_detail PARTITION (dt = '2026-07-25')
SELECT repay_id, contract_no, repay_amt, repay_date,
       CASE WHEN repay_type = 'PRE' THEN 1 ELSE 0 END AS is_prepay
FROM tmp_credit.tmp_repay_dedup
"""),
    dict(  # T10 视图定义与 ETL 同流程解析(5.3)
        task_id="etl_view_org_credit", node_seq=1, dialect="hive",
        target="ads.v_org_credit",
        sources={"ads.ads_credit_report"}, status="success", col_checks=[],
        sql="""
CREATE VIEW ads.v_org_credit AS
SELECT org_no, org_name, credit_amt
FROM ads.ads_credit_report
WHERE dt = '2026-07-25'
"""),
    dict(  # T11 select * 有 schema 快照:全列展开
        task_id="etl_apply_snapshot", node_seq=1, dialect="hive",
        target="dwd.dwd_apply_snapshot",
        sources={"ods.ods_t_apply"}, status="success",
        expect_col_edges=7,
        col_checks=[("apply_amt", "ods.ods_t_apply", "apply_amt", None)],
        sql="""
INSERT OVERWRITE TABLE dwd.dwd_apply_snapshot
SELECT * FROM ods.ods_t_apply WHERE dt = '2026-07-25'
"""),
    dict(  # T12 Spark:GROUPING SETS 多维立方
        task_id="etl_ads_cube", node_seq=1, dialect="spark",
        target="ads.ads_apply_cube",
        sources={"dwd.dwd_apply_detail", "ods.ods_t_cust"}, status="success",
        col_checks=[("org_no", "ods.ods_t_cust", "org_no", None)],
        sql="""
INSERT OVERWRITE TABLE ads.ads_apply_cube PARTITION (dt = '2026-07-25')
SELECT u.org_no, a.apply_status, COUNT(1) AS apply_cnt
FROM dwd.dwd_apply_detail a
JOIN ods.ods_t_cust u ON u.cust_no = a.cust_no AND u.dt = '2026-07-25'
WHERE a.dt = '2026-07-25'
GROUP BY u.org_no, a.apply_status
GROUPING SETS ((u.org_no, a.apply_status), (u.org_no), ())
"""),
]
