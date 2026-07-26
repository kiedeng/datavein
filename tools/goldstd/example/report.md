# 血缘金标准抽检报告

- 生成时间:2026-07-26 14:32:58
- 口径:设计方案 13 章 —— 表级准确率 ≥99%、字段级 ≥95% 方可上线
- 参与比对用例(verified: true):13;未核对被忽略:0

## 汇总

| 指标 | 数值 | 阈值 | 结论 |
| --- | --- | --- | --- |
| 表级准确率(按用例,target+sources 全对) | 13/13 = 100.00% | ≥99% | 达标 |
| 字段级准确率(按边) | 54/56 = 96.43% | ≥95% | 达标 |

字段级明细:匹配 54 / 缺失边 1 / 多余边 1 / 表达式不符 0(分母 = 四者之和,即金标准边与机器边的并集口径)

**验收结论:通过**

## 用例明细

| 用例 | 机器状态 | 表级 | 金标准边数 | 匹配 | 缺失 | 多余 | 表达式不符 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 01_etl_dwd_apply_n1 | success | 对 | 6 | 6 | 0 | 0 | 0 |
| 02_etl_dwd_contract_n1 | success | 对 | 5 | 4 | 1 | 1 | 0 |
| 03_etl_dwd_repay_n1 | success | 对 | 5 | 5 | 0 | 0 | 0 |
| 04_etl_dws_cust_summary_n1 | success | 对 | 8 | 8 | 0 | 0 | 0 |
| 05_etl_dws_org_day_n1 | success | 对 | 3 | 3 | 0 | 0 | 0 |
| 06_etl_ads_report_n1 | success | 对 | 4 | 4 | 0 | 0 | 0 |
| 07_etl_ads_risk_tag_n1 | success | 对 | 2 | 2 | 0 | 0 | 0 |
| 08_etl_multi_insert_n1 | failed | 对 | 0 | 0 | 0 | 0 | 0 |
| 09_etl_tmp_chain_n1 | success | 对 | 5 | 5 | 0 | 0 | 0 |
| 10_etl_tmp_chain_n2 | success | 对 | 5 | 5 | 0 | 0 | 0 |
| 11_etl_view_org_credit_n1 | success | 对 | 3 | 3 | 0 | 0 | 0 |
| 12_etl_apply_snapshot_n1 | success | 对 | 7 | 7 | 0 | 0 | 0 |
| 13_etl_ads_cube_n1 | success | 对 | 2 | 2 | 0 | 0 | 0 |

## 错误明细

### 02_etl_dwd_contract_n1

- [缺失边] ods.ods_t_contract.start_date -> is_active(金标准有,机器未解析出)
- [多余边] ods.ods_t_contract.end_date -> is_active(机器解析出,金标准无)
