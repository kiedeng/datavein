# goldstd — 血缘金标准抽检辅助工具

服务于设计方案 **13 章「测试与验收方案」** 的血缘准确性验收:

> 抽样 100 段代表性 SQL(覆盖 CTE/临时表/自依赖/多写/视图/UDF)人工标注金标准,
> 解析结果对比,**表级准确率 ≥99%、字段级 ≥95% 方可上线**;上线后每月抽检 20 段。

人工标注最贵,所以流程是 **机器预填 → 人工只核对改错 → 自动比对出报告**,
把"从零手写 100 份标注"压缩成"核对 100 份预填 YAML"。

## 三步工作流

```bash
# 第 0 步(可选):从 sql_repository 分层抽样,导出 .sql 目录 + dialects.json
python tools/goldstd/sample.py --out work/sql --total 100 --seed 42 \
    [--host 127.0.0.1 --user datavein --password test_pw --db datavein]
# 无库环境:手工准备目录(每段一个 .sql,文件名即用例 id)即可,可选放 dialects.json

# 第 1 步:机器预填标注(schema 快照格式同 pipeline:{db:{table:{col:type}}})
python tools/goldstd/extract.py work/sql --schema work/schema.json --out work/annotations

# 第 2 步:人工核对 work/annotations/*.yaml
#   确认无误 → 把 verified 改为 true;有错 → 直接改数据后同样置 true

# 第 3 步:自动比对出报告(只统计 verified: true 的用例)
python tools/goldstd/compare.py work/annotations --sql-dir work/sql \
    --schema work/schema.json --report work/report.md
echo $?   # 0=达标 1=表级<99% 或字段级<95%(可直接接 CI) 2=输入错误
```

## 标注文件格式(一 SQL 一 YAML,extract 预填)

```yaml
case_id: 02_etl_dwd_contract_n1
sql_file: 02_etl_dwd_contract_n1.sql   # 相对 --sql-dir
dialect: hive                          # 来自 dialects.json 或 --dialect
machine_status: success                # 机器解析状态快照,仅参考,不参与比对
machine_reason: ''
verified: false                        # 人工核对完成后置 true,compare 才统计
notes: ''                              # 人工备注(不参与比对)
target_table: dwd.dwd_contract_detail  # 金标准目标表;null = 人工确认不可解析
table_sources:                         # 金标准物理源表集合
- dwd.dwd_apply_detail
- ods.ods_t_contract
column_edges:                          # 金标准列级边;边身份 = dst_col+src_table+src_col
- dst_col: is_active
  src_table: ods.ods_t_contract
  src_col: end_date
  expr: CASE WHEN c.end_date >= ...    # 表达式摘要;留空 = 跳过表达式比对
```

## 验收口径(与 13 章对齐)

- **表级准确率(按用例)**:`target_table` + `table_sources` 集合完全一致才算该用例正确;
  阈值 ≥99%(`--table-threshold` 可调)。
- **字段级准确率(按边)**:边身份 `(dst_col, src_table, src_col)` 逐条比对,
  `accuracy = 匹配 / (匹配 + 缺失边 + 多余边 + 表达式不符)`
  —— 分母是金标准边与机器边的并集,漏解析与误解析同权计入;阈值 ≥95%(`--field-threshold` 可调)。
- **表达式比对**:金标准 `expr` 归一化(压空白/大写/去截断标记)后须是机器表达式的**子串**
  (与语料库 `col_checks` 的"应包含子串"口径一致);留空即跳过。
- **不可解析用例**:`target_table: null` + `verified: true` 表示人工确认该 SQL 无法/不应解析
  (如 Hive 多表插入按登记规范应拆分,原因码 `multi_insert_unsupported`);
  机器同样失败 → 该用例表级算对,不贡献列边。

## 常见标注陷阱

1. **dst_col 以目标表为准(位置映射)**:Hive 语义下 SELECT 列按**位置**对应目标表列,
   不是按别名 —— `to_date(apply_time) AS biz_date` 写入 `dwd_apply_detail` 的第 6 列时,
   dst_col 是 `apply_date` 而非 `biz_date`。静态分区列不在 SELECT 中且恒排 schema 最后,
   前缀位置映射即正确语义。
2. **CTE / 子查询别名不是物理源表**:`WITH t AS (...)` 的 `t` 已被 lineage 递归穿透,
   `table_sources` 只留真实库表;来源列要穿透到最底层物理列。
3. **同任务临时表(tmp)**:解析层按"所见即所得"——写 tmp 的段目标就是 tmp 表,
   读 tmp 的段来源就是 tmp 表(穿透折叠是 derive 阶段的合成边,不在本抽检口径内);
   标注时不要自行把 tmp 折叠掉。
4. **自依赖**:目标表出现在 FROM/UNION 里是合法自依赖(增量滚动),
   要保留在 `table_sources` 且相应列边照标,别当成错误删掉。
5. **同一边身份多来源**:一个 dst_col 可以有多条边(UNION 各分支、多表表达式),逐条都要标;
   同身份重复(同表同列出现多次)按一条计。
6. **常量/COUNT(1) 列没有来源边**:`0 AS x`、`COUNT(1)` 无物理来源列,机器不出边,
   人工也不要补边。
7. **Spark hint 不是表**:`/*+ BROADCAST(o) */` 的参数不算源表。

## 目录内容

| 文件 | 说明 |
| --- | --- |
| `sample.py` | 第 0 步:sql_repository 按 层×形态 分层抽样导出(形态启发式:multi_insert > 自依赖 > LATERAL VIEW > WITH > OVER > JOIN > simple) |
| `extract.py` | 第 1 步:机器预填标注;已 `verified: true` 的文件**永不覆盖**(含 `--force`),未 verified 的默认跳过、`--force` 重新生成 |
| `compare.py` | 第 3 步:重解析比对 + markdown 报告 + CI 退出码 |
| `common.py` | 共享口径:标注 IO、边身份、比对判定 |
| `make_example.py` | 端到端自验:用 `lineage-pipeline/tests/warehouse_fixture.py` 的 13 段 SQL 走完整流程,重新生成 `example/` |
| `example/` | 演示产物:`sql/`(13 段 SQL + dialects.json)、`schema.json`、`annotations/`(其中 `02_etl_dwd_contract_n1` 故意改错一条边)、`report.md` |
| `tests/` | pytest:`python -m pytest tools/goldstd/tests -q` |

`example/report.md` 展示了故意改错被抓出的样子:13 用例表级 100%,
字段级 54/56 = 96.43%(1 缺失边 + 1 多余边,来自故意把 `is_active` 的来源列
`end_date` 改成 `start_date`)。
