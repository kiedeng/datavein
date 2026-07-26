# 全量重建压测工具(tools/benchmark)

在拿到行内真实 SQL 之前,用合成数仓提前验证设计方案的三条性能承诺:

| 指标 | 承诺 | 出处 |
|---|---|---|
| 全量血缘重建(5 千段 SQL,8 进程) | < 1h | 设计方案 5.1(1.3 给出 2h 上限) |
| 血缘查询 get_lineage(depth 3) | P95 < 500ms | 设计方案 1.3 |
| 影响分析 impact_analysis(全下游) | P95 < 2s | 设计方案 1.3 |

工具**不修改任何现有代码**:直接驱动 `pipeline.rebuild.full_rebuild()` 与
`lineage-mcp-server` 的 `server.repo`,分段计时通过运行期函数包装采集。

## 组成

- `generator.py` —— 合成数仓生成器(纯函数,同 seed 可复现)。
- `run_benchmark.py` —— 一键压测:建库 → DDL → 灌数 → 重建计时 → 查询延迟 → markdown 报告。
- `tests/test_generator.py` —— 生成器轻量测试(可复现性、解析通过率),不依赖 MySQL。
- `results/` —— 实测报告(`bench-500.md` 快速档、`bench-5000.md` 设计基线档)。

## 依赖与连接

Python 3.11,`sqlglot`/`pymysql`/`DBUtils`/`pytest` 已装,`lineage-pipeline` 已
`pip install -e`。MySQL 连接取环境变量(与集成测试一致):
`MYSQL_HOST`(默认 127.0.0.1)/`MYSQL_PORT`/`MYSQL_USER`(datavein)/
`MYSQL_PASSWORD`(test_pw)。账号需有建库权限;压测使用独立库
`datavein_bench`(每次运行先 DROP 重建,勿指向生产库)。

## 用法

```bash
cd tools/benchmark

# 单元测试(秒级,不连库)
python -m pytest tests -q

# 快速档:500 表 / 500 段 SQL
python run_benchmark.py --tables 500 --sqls 500 --seed 42

# 设计基线档:5000 / 5000,现状代码限时 20 分钟,超时自动以诊断模式重跑
python run_benchmark.py --tables 5000 --sqls 5000 --seed 42 \
    --budget-min 20 --retry-with-schema-cache

# 只灌数不压测(库需已建好并打过 sql/ DDL)
python generator.py --tables 500 --sqls 500 --seed 42 --database datavein_bench
```

### run_benchmark 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--tables / --sqls / --seed` | 500 / 500 / 42 | 合成规模与随机种子(同 seed 完全可复现) |
| `--database` | datavein_bench | 压测库名,运行前 DROP 重建 |
| `--workers` | 8 | 解析并行进程数(设计基线 8) |
| `--budget-min` | 0(不限) | 重建墙钟预算;超时杀进程组,保留 worker 已写出的逐 SQL 进度并外推 |
| `--schema-cache` | 关 | 直接以「诊断模式」运行(见下) |
| `--retry-with-schema-cache` | 关 | 现状代码超预算后,自动以诊断模式重跑并完成查询测试 |
| `--lineage-queries / --impact-queries / --depth` | 200 / 100 / 3 | 查询延迟采样量与深度 |
| `--report` | results/bench-\<tables\>.md | 报告输出路径 |

### 诊断模式(--schema-cache)是什么

压测发现现状代码的头号瓶颈:`pipeline/parser/core.py` 把原始 dict schema 直接
传给 sqlglot 的 `qualify` 与逐列 `lineage`,sqlglot 每次调用都重建一遍
MappingSchema(耗时与全库列数线性,5000 表约 4-5s/次;单段 SQL 触发约
`1 + 输出列数` 次重建,合计约 45s/段)。诊断模式在 benchmark 进程运行期为
`sqlglot.schema.ensure_schema` 打 id 缓存补丁(同一 schema dict 只构建一次),
**不改动仓库任何代码**,用来量化"schema 只建一次"修复后的可达水平。报告中
两种模式的数字分开标注,达标结论以现状代码为准。

## 合成语料说明

- 分层表比例:ods 32% / dwd 28% / dws 18% / ads 12% / dim 10%;每表 10-40 列,
  分区列 `dt` 恒排最后(与 `warehouse_fixture.py`、metadata_sync 口径一致);
  tmp 中转表按 tmp 链数量在 N 之外少量追加(库名 `tmp_bench`,layer=tmp)。
- SQL 形态(按段计):简单清洗 40% / 多表 JOIN 25%(含 Spark BROADCAST hint)/
  CTE+窗口 15% / 聚合 10%(含 DISTRIBUTE BY)/ 自依赖 5% / tmp 链 3%
  (ods→tmp、tmp→dwd 成对两段)/ LATERAL VIEW explode 2%。
  方言约 3/4 hive、1/4 spark。
- 血缘图为分层 DAG(同层不互写,自依赖仅自环),来源表按幂律偏斜采样,
  制造少量"ODS 热点表 + 大下游"的真实形态,供影响分析压测。
- 写入 `table_metadata` / `column_metadata` / `schema_snapshot` / `sql_repository`
  四表,`parse_status='pending'`,与生产采集链路落库口径相同。

## 报告解读

- **指标对照**表直接给出三项承诺的达标结论;现状代码与诊断模式分行列示。
- **重建耗时明细**:分段 = 解析(worker 并行,含 sqllineage 交叉校验)/
  入库 / tmp 折叠 / 闭包 / 两次影子表切换;"其余"为取数、影子建表、
  行数校验、统计落库等编排开销。分段数据来自运行期包装计时,
  与 `pipeline_run.stats_json` 的解析统计互为印证。
- 超预算被终止的运行,报告保留已完成段数、吞吐与单段耗时分位数,并按吞吐
  外推全量解析耗时(仅解析段,是总耗时的下界)。
- 查询延迟为 `server.repo` 进程内直连测得(无 MCP/HTTP 层、无缓存),
  P50/P95/P99 单位 ms。

## 与真实环境的差异(数字只作下界参考)

1. **合成 SQL 复杂度低于真实**:无数百行的存储过程式脚本、无动态拼接渲染残留、
   无生僻方言函数;真实 SQL 的解析耗时与 degraded/failed 比例都会更高。
2. **MySQL 与压测进程同机**:无网络 RTT、无连接抖动;行内跨机部署时查询延迟
   需加上网络与网关开销(1.3 的 P95 承诺是端到端口径的下界)。
3. **无并发负载**:查询延迟在库空闲时测得;重建窗口内或多用户并发时会更高。
4. **元数据规整**:合成 schema 快照 100% 齐全、列名规范;真实环境缺快照会把
   `select *` 降级为表级血缘,影响字段级覆盖率而非耗时。
5. 机器规格(CPU 核数、磁盘)与行内环境不同,重建吞吐按核数近似线性伸缩,
   报告头部已记录本机规格,供换算。

结论使用建议:合成档"未达标"的项,真实环境必然更差,应先修复再上线;
合成档"达标"的项只说明无结构性风险,仍需拿到真实 SQL 后复测。
