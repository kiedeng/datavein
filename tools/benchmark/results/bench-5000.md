# 全量重建压测报告:5000 表 / 5000 段 SQL

> **重要:本报告数字为 schema 缓存修复(commit 89246a5)之后。** 压测最初暴露解析器
> 把 dict schema 反复重建 MappingSchema 的缺陷,修复前同规模仅 ~0.8 SQL/s、
> 全量重建需 100+ 分钟(击穿 <1h 承诺);修复后如下。这是本次压测的核心产出。

- 时间:2026-07-26 14:56:49;seed=42;并行 worker=8;库=`datavein_bench`
- 环境:Linux-6.18.5-x86_64-with-glibc2.39;CPU 4 核;MySQL 8.0.46-0ubuntu0.24.04.3(本机,无网络延迟)
- 建库+DDL 0.8 s;生成+灌数 7.5 s(表 5075 含 tmp、列 125526、SQL 5000)
- 分层表数:{'ods': 1600, 'dwd': 1400, 'dws': 900, 'ads': 600, 'dim': 500};SQL 形态:{'simple': 2000, 'join': 1250, 'cte_window': 750, 'agg': 500, 'self_dep': 250, 'tmp_chain': 150, 'lateral': 100}

## 指标对照(设计方案 1.3 / 5.1)

| 指标 | 承诺 | 实测 | 结论 |
|---|---|---|---|
| 全量重建总耗时(现状代码) | <1h(5.1;1.3 上限 2h) | 3.3 min | 达标 |
| get_lineage depth=3 P95 | <500ms | 1.6 ms | 达标 |
| impact_analysis 全下游 P95 | <2s | 0.7 ms | 达标 |

## 重建耗时明细

### 重建执行:现状代码(as-is)

- 结果:完成,full_rebuild 总耗时 **3.3 min**(197.5 s)

- 解析进度:5000/5000 段,吞吐 28.16 段/s,状态分布 {'success': 5000}
- 单段解析耗时(worker 内,含交叉校验):P50 230 ms / P95 408 ms / P99 520 ms / max 14812 ms

| 分段 | 耗时 |
|---|---|
| 解析(并行 worker,含 schema 装载与 sqllineage 交叉校验) | 3.0 min |
| 入库(节点/边 bulk insert 影子表) | 18.5 s |
| tmp 穿透折叠 | 0.7 s |
| 闭包重算(含影子表写入) | 0.6 s |
| 切换(边表 RENAME) | 0.0 s |
| 切换(闭包表 RENAME) | 0.0 s |
| 其余(取数/影子建表/校验/统计落库) | 0.1 s |
| **合计(full_rebuild)** | **3.3 min** |

- pipeline 返回 stats:`{"parse": {"success": 5000, "degraded": 0, "failed": 0, "crosscheck_mismatch": 0}, "edges": 48989, "closure_rows": 15157, "tmp_fold": {"synthetic_edges": 560, "deleted_tmp_edges": 1120}, "cycles": []}`

## 查询延迟(server.repo 直连,单位 ms)

- 采样:get_lineage 200 次(depth=3,上/下游随机,表级;节点池 4038);impact_analysis 100 次(闭包表直查;祖先池 2784)

| 查询 | P50 | P95 | P99 | max | avg |
|---|---|---|---|---|---|
| get_lineage | 0.8 | 1.6 | 2.2 | 2.3 | 0.9 |
| impact_analysis | 0.5 | 0.7 | 0.8 | 4.0 | 0.5 |

## 血缘库规模与解析状态

- lineage_node 42163;lineage_edge 表级 7212 / 字段级 41777;table_closure 15157
- sql_repository 解析状态:
  - success: 5000
- pipeline_run(run_type=full):
  - run_id=1 status=success 耗时=198s

## 结论与说明

- 修复后代码在本档规模满足 <1h 重建承诺,余量充分(3.3 min vs 60 min)。
- 查询侧:get_lineage P95 1.6 ms、impact P95 0.7 ms,均远在承诺范围内。
- **解析(3.0 min)占总耗时 91%,其中 sqllineage 交叉校验是修复 schema 后的新主导
  成本**(单段 P50 230ms 大部分在此)。若未来真实规模紧张,可评估全量重建时交叉校验
  改为抽样(如 10%)而非逐段——增量路径保持逐段全校验。
- **真实规模外推**:本档 5000 表 / 12.5 万列,是设计基线(5 万表 / 125 万列)的
  1/10 表数。解析并行且交叉校验主导,基本线性,10 倍规模线性外推约 33 min,仍 <1h;
  且 MappingSchema 每 worker 每 dialect 只建一次(不随段数/表数线性),真实规模相对
  更优。但需以行内真实 SQL 与生产 MySQL 调优复测为准。
- 本报告数字仅作**下界参考**:合成 SQL 复杂度低于真实(无超长存储过程式脚本、
  无动态拼接残留、注释/中文常量少),MySQL 与压测进程同机无网络延迟,查询期无并发
  负载;详见 README「与真实环境的差异」。
