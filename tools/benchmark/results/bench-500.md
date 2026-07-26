# 全量重建压测报告:500 表 / 500 段 SQL

> 数字为 schema 缓存修复(commit 89246a5)之后。快速档,验证正确性用;
> 设计基线规模见 bench-5000.md。

- 时间:2026-07-26 14:53:03;seed=42;并行 worker=8;库=`datavein_bench`
- 环境:Linux-6.18.5-x86_64-with-glibc2.39;CPU 4 核;MySQL 8.0.46-0ubuntu0.24.04.3(本机,无网络延迟)
- 建库+DDL 1.0 s;生成+灌数 1.0 s(表 507 含 tmp、列 12491、SQL 500)
- 分层表数:{'ods': 160, 'dwd': 140, 'dws': 90, 'ads': 60, 'dim': 50};SQL 形态:{'simple': 201, 'join': 125, 'cte_window': 75, 'agg': 50, 'self_dep': 25, 'tmp_chain': 14, 'lateral': 10}

## 指标对照(设计方案 1.3 / 5.1)

| 指标 | 承诺 | 实测 | 结论 |
|---|---|---|---|
| 全量重建总耗时(现状代码) | <1h(5.1;1.3 上限 2h) | 21.8 s | 达标 |
| get_lineage depth=3 P95 | <500ms | 7.0 ms | 达标 |
| impact_analysis 全下游 P95 | <2s | 0.8 ms | 达标 |

## 重建耗时明细

### 重建执行:现状代码(as-is)

- 结果:完成,full_rebuild 总耗时 **21.8 s**(21.8 s)

- 解析进度:500/500 段,吞吐 25.76 段/s,状态分布 {'success': 500}
- 单段解析耗时(worker 内,含交叉校验):P50 224 ms / P95 429 ms / P99 3243 ms / max 4110 ms

| 分段 | 耗时 |
|---|---|
| 解析(并行 worker,含 schema 装载与 sqllineage 交叉校验) | 19.5 s |
| 入库(节点/边 bulk insert 影子表) | 2.1 s |
| tmp 穿透折叠 | 0.1 s |
| 闭包重算(含影子表写入) | 0.1 s |
| 切换(边表 RENAME) | 0.0 s |
| 切换(闭包表 RENAME) | 0.0 s |
| 其余(取数/影子建表/校验/统计落库) | 0.0 s |
| **合计(full_rebuild)** | **21.8 s** |

- pipeline 返回 stats:`{"parse": {"success": 500, "degraded": 0, "failed": 0, "crosscheck_mismatch": 0}, "edges": 4888, "closure_rows": 1340, "tmp_fold": {"synthetic_edges": 52, "deleted_tmp_edges": 104}, "cycles": []}`

## 查询延迟(server.repo 直连,单位 ms)

- 采样:get_lineage 200 次(depth=3,上/下游随机,表级;节点池 404);impact_analysis 100 次(闭包表直查;祖先池 266)

| 查询 | P50 | P95 | P99 | max | avg |
|---|---|---|---|---|---|
| get_lineage | 0.7 | 7.0 | 10.0 | 11.0 | 1.6 |
| impact_analysis | 0.4 | 0.8 | 1.2 | 1.3 | 0.5 |

## 血缘库规模与解析状态

- lineage_node 4151;lineage_edge 表级 711 / 字段级 4177;table_closure 1340
- sql_repository 解析状态:
  - success: 500
- pipeline_run(run_type=full):
  - run_id=1 status=success 耗时=22s

## 结论与说明

- 现状代码在本档规模下满足 <1h 重建承诺。
- 查询侧:get_lineage P95 7.0 ms、impact P95 0.8 ms,均在承诺范围内。
- 本报告数字仅作**下界参考**:合成 SQL 复杂度低于真实(无超长存储过程式脚本、无动态拼接残留、注释/中文常量少),MySQL 与压测进程同机无网络延迟,查询期无并发负载;详见 README「与真实环境的差异」。
