"""一键全量重建压测:提前验证设计方案 1.3 / 5.1 的性能承诺。

流程:
  1. 建独立库(默认 datavein_bench,先 DROP)→ 打 sql/ 全部 DDL;
  2. generator 合成数仓灌数(--tables/--sqls/--seed);
  3. 子进程内调 pipeline.rebuild.full_rebuild() 计时,分段耗时
     (解析/入库/折叠/闭包/切换)通过运行期包装函数与 pipeline_run.stats_json 采集,
     不修改任何 pipeline 代码;支持 --budget-min 墙钟预算,超时杀进程组并
     保留 worker 已写出的逐 SQL 解析进度数据(外推总耗时);
  4. 查询延迟:复用 lineage-mcp-server 的 server.repo(sys.path 引入),
     随机 200 次 get_lineage(depth 3)+ 100 次 impact_analysis(闭包表直查),
     报 P50/P95/P99;
  5. 输出 markdown 报告,与 1.3/5.1 指标逐项对照。

诊断模式(--schema-cache / --retry-with-schema-cache):
  压测发现 parse_sql 每次调用把原始 dict schema 交给 sqlglot,qualify 与逐列
  lineage 各自重建一遍 MappingSchema(5000 表约 4-5s/次,单 SQL 重建 ~11 次),
  这是全量重建的头号瓶颈。诊断模式在 benchmark 侧运行期为 sqlglot 的
  ensure_schema 加 id 缓存(同一 schema dict 只建一次 MappingSchema),
  用于量化修复后的可达水平——它不修改任何仓库代码,数字单独标注,
  不与"现状代码"混淆。

用法:
  python run_benchmark.py --tables 500 --sqls 500 --seed 42
  python run_benchmark.py --tables 5000 --sqls 5000 --seed 42 \
      --budget-min 20 --retry-with-schema-cache
"""

import argparse
import json
import math
import multiprocessing as mp
import os
import platform
import random
import signal
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))

import generator  # noqa: E402

MYSQL_CONF = dict(
    host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "datavein"),
    password=os.environ.get("MYSQL_PASSWORD", "test_pw"),
    charset="utf8mb4",
)

# 1.3 / 5.1 承诺值
TARGET_REBUILD_SEC = 3600          # 5.1:5 千段 SQL 并行 8 进程 <1h(1.3 上限 2h)
TARGET_LINEAGE_P95_MS = 500        # 1.3:血缘查询 P95 <500ms
TARGET_IMPACT_P95_MS = 2000       # 1.3:影响分析 P95 <2s


# ---------------- 基础工具 ----------------

def _pconn(database=None):
    import pymysql
    return pymysql.connect(**MYSQL_CONF, database=database, autocommit=True,
                           cursorclass=pymysql.cursors.DictCursor)


def _pctl(vals: list, q: float) -> float:
    s = sorted(vals)
    return s[min(len(s) - 1, max(0, math.ceil(q / 100 * len(s)) - 1))]


def _lat_stats(vals: list) -> dict:
    return {"n": len(vals), "avg": sum(vals) / len(vals),
            "p50": _pctl(vals, 50), "p95": _pctl(vals, 95),
            "p99": _pctl(vals, 99), "max": max(vals)}


def _fmt_sec(sec: float) -> str:
    if sec >= 3600:
        return f"{sec / 3600:.2f} h"
    if sec >= 60:
        return f"{sec / 60:.1f} min"
    return f"{sec:.1f} s"


# ---------------- 建库 + 灌数(仅父进程,直连 pymysql) ----------------

def setup_database(dbname: str) -> float:
    t0 = time.monotonic()
    admin = _pconn()
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
        cur.execute(f"CREATE DATABASE {dbname} CHARACTER SET utf8mb4")
    admin.close()
    conn = _pconn(dbname)
    with conn.cursor() as cur:
        for ddl_file in sorted((REPO_ROOT / "sql").glob("0*.sql")):
            for stmt in ddl_file.read_text().split(";"):
                if stmt.strip():
                    cur.execute(stmt)
    conn.close()
    return time.monotonic() - t0


def seed_data(dbname: str, tables: int, sqls: int, seed: int):
    t0 = time.monotonic()
    data = generator.generate(tables, sqls, seed)
    import pymysql
    conn = pymysql.connect(**MYSQL_CONF, database=dbname)
    counts = generator.load(conn, data)
    conn.close()
    return data, counts, time.monotonic() - t0


# ---------------- 重建子进程(pipeline 只在子进程内加载) ----------------

ORIG_PARSE_JOB = None       # fork 继承给 pool worker


def _emit(rec: dict):
    with open(os.environ["DV_BENCH_STAGES"], "a") as f:
        f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")


def _timed_parse_job(job):
    """替换 rebuild._parse_job:逐 SQL 记录进度与耗时(worker 内追加写)。"""
    t0 = time.monotonic()
    out = ORIG_PARSE_JOB(job)
    with open(os.environ["DV_BENCH_PROGRESS"], "a") as f:
        f.write(f"{time.time():.3f},{out[0]},{out[1].status},"
                f"{int((time.monotonic() - t0) * 1000)}\n")
    return out


def _install_schema_cache():
    """诊断模式:sqlglot ensure_schema 按 dict 身份缓存 MappingSchema。

    仅在 benchmark 进程运行期打补丁,不改动仓库代码;覆盖 sqlglot 内部
    以 from-import 方式持有 ensure_schema 的全部模块。
    """
    import sqlglot.schema as sgs
    orig = sgs.ensure_schema
    cache: dict = {}

    def cached(schema, **kw):
        if isinstance(schema, dict):
            key = id(schema)
            if key not in cache:
                cache[key] = orig(schema, **kw)
            return cache[key]
        return orig(schema, **kw)

    import sqlglot.lineage
    import sqlglot.optimizer.annotate_types
    import sqlglot.optimizer.isolate_table_selects
    import sqlglot.optimizer.optimizer
    import sqlglot.optimizer.pushdown_projections
    import sqlglot.optimizer.qualify
    import sqlglot.optimizer.qualify_columns
    for m in (sgs, sqlglot.optimizer.qualify, sqlglot.optimizer.qualify_columns,
              sqlglot.optimizer.pushdown_projections, sqlglot.optimizer.optimizer,
              sqlglot.optimizer.annotate_types,
              sqlglot.optimizer.isolate_table_selects, sqlglot.lineage):
        if hasattr(m, "ensure_schema"):
            m.ensure_schema = cached


def _wrap_stage(module, attr: str, stage: str, emit_start=False, name_by_arg=None):
    orig = getattr(module, attr)

    def wrapped(*a, **kw):
        label = f"{stage}:{a[name_by_arg]}" if name_by_arg is not None else stage
        if emit_start:
            _emit({"stage": f"{label}_start", "at": time.time()})
        t0 = time.monotonic()
        out = orig(*a, **kw)
        _emit({"stage": label, "sec": time.monotonic() - t0, "at": time.time()})
        return out

    setattr(module, attr, wrapped)


def _rebuild_child(dbname: str, workers: int, schema_cache: bool):
    os.setpgrp()                                  # 便于父进程整组终止
    from pipeline import config as pcfg
    pcfg.MYSQL.update({**MYSQL_CONF, "database": dbname})
    pcfg.PARALLEL_WORKERS = workers
    if schema_cache:
        _install_schema_cache()

    from pipeline import closure, derive, rebuild
    from pipeline import db as pdb
    global ORIG_PARSE_JOB
    ORIG_PARSE_JOB = rebuild._parse_job
    rebuild._parse_job = _timed_parse_job
    _wrap_stage(rebuild, "bulk_insert_shadow", "load_edges", emit_start=True)
    _wrap_stage(derive, "fold_tmp_edges", "tmp_fold")
    _wrap_stage(closure, "rebuild_closure_shadow", "closure")
    _wrap_stage(pdb, "swap_shadow", "swap", name_by_arg=1)

    _emit({"stage": "rebuild_start", "at": time.time(),
           "schema_cache": schema_cache, "workers": workers})
    t0 = time.monotonic()
    try:
        stats = rebuild.full_rebuild()
    except Exception as e:                        # 结构化落盘,父进程可读
        _emit({"stage": "error", "msg": repr(e)[:1000], "at": time.time()})
        raise
    _emit({"stage": "total", "sec": time.monotonic() - t0,
           "stats": stats, "at": time.time()})


def run_rebuild(dbname: str, workers: int, schema_cache: bool,
                budget_min: float, workdir: Path, tag: str, total_sqls: int) -> dict:
    stages_f = workdir / f"stages-{tag}.jsonl"
    progress_f = workdir / f"progress-{tag}.csv"
    stages_f.write_text("")
    progress_f.write_text("")
    os.environ["DV_BENCH_STAGES"] = str(stages_f)
    os.environ["DV_BENCH_PROGRESS"] = str(progress_f)

    ctx = mp.get_context("fork")
    proc = ctx.Process(target=_rebuild_child,
                       args=(dbname, workers, schema_cache))
    t0 = time.monotonic()
    proc.start()
    proc.join(budget_min * 60 if budget_min else None)
    aborted = False
    if proc.is_alive():
        aborted = True
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.join(15)
    wall = time.monotonic() - t0

    stages: dict = {}
    marks: dict = {}
    stats = error = None
    total_sec = None
    for line in stages_f.read_text().splitlines():
        rec = json.loads(line)
        st = rec["stage"]
        if st.endswith("_start") or st == "rebuild_start":
            marks[st] = rec["at"]
        elif st == "total":
            total_sec, stats = rec["sec"], rec.get("stats")
        elif st == "error":
            error = rec["msg"]
        else:
            stages[st] = stages.get(st, 0.0) + rec["sec"]
    if "rebuild_start" in marks and "load_edges_start" in marks:
        stages["parse"] = marks["load_edges_start"] - marks["rebuild_start"]

    prog_rows = []
    for line in progress_f.read_text().splitlines():
        ts, sql_id, status, cost = line.split(",")
        prog_rows.append((float(ts), status, int(cost)))
    parse_prog = None
    if prog_rows:
        costs = [c for _, _, c in prog_rows]
        elapsed = (max(r[0] for r in prog_rows)
                   - marks.get("rebuild_start", prog_rows[0][0]))
        statuses: dict = {}
        for _, s, _ in prog_rows:
            statuses[s] = statuses.get(s, 0) + 1
        rate = len(prog_rows) / elapsed if elapsed > 0 else 0
        parse_prog = {
            "done": len(prog_rows), "statuses": statuses,
            "elapsed_sec": elapsed, "rate_per_sec": rate,
            "cost_ms": _lat_stats(costs),
            "extrapolated_parse_sec": total_sqls / rate if rate else None,
        }
    return {"tag": tag, "schema_cache": schema_cache, "aborted": aborted,
            "exitcode": proc.exitcode, "wall_sec": wall, "total_sec": total_sec,
            "stages": stages, "stats": stats, "error": error,
            "parse_progress": parse_prog, "budget_min": budget_min}


# ---------------- 查询延迟(复用 server.repo) ----------------

def bench_queries(dbname: str, seed: int, n_lineage: int, n_impact: int,
                  depth: int) -> dict | None:
    sys.path.insert(0, str(REPO_ROOT / "lineage-mcp-server"))
    from server import config as scfg
    scfg.MYSQL.update({**MYSQL_CONF, "database": dbname})
    from server import repo
    conn = repo.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT full_name FROM lineage_node WHERE column_name=''")
        nodes = [r["full_name"] for r in cur.fetchall()]
        cur.execute("SELECT DISTINCT ancestor FROM table_closure")
        ancestors = [r["ancestor"] for r in cur.fetchall()]
    if not nodes or not ancestors:
        return None

    rng = random.Random(seed + 1)
    repo.get_lineage(conn, rng.choice(nodes), depth=depth)   # 预热连接
    lin, downstream_cnt = [], 0
    for _ in range(n_lineage):
        node = rng.choice(nodes)
        direction = rng.choice(["upstream", "downstream"])
        downstream_cnt += direction == "downstream"
        t0 = time.perf_counter()
        repo.get_lineage(conn, node, direction=direction, depth=depth)
        lin.append((time.perf_counter() - t0) * 1000)
    imp = []
    for _ in range(n_impact):
        anc = rng.choice(ancestors)
        t0 = time.perf_counter()
        repo.impact_analysis(conn, anc)
        imp.append((time.perf_counter() - t0) * 1000)
    return {"lineage": _lat_stats(lin), "impact": _lat_stats(imp),
            "depth": depth, "node_pool": len(nodes),
            "ancestor_pool": len(ancestors), "downstream_cnt": downstream_cnt}


# ---------------- 结果采集与报告 ----------------

def collect_db_facts(dbname: str) -> dict:
    conn = _pconn(dbname)
    facts: dict = {}
    with conn.cursor() as cur:
        cur.execute("SELECT VERSION() AS v")
        facts["mysql_version"] = cur.fetchone()["v"]
        cur.execute("""SELECT parse_status, COALESCE(parse_reason,'') AS reason,
                              COUNT(*) AS c
                       FROM sql_repository GROUP BY parse_status, parse_reason""")
        facts["parse_status"] = cur.fetchall()
        for name, sql in (
                ("nodes", "SELECT COUNT(*) AS c FROM lineage_node"),
                ("edges_table", "SELECT COUNT(*) AS c FROM lineage_edge WHERE edge_level='table'"),
                ("edges_column", "SELECT COUNT(*) AS c FROM lineage_edge WHERE edge_level='column'"),
                ("closure_rows", "SELECT COUNT(*) AS c FROM table_closure")):
            try:
                cur.execute(sql)
                facts[name] = cur.fetchone()["c"]
            except Exception:
                facts[name] = None
        cur.execute("""SELECT run_id, status, stats_json,
                              TIMESTAMPDIFF(SECOND, started_at, finished_at) AS sec
                       FROM pipeline_run WHERE run_type='full'
                       ORDER BY run_id""")
        facts["runs"] = cur.fetchall()
    conn.close()
    return facts


STAGE_LABELS = (("parse", "解析(并行 worker,含 schema 装载与 sqllineage 交叉校验)"),
                ("load_edges", "入库(节点/边 bulk insert 影子表)"),
                ("tmp_fold", "tmp 穿透折叠"),
                ("closure", "闭包重算(含影子表写入)"),
                ("swap:lineage_edge", "切换(边表 RENAME)"),
                ("swap:table_closure", "切换(闭包表 RENAME)"))


def _verdict(ok: bool | None) -> str:
    return {True: "达标", False: "未达标", None: "无法判定"}[ok]


def _render_attempt(a: dict, total_sqls: int) -> list:
    mode = ("诊断模式(benchmark 侧 sqlglot MappingSchema 缓存补丁,未改 pipeline 代码)"
            if a["schema_cache"] else "现状代码(as-is)")
    out = [f"### 重建执行:{mode}", ""]
    if a["aborted"]:
        out += [f"- 结果:**超出 {a['budget_min']:g} 分钟墙钟预算,已终止**"
                f"(实际运行 {_fmt_sec(a['wall_sec'])})"]
    elif a["error"]:
        out += [f"- 结果:**异常终止** `{a['error']}`"]
    else:
        out += [f"- 结果:完成,full_rebuild 总耗时 **{_fmt_sec(a['total_sec'])}**"
                f"({a['total_sec']:.1f} s)"]
    p = a["parse_progress"]
    if p:
        c = p["cost_ms"]
        out += ["",
                f"- 解析进度:{p['done']}/{total_sqls} 段,吞吐 "
                f"{p['rate_per_sec']:.2f} 段/s,状态分布 {p['statuses']}",
                f"- 单段解析耗时(worker 内,含交叉校验):P50 {c['p50']:.0f} ms / "
                f"P95 {c['p95']:.0f} ms / P99 {c['p99']:.0f} ms / max {c['max']:.0f} ms"]
        if a["aborted"] and p["extrapolated_parse_sec"]:
            out += [f"- **按已测吞吐外推**:{total_sqls} 段解析约需 "
                    f"{_fmt_sec(p['extrapolated_parse_sec'])}"
                    f"(仅解析段,未含入库/折叠/闭包/切换)"]
    if a["stages"]:
        out += ["", "| 分段 | 耗时 |", "|---|---|"]
        known = 0.0
        for key, label in STAGE_LABELS:
            if key in a["stages"]:
                out.append(f"| {label} | {_fmt_sec(a['stages'][key])} |")
                known += a["stages"][key]
        if a["total_sec"]:
            out.append(f"| 其余(取数/影子建表/校验/统计落库) | "
                       f"{_fmt_sec(max(a['total_sec'] - known, 0))} |")
            out.append(f"| **合计(full_rebuild)** | **{_fmt_sec(a['total_sec'])}** |")
    if a["stats"]:
        out += ["", f"- pipeline 返回 stats:`{json.dumps(a['stats'], ensure_ascii=False)}`"]
    out.append("")
    return out


def write_report(path: Path, args, gen_data, gen_counts, t_setup, t_seed,
                 attempts, queries, facts):
    final = attempts[-1]
    ok_rebuild = (None if final["aborted"] or final["error"]
                  else final["total_sec"] < TARGET_REBUILD_SEC)
    asis = attempts[0]
    asis_ok = (False if asis["aborted"]
               else (None if asis["error"] else asis["total_sec"] < TARGET_REBUILD_SEC))
    ok_lin = queries and queries["lineage"]["p95"] < TARGET_LINEAGE_P95_MS
    ok_imp = queries and queries["impact"]["p95"] < TARGET_IMPACT_P95_MS

    L = [f"# 全量重建压测报告:{args.tables} 表 / {args.sqls} 段 SQL", "",
         f"- 时间:{datetime.now():%Y-%m-%d %H:%M:%S};seed={args.seed};"
         f"并行 worker={args.workers};库=`{args.database}`",
         f"- 环境:{platform.platform()};CPU {os.cpu_count()} 核;"
         f"MySQL {facts['mysql_version']}(本机,无网络延迟)",
         f"- 建库+DDL {t_setup:.1f} s;生成+灌数 {t_seed:.1f} s"
         f"(表 {gen_counts['tables']} 含 tmp、列 {gen_counts['columns']}、"
         f"SQL {gen_counts['sqls']})",
         f"- 分层表数:{gen_data['layer_counts']};SQL 形态:{gen_data['shape_counts']}",
         "", "## 指标对照(设计方案 1.3 / 5.1)", "",
         "| 指标 | 承诺 | 实测 | 结论 |", "|---|---|---|---|"]

    if asis["aborted"]:
        p = asis["parse_progress"]
        est = (_fmt_sec(p["extrapolated_parse_sec"])
               if p and p["extrapolated_parse_sec"] else "无法外推")
        measured = (f"{asis['budget_min']:g} min 预算内未完成;"
                    f"仅解析段外推约 {est}")
    elif asis["error"]:
        measured = f"异常:{asis['error']}"
    else:
        measured = _fmt_sec(asis["total_sec"])
    L.append(f"| 全量重建总耗时(现状代码) | <1h(5.1;1.3 上限 2h) | {measured} | "
             f"{_verdict(asis_ok)} |")
    if len(attempts) > 1 and not final["aborted"] and not final["error"]:
        L.append(f"| 全量重建总耗时(schema 缓存诊断模式) | <1h | "
                 f"{_fmt_sec(final['total_sec'])} | {_verdict(ok_rebuild)}(修复后可达) |")
    if queries:
        L.append(f"| get_lineage depth={queries['depth']} P95 | <500ms | "
                 f"{queries['lineage']['p95']:.1f} ms | {_verdict(bool(ok_lin))} |")
        L.append(f"| impact_analysis 全下游 P95 | <2s | "
                 f"{queries['impact']['p95']:.1f} ms | {_verdict(bool(ok_imp))} |")
    else:
        L.append("| get_lineage / impact_analysis | <500ms / <2s | 重建未完成,未测 | 无法判定 |")
    L.append("")

    L.append("## 重建耗时明细")
    L.append("")
    for a in attempts:
        L += _render_attempt(a, args.sqls)

    if queries:
        L += ["## 查询延迟(server.repo 直连,单位 ms)", "",
              f"- 采样:get_lineage {queries['lineage']['n']} 次"
              f"(depth={queries['depth']},上/下游随机,表级;节点池 "
              f"{queries['node_pool']});impact_analysis {queries['impact']['n']} 次"
              f"(闭包表直查;祖先池 {queries['ancestor_pool']})", "",
              "| 查询 | P50 | P95 | P99 | max | avg |", "|---|---|---|---|---|---|"]
        for name, s in (("get_lineage", queries["lineage"]),
                        ("impact_analysis", queries["impact"])):
            L.append(f"| {name} | {s['p50']:.1f} | {s['p95']:.1f} | "
                     f"{s['p99']:.1f} | {s['max']:.1f} | {s['avg']:.1f} |")
        L.append("")

    L += ["## 血缘库规模与解析状态", "",
          f"- lineage_node {facts['nodes']};lineage_edge 表级 {facts['edges_table']} / "
          f"字段级 {facts['edges_column']};table_closure {facts['closure_rows']}",
          "- sql_repository 解析状态:"]
    for r in facts["parse_status"]:
        reason = f"({r['reason']})" if r["reason"] else ""
        L.append(f"  - {r['parse_status']}{reason}: {r['c']}")
    if facts["runs"]:
        L += ["- pipeline_run(run_type=full):"]
        for r in facts["runs"]:
            L.append(f"  - run_id={r['run_id']} status={r['status']} 耗时={r['sec']}s")
    L += ["", "## 结论与说明", ""]

    if asis["aborted"] or (asis["total_sec"] or 0) >= TARGET_REBUILD_SEC:
        L += ["- **现状代码在本档规模下无法兑现 <1h 承诺**。瓶颈定位:"
              "`pipeline/parser/core.py` 将原始 dict schema 直接传给 sqlglot 的 "
              "qualify 与逐列 lineage,每次调用都重建一遍 MappingSchema"
              "(全量 schema 规模线性),单段 SQL 重建约 11 次;5000 表(约 12 万列)"
              "规模下单段解析约 45s。",
              "- 修复建议(1 行级改动):在 rebuild 的 worker initializer 中把 "
              "schema dict 预构建为 `sqlglot.MappingSchema` 传入(或对 "
              "ensure_schema 结果做缓存),并同步调整 `_dst_columns` 的 dict 取数;"
              "诊断模式实测即为该修复的预期收益。"]
    else:
        L += ["- 现状代码在本档规模下满足 <1h 重建承诺。"]
    if queries:
        L += [f"- 查询侧:get_lineage P95 {queries['lineage']['p95']:.1f} ms、"
              f"impact P95 {queries['impact']['p95']:.1f} ms,"
              + ("均在承诺范围内。" if (ok_lin and ok_imp) else "存在超标项,见上表。")]
    L += ["- 本报告数字仅作**下界参考**:合成 SQL 复杂度低于真实(无超长存储过程式"
          "脚本、无动态拼接残留、注释/中文常量少),MySQL 与压测进程同机无网络延迟,"
          "查询期无并发负载;详见 README「与真实环境的差异」。", ""]

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(L))
    return {"rebuild_ok": asis_ok, "lineage_ok": ok_lin, "impact_ok": ok_imp}


def main():
    ap = argparse.ArgumentParser(description="全量重建一键压测")
    ap.add_argument("--tables", type=int, default=500)
    ap.add_argument("--sqls", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--database", default="datavein_bench")
    ap.add_argument("--workers", type=int, default=8,
                    help="解析并行进程数(设计基线 8)")
    ap.add_argument("--budget-min", type=float, default=0,
                    help="重建墙钟预算(分钟),0 为不限;超时杀进程并保留分段数据")
    ap.add_argument("--schema-cache", action="store_true",
                    help="直接以诊断模式运行(sqlglot MappingSchema 缓存补丁)")
    ap.add_argument("--retry-with-schema-cache", action="store_true",
                    help="现状代码超预算被终止后,自动以诊断模式重跑并完成后续阶段")
    ap.add_argument("--lineage-queries", type=int, default=200)
    ap.add_argument("--impact-queries", type=int, default=100)
    ap.add_argument("--depth", type=int, default=3)
    ap.add_argument("--report", default=None,
                    help="报告输出路径,默认 results/bench-<tables>.md")
    args = ap.parse_args()

    report_path = Path(args.report) if args.report else (
        HERE / "results" / f"bench-{args.tables}.md")

    print(f"[1/5] 建库 {args.database} + DDL ...", flush=True)
    t_setup = setup_database(args.database)
    print(f"[2/5] 生成并灌入 {args.tables} 表 / {args.sqls} SQL (seed={args.seed}) ...",
          flush=True)
    gen_data, gen_counts, t_seed = seed_data(
        args.database, args.tables, args.sqls, args.seed)
    print(f"      灌数完成 {gen_counts} 用时 {t_seed:.1f}s", flush=True)

    attempts = []
    with tempfile.TemporaryDirectory(prefix="dv-bench-") as wd:
        workdir = Path(wd)
        mode = "schema-cache" if args.schema_cache else "as-is"
        print(f"[3/5] full_rebuild({mode})"
              + (f",预算 {args.budget_min:g} min" if args.budget_min else "")
              + " ...", flush=True)
        a1 = run_rebuild(args.database, args.workers, args.schema_cache,
                         args.budget_min, workdir, "run1", args.sqls)
        attempts.append(a1)
        print(f"      run1: aborted={a1['aborted']} total={a1['total_sec']}",
              flush=True)
        if a1["aborted"] and args.retry_with_schema_cache and not args.schema_cache:
            print("[3b] 现状代码超预算,以诊断模式(schema 缓存)重跑 ...", flush=True)
            a2 = run_rebuild(args.database, args.workers, True, 0,
                             workdir, "run2", args.sqls)
            attempts.append(a2)
            print(f"      run2: aborted={a2['aborted']} total={a2['total_sec']}",
                  flush=True)

    final = attempts[-1]
    queries = None
    if not final["aborted"] and not final["error"]:
        print(f"[4/5] 查询延迟:get_lineage x{args.lineage_queries} "
              f"(depth={args.depth}) + impact x{args.impact_queries} ...", flush=True)
        queries = bench_queries(args.database, args.seed,
                                args.lineage_queries, args.impact_queries,
                                args.depth)
    else:
        print("[4/5] 重建未完成,跳过查询延迟测试", flush=True)

    facts = collect_db_facts(args.database)
    print(f"[5/5] 写报告 {report_path}", flush=True)
    verdict = write_report(report_path, args, gen_data, gen_counts,
                           t_setup, t_seed, attempts, queries, facts)
    print(json.dumps({"verdict": verdict,
                      "rebuild_total_sec": final["total_sec"],
                      "aborted_first_run": attempts[0]["aborted"],
                      "lineage_p95_ms": queries and round(queries["lineage"]["p95"], 1),
                      "impact_p95_ms": queries and round(queries["impact"]["p95"], 1)},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
