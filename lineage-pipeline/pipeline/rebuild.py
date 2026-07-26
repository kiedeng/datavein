"""流水线编排:全量重建与增量触发两种模式(设计方案 5.1)。

全量:并行解析(每 SQL 60s 超时)→ 边写影子表 → 行数校验 → 原子切换 →
闭包重算影子表 → 校验 → 切换。校验不过即中止,旧版血缘继续服务(4.3)。
"""

import json
import logging
import signal
from concurrent.futures import ProcessPoolExecutor

from . import closure, config, crosscheck, db, derive
from .loader import bulk_insert_shadow, preload_node_cache, replace_edges_for_sql
from .models import ParseResult
from .parser.core import parse_sql

log = logging.getLogger(__name__)

# ---- 并行解析 worker:schema 经 initializer 传一次,避免每任务序列化 125 万列 ----
_worker_schema: dict | None = None


def _init_worker(schema: dict):
    global _worker_schema
    _worker_schema = schema
    signal.signal(signal.SIGALRM, _raise_timeout)


def _raise_timeout(signum, frame):
    raise TimeoutError


def _parse_job(job: dict) -> tuple[int, ParseResult]:
    signal.alarm(config.PARSE_TIMEOUT_SECONDS)
    try:
        result = parse_sql(job["sql_text"],
                           dialect=job["dialect"] or config.SQL_DIALECT_DEFAULT,
                           schema=_worker_schema)
    except TimeoutError:
        result = ParseResult(status="degraded", reason="timeout",
                             target_table=job.get("target_table") or "",
                             message=f"解析超时>{config.PARSE_TIMEOUT_SECONDS}s,降级表级")
    except Exception as e:                       # 解析器内部异常不炸整批
        result = ParseResult(status="failed", reason="internal_error",
                             message=str(e)[:500])
    finally:
        signal.alarm(0)
    if (config.CROSSCHECK_ENABLED and result.status == "success"
            and crosscheck.mismatch(result, job["sql_text"],
                                    job["dialect"] or config.SQL_DIALECT_DEFAULT)):
        result.reason = "crosscheck_mismatch"    # 双引擎不一致 → 人工复核队列(5.2)
    return job["sql_id"], result


def load_schema_snapshot(conn, snap_date=None) -> dict:
    """schema_snapshot → sqlglot schema dict {db: {table: {col: type}}}(5.2)。"""
    sql = """SELECT full_name, columns_json FROM schema_snapshot s
             WHERE snap_date = (SELECT MAX(snap_date) FROM schema_snapshot
                                WHERE full_name = s.full_name
                                  AND (%s IS NULL OR snap_date <= %s))"""
    schema: dict = {}
    with conn.cursor() as cur:
        cur.execute(sql, (snap_date, snap_date))
        for row in cur.fetchall():
            database, table = row["full_name"].split(".", 1)
            cols = json.loads(row["columns_json"]) if row["columns_json"] else []
            schema.setdefault(database, {})[table] = {c["name"]: c["type"] for c in cols}
    return schema


def _count(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()["c"]


def full_rebuild():
    conn = db.connect()
    with db.record_run(conn, "full") as run:
        schema = load_schema_snapshot(conn)
        with conn.cursor() as cur:
            cur.execute("""SELECT sql_id, sql_text, dialect, target_table
                           FROM sql_repository WHERE is_active=1""")
            jobs = cur.fetchall()

        stats = {"success": 0, "degraded": 0, "failed": 0, "crosscheck_mismatch": 0}
        parsed: list[tuple[int, ParseResult]] = []
        with ProcessPoolExecutor(max_workers=config.PARALLEL_WORKERS,
                                 initializer=_init_worker,
                                 initargs=(schema,)) as pool:
            for sql_id, result in pool.map(_parse_job, jobs, chunksize=8):
                parsed.append((sql_id, result))
                stats[result.status] += 1
                if result.reason == "crosscheck_mismatch":
                    stats["crosscheck_mismatch"] += 1
        log.info("parse done: %s", stats)

        # 边影子表:重解析边 + 保留非 SQL 来源边(manual/ingest_map,5.3/5.5)
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS lineage_edge_shadow")
            cur.execute("CREATE TABLE lineage_edge_shadow LIKE lineage_edge")
            cur.execute("""INSERT INTO lineage_edge_shadow
                           SELECT * FROM lineage_edge WHERE src_type <> 'sql'""")
        conn.commit()
        bulk_insert_shadow(conn, parsed, preload_node_cache(conn))

        # tmp 穿透折叠(5.3):必须在影子表校验之前,行数口径为折叠后的最终边
        fold_stats = derive.fold_tmp_edges(conn, edge_table="lineage_edge_shadow")
        log.info("tmp fold: %s", fold_stats)

        old_edges = _count(conn, "SELECT COUNT(*) AS c FROM lineage_edge")
        new_edges = _count(conn, "SELECT COUNT(*) AS c FROM lineage_edge_shadow")
        if old_edges > 0 and abs(new_edges - old_edges) / old_edges > config.REBUILD_EDGE_DELTA_MAX:
            run["status"] = "aborted"
            run["stats"] = {"parse": stats, "edge_delta": [old_edges, new_edges],
                            "reason": "edge_delta_exceeded"}
            log.error("edge delta %s -> %s exceeds %.0f%%, swap aborted",
                      old_edges, new_edges, config.REBUILD_EDGE_DELTA_MAX * 100)
            return run["stats"]
        db.swap_shadow(conn, "lineage_edge")

        closure_rows = closure.rebuild_closure_shadow(conn)
        old_closure = _count(conn, "SELECT COUNT(*) AS c FROM table_closure")
        if old_closure > 0 and abs(closure_rows - old_closure) / old_closure > config.REBUILD_CLOSURE_DELTA_MAX:
            run["status"] = "aborted"
            run["stats"] = {"parse": stats, "closure_delta": [old_closure, closure_rows],
                            "reason": "closure_delta_exceeded"}
            log.error("closure delta %s -> %s exceeds threshold, swap aborted",
                      old_closure, closure_rows)
            return run["stats"]
        db.swap_shadow(conn, "table_closure")

        # 非自环环路检测(5.4):建模错误信号,推送双方 owner 走行内告警通道,
        # 代码侧到 stats 与 warning 日志为止
        cycles = closure.detect_cycles(conn)
        if cycles:
            log.warning("检出非自环环路 %d 组(建模错误信号,5.4),需通知双方 owner: %s",
                        len(cycles), cycles[:20])

        run["stats"] = {"parse": stats, "edges": new_edges, "closure_rows": closure_rows,
                        "tmp_fold": fold_stats, "cycles": cycles}
        return run["stats"]


def incremental(sql_id: int):
    """发布钩子触发:单条重解析 → 换边 → 闭包区域修补,分钟级生效(5.1)。"""
    conn = db.connect()
    with db.record_run(conn, "incremental") as run:
        with conn.cursor() as cur:
            cur.execute("""SELECT sql_id, sql_text, dialect, target_table
                           FROM sql_repository WHERE sql_id=%s""", (sql_id,))
            job = cur.fetchone()
        if not job:
            raise ValueError(f"sql_id {sql_id} not found")
        result = parse_sql(job["sql_text"],
                           dialect=job["dialect"] or config.SQL_DIALECT_DEFAULT,
                           schema=load_schema_snapshot(conn))
        if (config.CROSSCHECK_ENABLED and result.status == "success"
                and crosscheck.mismatch(result, job["sql_text"],
                                        job["dialect"] or config.SQL_DIALECT_DEFAULT)):
            result.reason = "crosscheck_mismatch"
        replace_edges_for_sql(conn, sql_id, result)
        # tmp 穿透折叠(5.3):增量只在同 task_id 范围内折叠,限制见 fold_for_task
        fold_stats = derive.fold_for_task(conn, sql_id)
        outcome = closure.incremental_patch(
            conn, result.target_table or job["target_table"])
        if outcome == "degrade_full_rebuild":
            # 核心枢纽表:增量不划算,转异步全量并标注数据时点(2.2)
            log.warning("closure patch degraded for %s, schedule full rebuild",
                        result.target_table)
        run["stats"] = {"sql_id": sql_id, "parse": result.status,
                        "reason": result.reason, "closure": outcome,
                        "tmp_fold": fold_stats}
        return run["stats"]
