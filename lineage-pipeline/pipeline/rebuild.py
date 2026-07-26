"""流水线编排:全量重建与增量触发两种模式(设计方案 5.1)。"""

import json
import logging

from . import closure, config, db
from .loader import replace_edges_for_sql
from .parser.core import parse_sql

log = logging.getLogger(__name__)


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


def full_rebuild():
    """每日全量:重解析全部 active SQL → 重建边/闭包 → 校验后原子切换(4.3)。

    当前骨架为顺序解析;上线前按 PARALLEL_WORKERS 换 ProcessPoolExecutor,
    并对单条 SQL 施加 PARSE_TIMEOUT_SECONDS 超时(超时 → degraded/timeout)。
    """
    conn = db.connect()
    schema = load_schema_snapshot(conn)
    node_cache: dict = {}
    stats = {"success": 0, "degraded": 0, "failed": 0}
    with conn.cursor() as cur:
        cur.execute("""SELECT sql_id, sql_text, dialect, target_table
                       FROM sql_repository WHERE is_active=1""")
        jobs = cur.fetchall()
    for job in jobs:
        result = parse_sql(job["sql_text"],
                           dialect=job["dialect"] or config.SQL_DIALECT_DEFAULT,
                           schema=schema)
        replace_edges_for_sql(conn, job["sql_id"], result, node_cache)
        stats[result.status] += 1
    log.info("parse done: %s", stats)

    rows = closure.rebuild_closure_shadow(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM table_closure")
        old = cur.fetchone()["c"]
    # 校验:闭包行数波动 >10% 视为异常,不切换,保持旧版血缘继续服务(4.3)
    if old > 0 and abs(rows - old) / old > 0.10:
        log.error("closure row delta too large (%s -> %s), swap aborted", old, rows)
        return {"parse": stats, "closure": "swap_aborted"}
    db.swap_shadow(conn, "table_closure")
    log.info("closure swapped: %s rows", rows)
    return {"parse": stats, "closure_rows": rows}


def incremental(sql_id: int):
    """发布钩子触发:单条重解析 → 换边 → 闭包区域修补,分钟级生效(5.1)。"""
    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("""SELECT sql_id, sql_text, dialect, target_table
                       FROM sql_repository WHERE sql_id=%s""", (sql_id,))
        job = cur.fetchone()
    if not job:
        raise ValueError(f"sql_id {sql_id} not found")
    result = parse_sql(job["sql_text"],
                       dialect=job["dialect"] or config.SQL_DIALECT_DEFAULT,
                       schema=load_schema_snapshot(conn))
    replace_edges_for_sql(conn, sql_id, result)
    outcome = closure.incremental_patch(conn, result.target_table or job["target_table"])
    if outcome == "degrade_full_rebuild":
        # 核心枢纽表:增量不划算,转异步全量并标注数据时点(2.2)
        log.warning("closure patch degraded for %s, full rebuild required",
                    result.target_table)
    return {"parse": result.status, "closure": outcome}
