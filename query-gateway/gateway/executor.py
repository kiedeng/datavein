"""只读执行器(设计方案 11 章):数仓凭据唯一持有点。

红线(硬约束,不依赖 LLM 自觉):白名单只放行单条 SELECT;敏感表黑名单;
强制 LIMIT;超时;并发上限;结果按 masked_columns 掩码。
"""

import threading
import time

import pymysql
from dbutils.pooled_db import PooledDB
from sqlglot import exp, parse_one

from . import config

_pool: PooledDB | None = None
_semaphore = threading.BoundedSemaphore(config.MAX_CONCURRENCY)


class ExecutionRejected(ValueError):
    """静态校验拒绝;写审计并告警(11 章)。"""


def _connect():
    global _pool
    if _pool is None:
        _pool = PooledDB(creator=pymysql, maxconnections=config.MAX_CONCURRENCY,
                         blocking=True, ping=1, autocommit=True,
                         cursorclass=pymysql.cursors.DictCursor,
                         read_timeout=config.QUERY_TIMEOUT_MS // 1000 + 5,
                         **config.WAREHOUSE_MYSQL)
    return _pool.connection()


def validate_select_only(sql: str) -> exp.Select:
    """单条 SELECT + 黑名单 + LIMIT 存在性;违规抛 ExecutionRejected。"""
    try:
        trees = [t for t in
                 __import__("sqlglot").parse(sql, dialect=config.WAREHOUSE_DIALECT)
                 if t is not None]
    except Exception as e:
        raise ExecutionRejected(f"SQL 无法解析: {str(e)[:200]}")
    if len(trees) != 1:
        raise ExecutionRejected("仅允许单条语句")
    tree = trees[0]
    if not isinstance(tree, exp.Select):
        raise ExecutionRejected(f"仅放行 SELECT,收到 {type(tree).__name__};已审计告警")
    for t in tree.find_all(exp.Table):
        full = f'{t.text("db")}.{t.name}' if t.text("db") else t.name
        if full in config.BLACKLIST_TABLES:
            raise ExecutionRejected(f"表 {full} 在敏感黑名单,拒绝执行")
    if tree.args.get("limit") is None:
        raise ExecutionRejected("缺少 LIMIT(编译器/校验层必须注入)")
    return tree


def _mask(value):
    s = "" if value is None else str(value)
    return (s[:1] + "***") if s else s


def execute(sql: str, masked_columns: list[str] | None = None) -> dict:
    """校验 → EXPLAIN 预检 → 执行 → 掩码。返回 {columns, rows, row_count, cost_ms}。"""
    validate_select_only(sql)
    masked = set(masked_columns or [])
    t0 = time.monotonic()
    with _semaphore:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN {sql}")            # 预检:语义/权限错误在此暴露
            if config.WAREHOUSE_DIALECT == "mysql":
                cur.execute(
                    f"SET SESSION max_execution_time = {config.QUERY_TIMEOUT_MS}")
            cur.execute(sql)
            rows = cur.fetchall()
    for row in rows:
        for col in masked & row.keys():
            row[col] = _mask(row[col])
    return {"columns": list(rows[0].keys()) if rows else [],
            "rows": rows, "row_count": len(rows),
            "cost_ms": int((time.monotonic() - t0) * 1000)}
