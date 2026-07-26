"""审计落库(设计方案 4.5/11 章):每次取数请求必留痕,拒绝也留痕。"""

from datetime import datetime

import pymysql
from dbutils.pooled_db import PooledDB

from . import config

_pool: PooledDB | None = None


def _connect():
    global _pool
    if _pool is None:
        _pool = PooledDB(creator=pymysql, maxconnections=5, blocking=True, ping=1,
                         autocommit=True, cursorclass=pymysql.cursors.DictCursor,
                         **config.PLATFORM_MYSQL)
    return _pool.connection()


def record(user_ctx: dict, question: str, sql_channel: str,
           final_sql: str = "", metric_version: str = "",
           row_policy_applied: str = "", result_rows: int = 0, cost_ms: int = 0,
           status: str = "ok", refuse_reason: str = "",
           is_customer_query: bool = False):
    conn = _connect()
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO query_audit
               (session_id, user_id, channel, question, final_sql, sql_channel,
                metric_version, row_policy_applied, result_rows, cost_ms,
                status, refuse_reason, is_customer_query, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (user_ctx.get("session_id", ""), user_ctx.get("user_id", "unknown"),
             user_ctx.get("channel", "gateway"), question[:2000], final_sql,
             sql_channel, metric_version, row_policy_applied, result_rows,
             cost_ms, status, refuse_reason[:64], int(is_customer_query),
             datetime.now()))
