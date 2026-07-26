import json
import logging
from contextlib import contextmanager
from datetime import datetime

import pymysql
from dbutils.pooled_db import PooledDB

from . import config

log = logging.getLogger(__name__)

_pool: PooledDB | None = None


def connect():
    """池化连接(ping=1:取用前探活,自动重连断连)。"""
    global _pool
    if _pool is None:
        _pool = PooledDB(creator=pymysql, maxconnections=config.DB_POOL_SIZE,
                         blocking=True, ping=1, autocommit=False,
                         cursorclass=pymysql.cursors.DictCursor, **config.MYSQL)
    return _pool.connection()


@contextmanager
def tx(conn):
    """单事务边界:血缘边先删后插必须在同一事务内(设计方案 4.3)。"""
    try:
        yield conn.cursor()
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def swap_shadow(conn, table: str):
    """影子表原子切换:table_shadow -> table,旧表保留为 table_prev 一代可回滚(4.3)。"""
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}_prev")
        cur.execute(
            f"RENAME TABLE {table} TO {table}_prev, {table}_shadow TO {table}")
    conn.commit()


@contextmanager
def record_run(conn, run_type: str):
    """pipeline_run 落库:运行状态是监控告警的数据源(12 章)。

    用法:with record_run(conn, "full") as run: ...; run["stats"] = {...}
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_run (run_type, status, started_at) VALUES (%s,'running',%s)",
            (run_type, datetime.now()))
        run_id = cur.lastrowid
    conn.commit()
    run: dict = {"run_id": run_id, "stats": {}}
    try:
        yield run
        status = run.get("status", "success")
    except Exception as e:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE pipeline_run SET status='failed', error_msg=%s, finished_at=%s
                   WHERE run_id=%s""",
                (str(e)[:2000], datetime.now(), run_id))
        conn.commit()
        raise
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE pipeline_run SET status=%s, stats_json=%s, finished_at=%s
               WHERE run_id=%s""",
            (status, json.dumps(run["stats"], ensure_ascii=False, default=str),
             datetime.now(), run_id))
    conn.commit()
