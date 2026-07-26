from contextlib import contextmanager

import pymysql

from . import config


def connect():
    return pymysql.connect(**config.MYSQL, autocommit=False,
                           cursorclass=pymysql.cursors.DictCursor)


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
