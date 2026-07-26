"""SQL 收集(设计方案 4.2):调度平台 API 每日增量同步 + 发布钩子即时收集。

制度前提(M0):生产 ETL SQL 统一入调度平台,动态拼接必须归档渲染后版本,
上线强制登记 target_table/owner/domain,未登记不予发布。
"""

import hashlib
from datetime import datetime


def fetch_from_scheduler(since: datetime | None = None) -> list[dict]:
    """返回 [{task_id, task_name, node_seq, target_table, sql_text, dialect, owner, domain}]。"""
    raise NotImplementedError("M1: 对接调度平台 API(SCHEDULER_API_BASE)")


def upsert_sql(conn, item: dict) -> tuple[int, bool]:
    """入库并返回 (sql_id, hash是否变化)。hash 变化才需要触发增量解析(5.1)。"""
    sql_hash = hashlib.sha256(item["sql_text"].encode()).hexdigest()
    with conn.cursor() as cur:
        cur.execute("SELECT sql_id, sql_hash FROM sql_repository WHERE task_id=%s AND node_seq=%s",
                    (item["task_id"], item["node_seq"]))
        row = cur.fetchone()
        if row and row["sql_hash"] == sql_hash:
            return row["sql_id"], False
        if row:
            cur.execute(
                """UPDATE sql_repository SET sql_text=%s, sql_hash=%s, target_table=%s,
                   parse_status='pending', updated_at=%s WHERE sql_id=%s""",
                (item["sql_text"], sql_hash, item["target_table"],
                 datetime.now(), row["sql_id"]))
            sql_id = row["sql_id"]
        else:
            cur.execute(
                """INSERT INTO sql_repository
                   (task_id, task_name, node_seq, target_table, sql_text, sql_hash,
                    dialect, owner, domain, parse_status, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s)""",
                (item["task_id"], item["task_name"], item["node_seq"],
                 item["target_table"], item["sql_text"], sql_hash,
                 item.get("dialect"), item.get("owner"), item.get("domain"),
                 datetime.now()))
            sql_id = cur.lastrowid
    conn.commit()
    return sql_id, True
