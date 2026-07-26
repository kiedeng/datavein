"""SQL 收集(设计方案 4.2):调度平台通用 REST 适配器。

平台差异通过 SCHEDULER_FIELD_MAP(平台字段名 -> 标准字段名)与端点配置吸收,
换调度平台只改配置不改代码。分页拉取,带重试与指数退避。

制度前提(M0):生产 ETL SQL 统一入调度平台,动态拼接必须归档渲染后版本,
上线强制登记 target_table/owner/domain,未登记不予发布。
"""

import hashlib
import logging
import time
from datetime import datetime

import requests

from . import config

log = logging.getLogger(__name__)


def _get_with_retry(url: str, params: dict, attempts: int = 4) -> dict:
    headers = {"Authorization": f"Bearer {config.SCHEDULER_API_TOKEN}"}
    for i in range(attempts):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if i == attempts - 1:
                raise
            wait = 2 ** (i + 1)
            log.warning("scheduler api retry in %ss: %s", wait, e)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def fetch_from_scheduler(since: datetime | None = None) -> list[dict]:
    if not config.SCHEDULER_API_BASE:
        raise RuntimeError("SCHEDULER_API_BASE 未配置:SQL 收集需要调度平台 API")
    url = config.SCHEDULER_API_BASE.rstrip("/") + config.SCHEDULER_SQL_ENDPOINT
    fmap = config.SCHEDULER_FIELD_MAP
    items, page = [], 1
    while True:
        params = {"page": page, "page_size": config.SCHEDULER_PAGE_SIZE}
        if since:
            params["updated_since"] = since.isoformat()
        data = _get_with_retry(url, params)
        batch = data.get("items", data if isinstance(data, list) else [])
        if not batch:
            break
        for raw in batch:
            items.append({std: raw.get(platform_key)
                          for std, platform_key in fmap.items()})
        if len(batch) < config.SCHEDULER_PAGE_SIZE:
            break
        page += 1
    # 未登记 target_table 的按制度不应出现;出现即数据问题,记日志并跳过
    valid = [i for i in items if i.get("target_table") and i.get("sql_text")]
    if len(valid) < len(items):
        log.error("scheduler returned %s items missing target_table/sql_text (制度漏洞,需追责登记)",
                  len(items) - len(valid))
    return valid


def upsert_sql(conn, item: dict) -> tuple[int, bool]:
    """入库并返回 (sql_id, hash是否变化)。hash 变化才触发增量解析(5.1)。"""
    sql_hash = hashlib.sha256(item["sql_text"].encode()).hexdigest()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT sql_id, sql_hash FROM sql_repository WHERE task_id=%s AND node_seq=%s",
            (item["task_id"], item.get("node_seq", 0)))
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
                (item["task_id"], item.get("task_name"), item.get("node_seq", 0),
                 item["target_table"], item["sql_text"], sql_hash,
                 item.get("dialect"), item.get("owner"), item.get("domain"),
                 datetime.now()))
            sql_id = cur.lastrowid
    conn.commit()
    return sql_id, True
