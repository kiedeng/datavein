"""元数据同步(设计方案 4.1):直读 Hive Metastore 后端库(生产常用采集方式)。

只需一个 Metastore 后端 MySQL 的只读账号(METASTORE_DB_* 配置),
读 DBS/TBLS/TABLE_PARAMS/SDS/COLUMNS_V2/PARTITION_KEYS 标准表结构。
同步后写当日 schema_snapshot 供解析使用(5.2);消失的表置 is_online=0(5.7)。
"""

import json
import logging
from datetime import date, datetime

import pymysql

from . import config

log = logging.getLogger(__name__)

_TABLES_SQL = """
SELECT d.NAME AS db_name, t.TBL_ID, t.SD_ID, t.TBL_NAME, t.TBL_TYPE,
       cmt.PARAM_VALUE AS tbl_comment, own.PARAM_VALUE AS owner_param, t.OWNER AS tbl_owner
FROM TBLS t
JOIN DBS d ON d.DB_ID = t.DB_ID
LEFT JOIN TABLE_PARAMS cmt ON cmt.TBL_ID = t.TBL_ID AND cmt.PARAM_KEY = 'comment'
LEFT JOIN TABLE_PARAMS own ON own.TBL_ID = t.TBL_ID AND own.PARAM_KEY IN %(owner_keys)s
"""

_COLUMNS_SQL = """
SELECT s.SD_ID, c.COLUMN_NAME, c.TYPE_NAME, c.COMMENT, c.INTEGER_IDX
FROM SDS s JOIN COLUMNS_V2 c ON c.CD_ID = s.CD_ID
ORDER BY s.SD_ID, c.INTEGER_IDX
"""

_PARTITION_KEYS_SQL = """
SELECT TBL_ID, PKEY_NAME, PKEY_TYPE, PKEY_COMMENT, INTEGER_IDX
FROM PARTITION_KEYS ORDER BY TBL_ID, INTEGER_IDX
"""


def fetch_from_metastore() -> list[dict]:
    if not config.METASTORE_DB["host"]:
        raise RuntimeError("METASTORE_DB_HOST 未配置:元数据同步需要 Metastore 后端库只读账号")
    conn = pymysql.connect(**config.METASTORE_DB,
                           cursorclass=pymysql.cursors.DictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(_TABLES_SQL,
                        {"owner_keys": config.METASTORE_OWNER_PARAM_KEYS})
            tables = cur.fetchall()
            cur.execute(_COLUMNS_SQL)
            cols_by_sd: dict[int, list] = {}
            for c in cur.fetchall():
                cols_by_sd.setdefault(c["SD_ID"], []).append(c)
            cur.execute(_PARTITION_KEYS_SQL)
            pkeys_by_tbl: dict[int, list] = {}
            for p in cur.fetchall():
                pkeys_by_tbl.setdefault(p["TBL_ID"], []).append(p)
    finally:
        conn.close()

    result = []
    for t in tables:
        columns = [{"name": c["COLUMN_NAME"], "type": c["TYPE_NAME"],
                    "comment": c["COMMENT"]}
                   for c in cols_by_sd.get(t["SD_ID"], [])]
        # 分区键(dt 等)追加在普通列之后:SQL 过滤条件大量使用,schema 快照必须包含
        columns += [{"name": p["PKEY_NAME"], "type": p["PKEY_TYPE"],
                     "comment": p["PKEY_COMMENT"]}
                    for p in pkeys_by_tbl.get(t["TBL_ID"], [])]
        result.append({
            "full_name": f'{t["db_name"]}.{t["TBL_NAME"]}',
            "table_type": "view" if "VIEW" in (t["TBL_TYPE"] or "") else "table",
            "layer": config.derive_layer(t["TBL_NAME"]),
            "domain": t["db_name"],
            "comment": t["tbl_comment"],
            "owner": t["owner_param"] or t["tbl_owner"],
            "columns": columns,
        })
    return result


def sync(conn):
    tables = fetch_from_metastore()
    today, now = date.today(), datetime.now()
    seen = set()
    missing_comment = 0
    with conn.cursor() as cur:
        for t in tables:
            seen.add(t["full_name"])
            db_name, table_name = t["full_name"].split(".", 1)
            cur.execute(
                """REPLACE INTO table_metadata
                   (full_name, db_name, table_name, table_type, layer, domain,
                    comment, owner, is_online, synced_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s)""",
                (t["full_name"], db_name, table_name, t["table_type"],
                 t["layer"], t["domain"], t["comment"], t["owner"], now))
            for c in t["columns"]:
                if not (c.get("comment") or "").strip():
                    missing_comment += 1        # 注释缺失队列数据源(6.4)
                cur.execute(
                    """REPLACE INTO column_metadata
                       (full_name, column_name, data_type, comment)
                       VALUES (%s,%s,%s,%s)""",
                    (t["full_name"], c["name"], c["type"], c.get("comment")))
            cur.execute(
                """REPLACE INTO schema_snapshot (full_name, snap_date, columns_json)
                   VALUES (%s,%s,%s)""",
                (t["full_name"], today,
                 json.dumps([{"name": c["name"], "type": c["type"]}
                             for c in t["columns"]], ensure_ascii=False)))
        # 消失的表下线,不删(5.7 僵尸治理)
        if seen:
            cur.execute(
                "UPDATE table_metadata SET is_online=0 WHERE full_name NOT IN ({})"
                .format(",".join(["%s"] * len(seen))), tuple(seen))
    conn.commit()
    log.info("metadata sync: %s tables, %s columns missing comment",
             len(tables), missing_comment)
    return {"tables": len(tables), "columns_missing_comment": missing_comment}
