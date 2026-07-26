"""元数据同步(设计方案 4.1):每日全量拉取 Metastore → diff 更新。

对接点(M1 落地时实现):
- Hive Metastore thrift(HIVE_METASTORE_URI)或 information_schema 直查
- 消失的表置 is_online=0 而非删除(血缘历史保留)
- 新增无注释字段进"注释缺失队列"按 owner 分发(6.4 注释质量治理共用)
- 同步完成后写当日 schema_snapshot(5.2 解析期快照)
"""

from datetime import date, datetime


def fetch_from_metastore() -> list[dict]:
    """返回 [{full_name, table_type, layer, domain, comment, owner, columns:[{name,type,comment}]}]。"""
    raise NotImplementedError("M1: 对接 Hive Metastore / information_schema")


def sync(conn):
    tables = fetch_from_metastore()
    today, now = date.today(), datetime.now()
    seen = set()
    with conn.cursor() as cur:
        for t in tables:
            seen.add(t["full_name"])
            db_name, table_name = t["full_name"].split(".", 1)
            cur.execute(
                """REPLACE INTO table_metadata
                   (full_name, db_name, table_name, table_type, layer, domain,
                    comment, owner, is_online, synced_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s)""",
                (t["full_name"], db_name, table_name, t.get("table_type", "table"),
                 t.get("layer"), t.get("domain"), t.get("comment"), t.get("owner"), now))
            for c in t["columns"]:
                cur.execute(
                    """REPLACE INTO column_metadata
                       (full_name, column_name, data_type, comment) VALUES (%s,%s,%s,%s)""",
                    (t["full_name"], c["name"], c["type"], c.get("comment")))
            cur.execute(
                """REPLACE INTO schema_snapshot (full_name, snap_date, columns_json)
                   VALUES (%s,%s,JSON_ARRAY())""",  # M1: 填充有序列数组
                (t["full_name"], today))
        # 消失的表下线,不删(5.7 僵尸治理)
        if seen:
            cur.execute(
                "UPDATE table_metadata SET is_online=0 WHERE full_name NOT IN ({})"
                .format(",".join(["%s"] * len(seen))), tuple(seen))
    conn.commit()
