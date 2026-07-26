"""血缘边入库。

增量路径:按 sql_id 先删后插,单事务(4.3/5.3),manual 边不动。
全量路径:批量写 lineage_edge_shadow,由 rebuild 编排校验后原子切换。
"""

from datetime import datetime

from .db import tx
from .models import ColumnRef, LineageEdge, ParseResult

_EDGE_INSERT = """INSERT INTO {table}
   (src_node_id, dst_node_id, edge_level, sql_id, src_type, confidence,
    transform_expr, filter_cond, join_cond, is_derived, is_self_loop, updated_at)
   VALUES (%s,%s,%s,%s,'sql',%s,%s,%s,%s,%s,%s,%s)"""


def preload_node_cache(conn) -> dict:
    """节点表整体载入内存(130 万行,约数百 MB 内,全量重建专用)。"""
    cache: dict = {}
    with conn.cursor() as cur:
        cur.execute("SELECT node_id, full_name, column_name FROM lineage_node")
        for r in cur.fetchall():
            cache[(r["full_name"], r["column_name"])] = r["node_id"]
    return cache


def _node_id(cur, ref: ColumnRef, cache: dict) -> int:
    key = (ref.table, ref.column)
    if key not in cache:
        cur.execute(
            "INSERT IGNORE INTO lineage_node (node_type, full_name, column_name) VALUES (%s,%s,%s)",
            ("column" if ref.column else "table", ref.table, ref.column))
        if cur.lastrowid:
            cache[key] = cur.lastrowid
        else:                                  # 并发插入撞 UNIQUE,回查
            cur.execute(
                "SELECT node_id FROM lineage_node WHERE full_name=%s AND column_name=%s", key)
            cache[key] = cur.fetchone()["node_id"]
    return cache[key]


def _edge_rows(cur, sql_id: int, result: ParseResult, cache: dict, now) -> list[tuple]:
    confidence = "high" if result.status == "success" else "medium"   # 4.6
    return [(_node_id(cur, e.src, cache), _node_id(cur, e.dst, cache),
             e.edge_level, sql_id, confidence, e.transform_expr, e.filter_cond,
             e.join_cond, int(e.is_derived), int(e.is_self_loop), now)
            for e in result.edges]


def replace_edges_for_sql(conn, sql_id: int, result: ParseResult,
                          node_cache: dict | None = None):
    """增量:该 sql_id 旧边全删、新边全插,同一事务;manual 边不清理(5.3)。"""
    cache = node_cache if node_cache is not None else {}
    now = datetime.now()
    with tx(conn) as cur:
        cur.execute("DELETE FROM lineage_edge WHERE sql_id=%s AND src_type='sql'",
                    (sql_id,))
        rows = _edge_rows(cur, sql_id, result, cache, now)
        if rows:
            cur.executemany(_EDGE_INSERT.format(table="lineage_edge"), rows)
        _update_sql_status(cur, sql_id, result, now)


def bulk_insert_shadow(conn, parsed: list[tuple[int, ParseResult]],
                       node_cache: dict, batch: int = 5000):
    """全量:全部解析结果批量写影子表(百万级边,executemany 分批)。"""
    now = datetime.now()
    rows: list[tuple] = []
    with conn.cursor() as cur:
        for sql_id, result in parsed:
            rows.extend(_edge_rows(cur, sql_id, result, node_cache, now))
            _update_sql_status(cur, sql_id, result, now)
            while len(rows) >= batch:
                cur.executemany(_EDGE_INSERT.format(table="lineage_edge_shadow"),
                                rows[:batch])
                rows = rows[batch:]
        if rows:
            cur.executemany(_EDGE_INSERT.format(table="lineage_edge_shadow"), rows)
    conn.commit()


def _update_sql_status(cur, sql_id: int, result: ParseResult, now):
    cur.execute(
        """UPDATE sql_repository
           SET parse_status=%s, parse_reason=%s, parse_msg=%s,
               parse_cost_ms=%s, updated_at=%s WHERE sql_id=%s""",
        (result.status, result.reason, result.message[:2000],
         result.cost_ms, now, sql_id))
