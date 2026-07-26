"""血缘边入库:节点 get-or-create + 按 sql_id 先删后插(单事务,设计方案 4.3/5.3)。"""

from datetime import datetime

from .db import tx
from .models import ColumnRef, LineageEdge, ParseResult


def _node_id(cur, ref: ColumnRef, cache: dict) -> int:
    key = (ref.table, ref.column)
    if key in cache:
        return cache[key]
    cur.execute("SELECT node_id FROM lineage_node WHERE full_name=%s AND column_name=%s",
                key)
    row = cur.fetchone()
    if row:
        cache[key] = row["node_id"]
    else:
        cur.execute(
            "INSERT INTO lineage_node (node_type, full_name, column_name) VALUES (%s,%s,%s)",
            ("column" if ref.column else "table", ref.table, ref.column))
        cache[key] = cur.lastrowid
    return cache[key]


def replace_edges_for_sql(conn, sql_id: int, result: ParseResult,
                          node_cache: dict | None = None):
    """该 sql_id 的旧边全删、新边全插,同一事务;manual 边不动(5.3)。"""
    cache = node_cache if node_cache is not None else {}
    confidence = "high" if result.status == "success" else "medium"   # 4.6
    now = datetime.now()
    with tx(conn) as cur:
        cur.execute("DELETE FROM lineage_edge WHERE sql_id=%s AND src_type='sql'",
                    (sql_id,))
        for e in result.edges:
            cur.execute(
                """INSERT INTO lineage_edge
                   (src_node_id, dst_node_id, edge_level, sql_id, src_type, confidence,
                    transform_expr, filter_cond, join_cond, is_derived, is_self_loop, updated_at)
                   VALUES (%s,%s,%s,%s,'sql',%s,%s,%s,%s,%s,%s,%s)""",
                (_node_id(cur, e.src, cache), _node_id(cur, e.dst, cache),
                 e.edge_level, sql_id, confidence,
                 e.transform_expr, e.filter_cond, e.join_cond,
                 int(e.is_derived), int(e.is_self_loop), now))
        cur.execute(
            """UPDATE sql_repository
               SET parse_status=%s, parse_reason=%s, parse_msg=%s,
                   parse_cost_ms=%s, updated_at=%s WHERE sql_id=%s""",
            (result.status, result.reason, result.message[:2000],
             result.cost_ms, now, sql_id))
