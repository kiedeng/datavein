"""数据访问层:检索/血缘遍历/影响分析的 SQL 实现。

BFS 每层为一次 IN 批量点查(设计方案 2 章);一律带 visited 防环(5.4)。
"""

import pymysql
from dbutils.pooled_db import PooledDB

from . import config

_pool: PooledDB | None = None


def connect():
    """池化连接(ping=1 取用前探活);服务只读,autocommit。"""
    global _pool
    if _pool is None:
        _pool = PooledDB(creator=pymysql, maxconnections=config.DB_POOL_SIZE,
                         blocking=True, ping=1, autocommit=True,
                         cursorclass=pymysql.cursors.DictCursor, **config.MYSQL)
    return _pool.connection()


# ---------- 检索(6.1 三级匹配;L3 向量在 M2 接 Chroma) ----------

def search_term(conn, keyword: str, context_domain: str | None = None) -> dict:
    """L1 精确 → L2 模糊;命中候选经 table_closure 做血缘族归并(6.2)。"""
    candidates: list[dict] = []
    with conn.cursor() as cur:
        cur.execute(
            """SELECT term, caliber, domain, owner, ref_table, ref_column, certified
               FROM biz_glossary
               WHERE status='active' AND (term=%s OR JSON_CONTAINS(aliases, JSON_QUOTE(%s)))""",
            (keyword, keyword))
        for r in cur.fetchall():
            candidates.append({**r, "match_type": "term_exact", "confidence": 1.0})
        cur.execute(
            """SELECT full_name, comment, layer, domain, owner FROM table_metadata
               WHERE is_online=1 AND (table_name=%s OR full_name=%s)""",
            (keyword, keyword))
        for r in cur.fetchall():
            candidates.append({**r, "match_type": "table_exact", "confidence": 1.0})
        cur.execute(
            """SELECT c.full_name, c.column_name, c.comment, t.layer, t.domain, t.owner
               FROM column_metadata c JOIN table_metadata t ON t.full_name=c.full_name
               WHERE t.is_online=1 AND c.column_name=%s LIMIT 50""",
            (keyword,))
        for r in cur.fetchall():
            candidates.append({**r, "match_type": "column_exact", "confidence": 1.0})
        if not candidates:
            # L2:ngram 全文(中文注释);rapidfuzz 术语模糊与 L3 向量为 M2 接入点
            cur.execute(
                """SELECT full_name, comment, layer, domain, owner,
                          MATCH(table_name, comment) AGAINST (%s) AS score
                   FROM table_metadata WHERE is_online=1
                     AND MATCH(table_name, comment) AGAINST (%s)
                   ORDER BY score DESC LIMIT 10""",
                (keyword, keyword))
            for r in cur.fetchall():
                candidates.append({**r, "match_type": "fulltext",
                                   "confidence": min(float(r.pop("score")), 0.99)})
    return _merge_families(conn, candidates, context_domain)


def _connected(conn, t1: str, t2: str) -> bool:
    """闭包表 O(1) 判连通(6.2 同链/异链判定)。"""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT 1 FROM table_closure
               WHERE (ancestor=%s AND descendant=%s) OR (ancestor=%s AND descendant=%s)
               LIMIT 1""", (t1, t2, t2, t1))
        return cur.fetchone() is not None


def _merge_families(conn, candidates: list[dict], context_domain: str | None) -> dict:
    """同链合族(流转不是歧义),异链分族绝不猜;族内按 certified>层级>热度选锚点(6.2/6.3)。"""
    layer_rank = {"ads": 4, "dws": 3, "dwd": 2, "dim": 2, "ods": 1}
    families: list[list[dict]] = []
    for c in candidates:
        table = c.get("ref_table") or c.get("full_name")
        placed = False
        for fam in families:
            anchor_table = fam[0].get("ref_table") or fam[0].get("full_name")
            if table and anchor_table and (table == anchor_table
                                           or _connected(conn, table, anchor_table)):
                fam.append(c)
                placed = True
                break
        if not placed:
            families.append([c])
    for fam in families:
        fam.sort(key=lambda c: (c.get("certified") or 0,
                                layer_rank.get(c.get("layer", ""), 0),
                                1 if context_domain and c.get("domain") == context_domain else 0),
                 reverse=True)
    result = [{"anchor": fam[0], "members": fam[1:]} for fam in families]
    return {"families": result, "ambiguous": len(result) > 1}


# ---------- 血缘遍历(2 章 BFS;5.4 visited 防环) ----------

def _node_id_of(conn, full_name: str, column: str = "") -> int | None:
    with conn.cursor() as cur:
        cur.execute("SELECT node_id FROM lineage_node WHERE full_name=%s AND column_name=%s",
                    (full_name, column))
        row = cur.fetchone()
        return row["node_id"] if row else None


def get_lineage(conn, full_name: str, column: str = "", direction: str = "upstream",
                depth: int = 3, edge_level: str = "table") -> dict:
    depth = min(depth, config.MAX_DEPTH)
    start = _node_id_of(conn, full_name, column if edge_level == "column" else "")
    if start is None:
        return {"error": "node_not_found", "node": f"{full_name}.{column}".rstrip(".")}

    join_col, from_col = (("dst_node_id", "src_node_id") if direction == "upstream"
                          else ("src_node_id", "dst_node_id"))
    visited, frontier, hops, truncated = {start}, [start], [], False
    for level in range(1, depth + 1):
        if not frontier:
            break
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT e.{join_col} AS cur_id, e.{from_col} AS next_id,
                           e.transform_expr, e.filter_cond, e.join_cond,
                           e.confidence, e.src_type, e.sql_id, e.is_self_loop,
                           n.full_name, n.column_name
                    FROM lineage_edge e JOIN lineage_node n ON n.node_id=e.{from_col}
                    WHERE e.{join_col} IN ({",".join(["%s"] * len(frontier))})
                      AND e.edge_level=%s""",
                (*frontier, edge_level))
            rows = cur.fetchall()
        nxt = []
        for r in rows:
            if len(visited) >= config.MAX_NODES:
                truncated = True
                break
            hops.append({"level": level, **{k: r[k] for k in
                                            ("full_name", "column_name", "transform_expr",
                                             "filter_cond", "join_cond", "confidence",
                                             "src_type", "sql_id", "is_self_loop")}})
            if r["next_id"] not in visited:
                visited.add(r["next_id"])
                nxt.append(r["next_id"])
        frontier = nxt
    return {"node": {"full_name": full_name, "column": column},
            "direction": direction, "edge_level": edge_level, "depth": depth,
            "hops": hops, "truncated": truncated,
            "hint": "结果超上限已截断,建议减小 depth 或按表级聚合查看" if truncated else ""}


def impact_analysis(conn, full_name: str, group_by: str = "table",
                    page: int = 1, page_size: int = 100) -> dict:
    """查闭包表一跳出全量下游(S2);大结果分页,支持按任务聚合(8.1)。"""
    offset = (page - 1) * page_size
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM table_closure WHERE ancestor=%s",
                    (full_name,))
        total = cur.fetchone()["c"]
        if group_by == "task":
            cur.execute(
                """SELECT sr.task_id, sr.task_name, sr.owner,
                          COUNT(DISTINCT tc.descendant) AS affected_tables
                   FROM table_closure tc
                   JOIN sql_repository sr ON sr.target_table = tc.descendant AND sr.is_active=1
                   WHERE tc.ancestor=%s
                   GROUP BY sr.task_id, sr.task_name, sr.owner
                   ORDER BY affected_tables DESC LIMIT %s OFFSET %s""",
                (full_name, page_size, offset))
        else:
            cur.execute(
                """SELECT tc.descendant, tc.min_hops, tc.path_cnt,
                          tm.layer, tm.domain, tm.owner
                   FROM table_closure tc
                   LEFT JOIN table_metadata tm ON tm.full_name = tc.descendant
                   WHERE tc.ancestor=%s
                   ORDER BY tc.min_hops, tc.descendant LIMIT %s OFFSET %s""",
                (full_name, page_size, offset))
        rows = cur.fetchall()
    return {"ancestor": full_name, "total_downstream": total,
            "group_by": group_by, "page": page, "items": rows}


def get_table_info(conn, full_name: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM table_metadata WHERE full_name=%s", (full_name,))
        meta = cur.fetchone()
        if not meta:
            return {"error": "table_not_found", "full_name": full_name}
        cur.execute("""SELECT column_name, data_type, comment, is_sensitive, sens_level
                       FROM column_metadata WHERE full_name=%s""", (full_name,))
        columns = cur.fetchall()
        cur.execute("""SELECT sql_id, task_id, task_name, owner, parse_status
                       FROM sql_repository WHERE target_table=%s AND is_active=1""",
                    (full_name,))
        tasks = cur.fetchall()
    return {"meta": meta, "columns": columns, "producing_tasks": tasks}
