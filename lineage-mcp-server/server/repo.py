"""数据访问层:检索/血缘遍历/影响分析的 SQL 实现。

BFS 每层为一次 IN 批量点查(设计方案 2 章);一律带 visited 防环(5.4)。
"""

import json

import pymysql
from dbutils.pooled_db import PooledDB
from rapidfuzz import fuzz

from . import config, vector

_pool: PooledDB | None = None


def connect():
    """池化连接(ping=1 取用前探活);服务只读,autocommit。"""
    global _pool
    if _pool is None:
        _pool = PooledDB(creator=pymysql, maxconnections=config.DB_POOL_SIZE,
                         blocking=True, ping=1, autocommit=True,
                         cursorclass=pymysql.cursors.DictCursor, **config.MYSQL)
    return _pool.connection()


# ---------- 检索(6.1 三级匹配) ----------

FUZZY_THRESHOLD = 85          # L2 rapidfuzz 术语模糊阈值(6.1)


def search_term(conn, keyword: str, context_domain: str | None = None) -> dict:
    """L1 精确 → L2 全文+rapidfuzz 模糊 → L3 向量兜底;
    命中候选经 table_closure 做血缘族归并(6.1/6.2)。"""
    candidates: list[dict] = []
    with conn.cursor() as cur:
        cur.execute(
            """SELECT term, caliber, domain, owner, ref_table, ref_column, certified
               FROM biz_glossary
               WHERE status='active' AND (term=%s OR JSON_CONTAINS(aliases, JSON_QUOTE(%s)))""",
            (keyword, keyword))
        for r in cur.fetchall():
            candidates.append({**r, "match_type": "term_exact", "confidence": 1.0})
        # tmp 表不参与检索:5.3 折叠后 tmp 不留边,元数据也不作为定位候选
        cur.execute(
            """SELECT full_name, comment, layer, domain, owner FROM table_metadata
               WHERE is_online=1 AND COALESCE(layer,'') <> 'tmp'
                 AND (table_name=%s OR full_name=%s)""",
            (keyword, keyword))
        for r in cur.fetchall():
            candidates.append({**r, "match_type": "table_exact", "confidence": 1.0})
        cur.execute(
            """SELECT c.full_name, c.column_name, c.comment, t.layer, t.domain, t.owner
               FROM column_metadata c JOIN table_metadata t ON t.full_name=c.full_name
               WHERE t.is_online=1 AND COALESCE(t.layer,'') <> 'tmp'
                 AND c.column_name=%s LIMIT 50""",
            (keyword,))
        for r in cur.fetchall():
            candidates.append({**r, "match_type": "column_exact", "confidence": 1.0})
        if not candidates:
            # L2a:ngram 全文检索(中文注释,6.1)
            cur.execute(
                """SELECT full_name, comment, layer, domain, owner,
                          MATCH(table_name, comment) AGAINST (%s) AS score
                   FROM table_metadata WHERE is_online=1
                     AND COALESCE(layer,'') <> 'tmp'
                     AND MATCH(table_name, comment) AGAINST (%s)
                   ORDER BY score DESC LIMIT 10""",
                (keyword, keyword))
            for r in cur.fetchall():
                candidates.append({**r, "match_type": "fulltext",
                                   "confidence": min(float(r.pop("score")), 0.99)})
            # L2b:rapidfuzz 术语模糊(term+aliases,阈值 85,confidence=score/100,6.1)
            cur.execute(
                """SELECT term, aliases, caliber, domain, owner,
                          ref_table, ref_column, certified
                   FROM biz_glossary WHERE status='active'""")
            for r in cur.fetchall():
                names = [r["term"]] + (json.loads(r["aliases"]) if r["aliases"] else [])
                score = max((fuzz.ratio(keyword, str(n)) for n in names if n),
                            default=0.0)
                if score >= FUZZY_THRESHOLD:
                    cand = {k: v for k, v in r.items() if k != "aliases"}
                    candidates.append({**cand, "match_type": "term_fuzzy",
                                       "confidence": round(score / 100, 4)})
    if not candidates:
        # L3:向量兜底(6.1/6.4);Chroma/embedding 不可用时静默返回空
        candidates = vector.search_similar(keyword)
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
    """同链合族(流转不是歧义),异链分族绝不猜(6.2)。

    候选两两经闭包判连通,并查集归族——连通关系经共同上下游传递
    (A、B 均与 C 同链则三者一族),与候选出现顺序无关;
    族内按 certified > 上下文域 > 层级 > 置信度选锚点,族间按锚点同序排序(6.3,
    热度信号 query_audit 接入前以置信度代位)。
    """
    layer_rank = {"ads": 4, "dws": 3, "dwd": 2, "dim": 2, "ods": 1}
    n = len(candidates)
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    tables = [c.get("ref_table") or c.get("full_name") for c in candidates]
    pair_cache: dict[tuple, bool] = {}
    for i in range(n):
        for j in range(i + 1, n):
            if not (tables[i] and tables[j]) or find(i) == find(j):
                continue
            key = tuple(sorted((tables[i], tables[j])))
            if key not in pair_cache:
                pair_cache[key] = (tables[i] == tables[j]
                                   or _connected(conn, tables[i], tables[j]))
            if pair_cache[key]:
                parent[find(j)] = find(i)

    groups: dict[int, list[dict]] = {}
    for i, c in enumerate(candidates):
        groups.setdefault(find(i), []).append(c)

    def _rank(c: dict) -> tuple:
        return (c.get("certified") or 0,
                1 if context_domain and c.get("domain") == context_domain else 0,
                layer_rank.get(c.get("layer") or "", 0),
                float(c.get("confidence") or 0))

    families = sorted((sorted(fam, key=_rank, reverse=True)
                       for fam in groups.values()),
                      key=lambda fam: _rank(fam[0]), reverse=True)
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


def get_lineage_path(conn, from_table: str, to_table: str) -> dict:
    """两点链路明细(8.1):闭包 O(1) 判连通 → 表级边 BFS 取一条最短路 → 逐跳明细。"""
    base = {"from": from_table, "to": to_table}
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM table_closure
                       WHERE ancestor=%s AND descendant=%s LIMIT 1""",
                    (from_table, to_table))
        if cur.fetchone() is None:
            return {**base, "connected": False, "hops": []}
    src = _node_id_of(conn, from_table)
    dst = _node_id_of(conn, to_table)
    if src is None or dst is None:
        return {**base, "connected": False, "hops": [], "error": "node_not_found"}

    # BFS 分层批量点查,parent 指针回溯最短路;visited 防环(5.4)
    parent: dict[int, tuple | None] = {src: None}
    frontier = [src]
    while frontier and dst not in parent:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT e.src_node_id, e.dst_node_id, e.transform_expr,
                           e.filter_cond, e.sql_id, e.confidence,
                           s.full_name AS src, d.full_name AS dst
                    FROM lineage_edge e
                    JOIN lineage_node s ON s.node_id=e.src_node_id
                    JOIN lineage_node d ON d.node_id=e.dst_node_id
                    WHERE e.src_node_id IN ({",".join(["%s"] * len(frontier))})
                      AND e.edge_level='table'""", frontier)
            rows = cur.fetchall()
        nxt = []
        for r in rows:
            if r["dst_node_id"] not in parent:
                parent[r["dst_node_id"]] = (r["src_node_id"], r)
                nxt.append(r["dst_node_id"])
        frontier = nxt
    if dst not in parent:                    # 闭包与边表短暂不一致时的护栏
        return {**base, "connected": False, "hops": [], "error": "path_not_found"}

    hops = []
    node = dst
    while parent[node] is not None:
        prev, r = parent[node]
        hops.append({"src": r["src"], "dst": r["dst"],
                     "transform_expr": r["transform_expr"],
                     "filter_cond": r["filter_cond"],
                     "sql_id": r["sql_id"], "confidence": r["confidence"]})
        node = prev
    hops.reverse()
    return {**base, "connected": True, "min_hops": len(hops), "hops": hops}


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
