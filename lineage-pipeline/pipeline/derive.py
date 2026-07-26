"""临时表穿透折叠(设计方案 5.3)。

同任务临时表链 A→tmp→B 折叠为合成边 A→B:表级与字段级分别折叠,字段级按 tmp
中转列自然衔接;支持多级 tmp 链(tmp1→tmp2,迭代穿透,visited 防环 5.4)。
合成边 is_derived=1,transform_expr 记录外层(下游段)表达式并附注
「经 tmp 折叠:内层表达式」,sql_id 取下游段,confidence 取链上最低(4.6)。
折叠完成后删除 tmp 相关边;tmp 节点保留无妨(「tmp 不建正式节点」的语义由
「无边」体现,残留节点归僵尸治理 5.7 清理)。

tmp 表判定:table_metadata.layer='tmp',或库名以 tmp 开头,
或表名匹配 config.TMP_TABLE_PATTERNS(前缀/后缀规则)。
"""

import logging
from collections import defaultdict
from datetime import datetime

from . import config

log = logging.getLogger(__name__)

_CONF_RANK = {"low": 0, "medium": 1, "high": 2}

_EDGE_SELECT = """
SELECT e.edge_id, e.src_node_id, e.dst_node_id, e.edge_level, e.sql_id,
       e.src_type, e.confidence, e.transform_expr, e.filter_cond, e.join_cond,
       s.full_name AS src_table, s.column_name AS src_col,
       d.full_name AS dst_table, d.column_name AS dst_col
FROM {table} e
JOIN lineage_node s ON s.node_id = e.src_node_id
JOIN lineage_node d ON d.node_id = e.dst_node_id
"""

_SYN_INSERT = """INSERT INTO {table}
   (src_node_id, dst_node_id, edge_level, sql_id, src_type, confidence,
    transform_expr, filter_cond, join_cond, is_derived, is_self_loop, updated_at)
   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s)"""


def is_tmp_table(full_name: str, tmp_layer_tables: set[str]) -> bool:
    """tmp 表三重判定(5.3):元数据 layer / 库名前缀 / 表名命名规则。"""
    if full_name in tmp_layer_tables:
        return True
    db, _, table = full_name.partition(".")
    if db.startswith("tmp"):
        return True
    return any(table.endswith(p) if p.startswith("_") else table.startswith(p)
               for p in config.TMP_TABLE_PATTERNS)


def _min_confidence(chain) -> str:
    return min((e["confidence"] or "high" for e in chain),
               key=lambda v: _CONF_RANK.get(v, 1))


def _synthesize(chain: tuple) -> dict:
    """整条链 A→tmp…→B → 一条合成边。外层表达式=下游段,内层表达式拼接附注。"""
    down = chain[-1]
    inner = " ; ".join(e["transform_expr"] for e in chain[:-1] if e["transform_expr"])
    return {
        "src_node_id": chain[0]["src_node_id"], "dst_node_id": down["dst_node_id"],
        "src_key": chain[0]["src_key"], "dst_key": down["dst_key"],
        "edge_level": down["edge_level"], "sql_id": down["sql_id"],
        "src_type": down["src_type"], "confidence": _min_confidence(chain),
        "transform_expr": f"{down['transform_expr'] or ''}(经 tmp 折叠:{inner})",
        "filter_cond": down["filter_cond"], "join_cond": down["join_cond"],
        "is_self_loop": chain[0]["src_key"] == down["dst_key"],
    }


def fold_level(edges: list[dict], is_tmp) -> tuple[list[dict], set[int]]:
    """单一 edge_level 的纯折叠算法(表级 key=表全名,字段级 key=(表,列))。

    从「非 tmp → tmp」的链头出发,沿 tmp 节点 DFS 穿透至非 tmp 终点即合成;
    多级 tmp 链迭代穿透,visited 防环(5.4)。
    返回 (合成边列表, 参与完整折叠链的原边 edge_id 集合)。
    """
    out_adj: dict = defaultdict(list)
    for e in edges:
        out_adj[e["src_key"]].append(e)
    synthetic: dict[tuple, dict] = {}
    used: set[int] = set()
    for head in edges:
        if is_tmp(head["src_key"]) or not is_tmp(head["dst_key"]):
            continue
        stack = [(head["dst_key"], (head,), frozenset({head["dst_key"]}))]
        while stack:
            node, chain, visited = stack.pop()
            for nxt in out_adj.get(node, ()):
                full_chain = chain + (nxt,)
                if is_tmp(nxt["dst_key"]):
                    if nxt["dst_key"] not in visited:
                        stack.append((nxt["dst_key"], full_chain,
                                      visited | {nxt["dst_key"]}))
                else:
                    syn = _synthesize(full_chain)
                    key = (syn["src_node_id"], syn["dst_node_id"],
                           syn["edge_level"], syn["sql_id"])
                    synthetic.setdefault(key, syn)
                    used.update(e["edge_id"] for e in full_chain)
    return list(synthetic.values()), used


def _load_tmp_layer_tables(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT full_name FROM table_metadata WHERE layer='tmp'")
        return {r["full_name"] for r in cur.fetchall()}


def fold_tmp_edges(conn, edge_table: str = "lineage_edge_shadow",
                   sql_ids: list[int] | None = None,
                   drop_all_tmp: bool = True) -> dict:
    """对边表做 tmp 穿透折叠后处理并落库。

    全量重建:edge_table=lineage_edge_shadow,drop_all_tmp=True——折叠后删除
    影子表中所有 tmp 相关边(不论是否折叠成功,tmp 不留边)。
    增量:限定 sql_ids 范围,drop_all_tmp=False——只删完成折叠的 tmp 边,
    链不完整的 tmp 边保留(见 fold_for_task 的限制说明)。
    """
    tmp_layer = _load_tmp_layer_tables(conn)

    def table_is_tmp(full_name: str) -> bool:
        return is_tmp_table(full_name, tmp_layer)

    sql = _EDGE_SELECT.format(table=edge_table)
    args: tuple = ()
    if sql_ids:
        sql += " WHERE e.sql_id IN ({})".format(",".join(["%s"] * len(sql_ids)))
        args = tuple(sql_ids)

    tbl_edges, col_edges, tmp_edge_ids = [], [], set()
    with conn.cursor() as cur:
        cur.execute(sql, args)
        for r in cur.fetchall():
            if not (table_is_tmp(r["src_table"]) or table_is_tmp(r["dst_table"])):
                continue                       # 非 tmp 相关边不进工作集
            tmp_edge_ids.add(r["edge_id"])
            if r["edge_level"] == "table":
                e = {**r, "src_key": r["src_table"], "dst_key": r["dst_table"]}
                tbl_edges.append(e)
            else:                              # 字段级按 (表, 列) 键衔接中转列
                e = {**r, "src_key": (r["src_table"], r["src_col"]),
                     "dst_key": (r["dst_table"], r["dst_col"])}
                col_edges.append(e)

    syn_t, used_t = fold_level(tbl_edges, table_is_tmp)
    syn_c, used_c = fold_level(col_edges, lambda k: table_is_tmp(k[0]))
    synthetic = syn_t + syn_c
    delete_ids = tmp_edge_ids if drop_all_tmp else (used_t | used_c)

    now = datetime.now()
    with conn.cursor() as cur:
        if synthetic:
            cur.executemany(
                _SYN_INSERT.format(table=edge_table),
                [(s["src_node_id"], s["dst_node_id"], s["edge_level"], s["sql_id"],
                  s["src_type"], s["confidence"], s["transform_expr"],
                  s["filter_cond"], s["join_cond"], int(s["is_self_loop"]), now)
                 for s in synthetic])
        ids = sorted(delete_ids)
        for i in range(0, len(ids), 5000):
            batch = ids[i:i + 5000]
            cur.execute("DELETE FROM {} WHERE edge_id IN ({})".format(
                edge_table, ",".join(["%s"] * len(batch))), batch)
    conn.commit()
    return {"synthetic_edges": len(synthetic), "deleted_tmp_edges": len(delete_ids)}


def fold_for_task(conn, sql_id: int) -> dict:
    """增量路径的任务内折叠:同 task_id 下全部 sql_id 范围内穿透 tmp 链(5.3)。

    已知限制(全量重建每日兜底收敛):
    ① 只在该任务范围内折叠——跨任务共享 tmp 表的链需要全图视角,增量不处理;
    ② 若同任务另一段 SQL 的 tmp 边已被上次全量折叠删除(增量只重解析了本段),
       链不完整,故只删除完成折叠的 tmp 边(drop_all_tmp=False),折不动的
       tmp 边原样保留(血缘经 tmp 中转仍可见),待夜间全量重建统一折叠清理。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT task_id FROM sql_repository WHERE sql_id=%s", (sql_id,))
        row = cur.fetchone()
        if not row or not row["task_id"]:
            return {"synthetic_edges": 0, "deleted_tmp_edges": 0}
        cur.execute("SELECT sql_id FROM sql_repository WHERE task_id=%s AND is_active=1",
                    (row["task_id"],))
        sql_ids = [r["sql_id"] for r in cur.fetchall()]
    return fold_tmp_edges(conn, edge_table="lineage_edge",
                          sql_ids=sql_ids, drop_all_tmp=False)
