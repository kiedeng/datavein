"""表级传递闭包:全量重算 + 增量修补(设计方案 2.1/2.2)。

path_cnt 语义为「最短路径条数」(BFS 分层计数,环路下仍良定义);
影响分析只消费连通性与 min_hops,path_cnt 用于路径复杂度提示。
"""

from collections import defaultdict, deque

from . import config


def compute_closure(adjacency: dict[str, set[str]]) -> list[tuple[str, str, int, int]]:
    """邻接表 → [(ancestor, descendant, min_hops, path_cnt)]。自环不参与闭包。"""
    rows = []
    for anc in adjacency:
        hops = {anc: 0}
        cnt = defaultdict(int, {anc: 1})
        q = deque([anc])
        while q:
            cur = q.popleft()
            for nxt in adjacency.get(cur, ()):
                if nxt == anc:
                    continue                       # 自环
                if nxt not in hops:
                    hops[nxt] = hops[cur] + 1
                    cnt[nxt] = cnt[cur]
                    q.append(nxt)
                elif hops[nxt] == hops[cur] + 1:
                    cnt[nxt] += cnt[cur]           # 同层多路径累加
        rows.extend((anc, d, h, cnt[d]) for d, h in hops.items() if d != anc)
    return rows


def load_table_adjacency(conn) -> dict[str, set[str]]:
    """从 lineage_edge 表级边加载邻接表(node_id 解析回全名)。"""
    sql = """
    SELECT ns.full_name AS src, nd.full_name AS dst
    FROM lineage_edge e
    JOIN lineage_node ns ON ns.node_id = e.src_node_id
    JOIN lineage_node nd ON nd.node_id = e.dst_node_id
    WHERE e.edge_level = 'table'
    """
    adj: dict[str, set[str]] = defaultdict(set)
    with conn.cursor() as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            adj[row["src"]].add(row["dst"])
    return dict(adj)


def rebuild_closure_shadow(conn) -> int:
    """全量重算写影子表;切换由上层编排在校验后执行(4.3)。返回行数。"""
    rows = compute_closure(load_table_adjacency(conn))
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS table_closure_shadow")
        cur.execute("CREATE TABLE table_closure_shadow LIKE table_closure")
        for i in range(0, len(rows), 5000):
            cur.executemany(
                "INSERT INTO table_closure_shadow VALUES (%s,%s,%s,%s)",
                rows[i:i + 5000])
    conn.commit()
    return len(rows)


def find_cycle_pairs(pairs) -> list[tuple[str, str]]:
    """闭包连通对中 (a,b) 与 (b,a) 同时存在的非自环环路对,去重排序(5.4)。"""
    s = set(pairs)
    return sorted({tuple(sorted((a, b))) for a, b in s if a != b and (b, a) in s})


def detect_cycles(conn) -> list[tuple[str, str]]:
    """基于 table_closure 检出非自环环路(建模错误信号,5.4)。

    SQL 侧自联接完成互连通判定,避免全表拉回内存;结果进 pipeline_run.stats,
    推送双方 owner 由行内告警通道消费,代码到 stats 为止。
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t1.ancestor AS a, t1.descendant AS b
            FROM table_closure t1
            JOIN table_closure t2
              ON t1.ancestor = t2.descendant AND t1.descendant = t2.ancestor
            WHERE t1.ancestor < t1.descendant""")
        return sorted({(r["a"], r["b"]) for r in cur.fetchall()})


def incremental_patch(conn, target_table: str) -> str:
    """单表入边变化后的闭包修补(2.2)。返回 'patched' 或 'degrade_full_rebuild'。

    受影响区域 = (ancestors(T)∪{T}) × (descendants(T)∪{T})。
    超过退化阈值说明改的是核心枢纽表,增量不再划算,由调用方转异步全量。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT ancestor FROM table_closure WHERE descendant=%s",
                    (target_table,))
        ancestors = {r["ancestor"] for r in cur.fetchall()} | {target_table}
        cur.execute("SELECT descendant FROM table_closure WHERE ancestor=%s",
                    (target_table,))
        descendants = {r["descendant"] for r in cur.fetchall()} | {target_table}

    if len(ancestors) * len(descendants) > config.CLOSURE_INCR_DEGRADE_THRESHOLD:
        return "degrade_full_rebuild"

    adj = load_table_adjacency(conn)               # 已含 T 的新入边
    region_rows = [r for r in compute_closure({a: adj.get(a, set()) for a in adj})
                   if r[0] in ancestors and r[1] in descendants]
    with conn.cursor() as cur:
        # 区域内先删后插,单事务(4.3)
        for a in ancestors:
            cur.execute(
                "DELETE FROM table_closure WHERE ancestor=%s AND descendant IN ({})"
                .format(",".join(["%s"] * len(descendants))),
                (a, *descendants))
        for i in range(0, len(region_rows), 5000):
            cur.executemany("INSERT INTO table_closure VALUES (%s,%s,%s,%s)",
                            region_rows[i:i + 5000])
    conn.commit()
    return "patched"
