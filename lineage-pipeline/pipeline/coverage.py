"""覆盖率大盘(设计方案 5.6):分母分层定义,按 层×主题域×owner 三维下钻。

加工层覆盖率 = 有血缘入边的加工层表 / 全部 is_online 加工层表;
ODS 覆盖率(ingest_map)单列,避免采集表拉低统计。
"""

PROCESS_LAYERS = ("dwd", "dws", "ads", "dim")

COVERAGE_SQL = """
SELECT tm.layer, tm.domain, tm.owner,
       COUNT(*)                                   AS total_tables,
       SUM(CASE WHEN le.dst_node_id IS NOT NULL THEN 1 ELSE 0 END) AS covered_tables
FROM table_metadata tm
LEFT JOIN lineage_node ln
       ON ln.full_name = tm.full_name AND ln.column_name = ''
LEFT JOIN (SELECT DISTINCT dst_node_id FROM lineage_edge WHERE edge_level='table') le
       ON le.dst_node_id = ln.node_id
WHERE tm.is_online = 1 AND tm.layer IN %(layers)s
GROUP BY tm.layer, tm.domain, tm.owner
"""

STATUS_SQL = """
SELECT parse_status, parse_reason, COUNT(*) AS cnt
FROM sql_repository WHERE is_active=1
GROUP BY parse_status, parse_reason
"""


def report(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(COVERAGE_SQL, {"layers": PROCESS_LAYERS})
        coverage = cur.fetchall()
        cur.execute(STATUS_SQL)
        parse_status = cur.fetchall()
    total = sum(r["total_tables"] for r in coverage) or 1
    covered = sum(r["covered_tables"] for r in coverage)
    return {"process_layer_coverage": round(covered / total, 4),
            "by_dimension": coverage,            # 层×域×owner 下钻
            "parse_status": parse_status}        # failed/degraded 原因码分布(5.8)
