"""运营周报(设计方案 14 章):覆盖率/检索命中/采纳率/语义层命中/拒答分布。

cron 每周一执行:python -m pipeline.cli weekly-report > report.md,发值班群。
"""

from datetime import datetime

from . import coverage as coverage_mod


def _rows(conn, sql, *args):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def weekly(conn) -> str:
    cov = coverage_mod.report(conn)
    audit = _rows(conn, """
        SELECT sql_channel, status, refuse_reason, COUNT(*) AS cnt
        FROM query_audit WHERE created_at >= NOW() - INTERVAL 7 DAY
        GROUP BY sql_channel, status, refuse_reason""")
    fb = _rows(conn, """
        SELECT rating, COUNT(*) AS cnt FROM feedback
        WHERE created_at >= NOW() - INTERVAL 7 DAY GROUP BY rating""")
    active = _rows(conn, """
        SELECT COUNT(DISTINCT user_id) AS c FROM query_audit
        WHERE created_at >= NOW() - INTERVAL 7 DAY""")
    conf = _rows(conn, """
        SELECT confidence, COUNT(*) AS cnt FROM lineage_edge GROUP BY confidence""")

    total_q = sum(r["cnt"] for r in audit) or 1
    refused = sum(r["cnt"] for r in audit if r["status"] == "refused")
    semantic = sum(r["cnt"] for r in audit
                   if r["sql_channel"] == "semantic" and r["status"] == "ok")
    adhoc = sum(r["cnt"] for r in audit
                if r["sql_channel"] == "adhoc" and r["status"] == "ok")
    good = sum(r["cnt"] for r in fb if r["rating"] == 1)
    total_fb = sum(r["cnt"] for r in fb) or 1
    total_edges = sum(r["cnt"] for r in conf) or 1
    medium = sum(r["cnt"] for r in conf if r["confidence"] != "high")

    lines = [
        f"# 血缘平台运营周报({datetime.now():%Y-%m-%d})",
        "",
        "## 核心指标",
        f"- 加工层血缘覆盖率:{cov['process_layer_coverage']:.1%}",
        f"- 血缘边 medium 及以下占比:{medium / total_edges:.1%}(解析质量长期指标,4.6)",
        f"- 周活跃用户:{active[0]['c']}",
        f"- 取数请求:{total_q} 次(语义层 {semantic} / 兜底 {adhoc}),"
        f"语义层命中率 {semantic / max(semantic + adhoc, 1):.1%}",
        f"- 拒答率:{refused / total_q:.1%}",
        f"- 答案采纳率:{good / total_fb:.1%}(好评 {good}/{total_fb})",
        "",
        "## 拒答原因分布",
    ]
    for r in sorted((r for r in audit if r["status"] == "refused"),
                    key=lambda x: -x["cnt"]):
        lines.append(f"- {r['refuse_reason'] or '未知'}: {r['cnt']}")
    lines += ["", "## 解析状态(failed/degraded 到人,周会清零)"]
    for r in cov["parse_status"]:
        lines.append(f"- {r['parse_status']}"
                     f"{'/' + r['parse_reason'] if r['parse_reason'] else ''}"
                     f": {r['cnt']}")
    return "\n".join(lines)
