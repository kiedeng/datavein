"""门户侧写库与聚合查询:审计/反馈/术语管理/血缘补录/覆盖率大盘。

只读查询直接复用 server.repo 与 pipeline.coverage(8.2 不复制逻辑);
本模块只承载门户特有的写路径(4.4/4.5 表)与大盘聚合。
连接一律取自 server.repo.connect() 连接池(autocommit,事务处显式 begin)。
"""

import json
import time
from typing import Any

from pipeline import coverage  # sys.path 由 portal/__init__.py 注入
from server import repo

GLOSSARY_STATUSES = ("active", "pending_review", "deprecated")   # 4.4 status 流转域


# ---------- 审计与反馈(4.5) ----------

def write_audit(user_id: str, session_id: str, question: str, trace: list[dict],
                status: str, cost_ms: int, refuse_reason: str = "") -> int:
    """每次对话落 query_audit(channel='portal'),tool_trace 记录工具调用序列。"""
    conn = repo.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO query_audit
                   (session_id, user_id, channel, question, tool_trace,
                    status, refuse_reason, cost_ms, created_at)
                   VALUES (%s,%s,'portal',%s,%s,%s,%s,%s,NOW())""",
                (session_id, user_id, question,
                 json.dumps(trace, ensure_ascii=False, default=str),
                 status, refuse_reason or None, cost_ms))
            return cur.lastrowid
    finally:
        conn.close()


def write_feedback(user_id: str, audit_id: int, rating: int, comment: str) -> int:
    """好评/差评 + 评论落 feedback 表,triage_status 默认 pending 待运营闭环(14 章)。"""
    conn = repo.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO feedback (audit_id, user_id, rating, comment, created_at)
                   VALUES (%s,%s,%s,%s,NOW())""",
                (audit_id, user_id, rating, comment or None))
            return cur.lastrowid
    finally:
        conn.close()


# ---------- 覆盖率大盘(5.6,复用 pipeline.coverage 的口径) ----------

def dashboard() -> dict[str, Any]:
    """分层覆盖率 + 层×域×owner 下钻 + parse_status 原因码分布 + medium 边占比(4.6)。"""
    conn = repo.connect()
    try:
        rep = coverage.report(conn)                    # 口径复用,不自写覆盖率 SQL
        by_layer: dict[str, dict[str, int]] = {}
        for row in rep["by_dimension"]:
            agg = by_layer.setdefault(row["layer"], {"total_tables": 0, "covered_tables": 0})
            agg["total_tables"] += int(row["total_tables"])
            agg["covered_tables"] += int(row["covered_tables"] or 0)
        with conn.cursor() as cur:
            cur.execute("""SELECT confidence, COUNT(*) AS cnt
                           FROM lineage_edge GROUP BY confidence""")
            conf_rows = cur.fetchall()
        total_edges = sum(int(r["cnt"]) for r in conf_rows) or 1
        medium = sum(int(r["cnt"]) for r in conf_rows if r["confidence"] == "medium")
        return {
            "process_layer_coverage": rep["process_layer_coverage"],
            "by_layer": [{"layer": k, **v,
                          "coverage": round(v["covered_tables"] / v["total_tables"], 4)
                          if v["total_tables"] else 0.0}
                         for k, v in sorted(by_layer.items())],
            "by_dimension": rep["by_dimension"],
            "parse_status": rep["parse_status"],
            "edge_confidence": conf_rows,
            "medium_edge_ratio": round(medium / total_edges, 4),
        }
    finally:
        conn.close()


# ---------- 术语管理(4.4 biz_glossary) ----------

def list_glossary(status: str | None = None, keyword: str | None = None,
                  domain: str | None = None, limit: int = 200) -> list[dict]:
    conn = repo.connect()
    try:
        conds, args = ["1=1"], []
        if status:
            conds.append("status=%s"); args.append(status)
        if domain:
            conds.append("domain=%s"); args.append(domain)
        if keyword:
            conds.append("(term LIKE %s OR caliber LIKE %s)")
            args += [f"%{keyword}%", f"%{keyword}%"]
        with conn.cursor() as cur:
            cur.execute(f"""SELECT term_id, term, aliases, caliber, domain, owner,
                                   ref_table, ref_column, metric_name, certified,
                                   sensitivity, status, updated_by, updated_at
                            FROM biz_glossary WHERE {' AND '.join(conds)}
                            ORDER BY updated_at DESC, term_id DESC LIMIT %s""",
                        (*args, int(limit)))
            rows = cur.fetchall()
        for r in rows:
            r["aliases"] = json.loads(r["aliases"]) if r["aliases"] else []
        return rows
    finally:
        conn.close()


def create_glossary(item: dict, user: str) -> int:
    """新增术语,默认 pending_review 待审批(status 流转:pending_review→active→deprecated)。"""
    status = item.get("status", "pending_review")
    if status not in GLOSSARY_STATUSES:
        raise ValueError(f"status 必须为 {GLOSSARY_STATUSES}")
    conn = repo.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO biz_glossary
                   (term, aliases, caliber, domain, owner, ref_table, ref_column,
                    metric_name, certified, sensitivity, status, updated_by, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())""",
                (item["term"], json.dumps(item.get("aliases", []), ensure_ascii=False),
                 item.get("caliber"), item.get("domain"), item.get("owner"),
                 item.get("ref_table"), item.get("ref_column"), item.get("metric_name"),
                 int(item.get("certified", 0)), item.get("sensitivity"), status, user))
            return cur.lastrowid
    finally:
        conn.close()


def update_glossary(term_id: int, patch: dict, user: str) -> bool:
    """增改/certified 认领(6.3 owner 钦定)/status 流转;仅白名单字段可改。"""
    allowed = ("term", "aliases", "caliber", "domain", "owner", "ref_table",
               "ref_column", "metric_name", "certified", "sensitivity", "status")
    sets, args = [], []
    for key in allowed:
        if key not in patch:
            continue
        val = patch[key]
        if key == "status" and val not in GLOSSARY_STATUSES:
            raise ValueError(f"status 必须为 {GLOSSARY_STATUSES}")
        if key == "aliases":
            val = json.dumps(val or [], ensure_ascii=False)
        if key == "certified":
            val = int(val)
        sets.append(f"{key}=%s"); args.append(val)
    if not sets:
        return False
    conn = repo.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""UPDATE biz_glossary
                            SET {', '.join(sets)}, updated_by=%s, updated_at=NOW()
                            WHERE term_id=%s""", (*args, user, int(term_id)))
            return cur.rowcount > 0
    finally:
        conn.close()


# ---------- 血缘补录(M3:failed 队列 + 人工画边) ----------

def list_failed_sql(limit: int = 100) -> list[dict]:
    """sql_repository 里 parse_status='failed' 的待补录队列(5.3 疑难 SQL 兜底)。"""
    conn = repo.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT sql_id, task_id, task_name, node_seq, target_table, dialect,
                          owner, domain, parse_reason, parse_msg, updated_at,
                          LEFT(sql_text, 2000) AS sql_preview
                   FROM sql_repository
                   WHERE parse_status='failed' AND is_active=1
                   ORDER BY updated_at DESC LIMIT %s""", (int(limit),))
            return cur.fetchall()
    finally:
        conn.close()


def _get_or_create_node(cur, full_name: str, column: str) -> int:
    """lineage_node get-or-create;利用 uk(full_name,column_name) + LAST_INSERT_ID 技巧原子取 id。"""
    cur.execute(
        """INSERT INTO lineage_node (node_type, full_name, column_name)
           VALUES (%s,%s,%s)
           ON DUPLICATE KEY UPDATE node_id=LAST_INSERT_ID(node_id)""",
        ("column" if column else "table", full_name, column))
    return cur.lastrowid


def manual_lineage(dst_full_name: str, dst_column: str,
                   sources: list[dict], user: str, sql_id: int | None = None) -> dict:
    """人工画边:src_type='manual', confidence='medium'(4.6:人工补录默认 medium)。

    幂等语义:同一 dst(同 edge_level)重复提交,先删旧 manual 边再插新边,单事务;
    传 sql_id 时同步把 sql_repository 该行置 parse_status='manual'(4.2 状态域)。
    """
    edge_level = "column" if dst_column else "table"
    conn = repo.connect()
    try:
        conn.begin()                                   # 池连接 autocommit,显式开事务
        try:
            with conn.cursor() as cur:
                dst_id = _get_or_create_node(cur, dst_full_name, dst_column)
                cur.execute("""DELETE FROM lineage_edge
                               WHERE dst_node_id=%s AND edge_level=%s AND src_type='manual'""",
                            (dst_id, edge_level))
                deleted = cur.rowcount
                edge_ids = []
                for src in sources:
                    src_col = (src.get("column") or "") if edge_level == "column" else ""
                    src_id = _get_or_create_node(cur, src["full_name"], src_col)
                    cur.execute(
                        """INSERT INTO lineage_edge
                           (src_node_id, dst_node_id, edge_level, sql_id, src_type,
                            confidence, transform_expr, is_self_loop, updated_at)
                           VALUES (%s,%s,%s,%s,'manual','medium',%s,%s,NOW())""",
                        (src_id, dst_id, edge_level, sql_id,
                         src.get("transform_expr"),
                         1 if src_id == dst_id else 0))
                    edge_ids.append(cur.lastrowid)
                if sql_id:
                    cur.execute("""UPDATE sql_repository
                                   SET parse_status='manual', updated_at=NOW()
                                   WHERE sql_id=%s""", (int(sql_id),))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return {"dst_node_id": dst_id, "edge_level": edge_level,
                "replaced": deleted, "edge_ids": edge_ids, "operator": user}
    finally:
        conn.close()
