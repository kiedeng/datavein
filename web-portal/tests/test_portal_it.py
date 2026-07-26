"""门户集成测试(真实 MySQL,DV_IT_MYSQL=1):

降级模式 chat 结构化卡片 / 大盘 / 术语 CRUD / 补录写库与幂等 / 反馈。
运行:DV_IT_MYSQL=1 python -m pytest tests -q(在 web-portal/ 下)
"""

import json

from conftest import all_rows, one, parse_sse, requires_mysql

pytestmark = requires_mysql


# ---------- 对话:降级模式结构化卡片(9.3)+ 审计(4.5) ----------

def test_chat_degraded_cards_and_audit(portal_env):
    client, conn = portal_env["client"], portal_env["conn"]
    resp = client.post("/api/chat", headers={"X-User": "zhangsan"},
                       json={"question": "dws_cust_credit_summary"})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = parse_sse(resp.text)
    kinds = [e for e, _ in events]
    assert kinds[0] == "meta" and events[0][1]["degraded"] is True
    assert kinds[-1] == "done"

    cards = [p for e, p in events if e == "card"]
    search = next(c for c in cards if c["type"] == "search")
    assert search["data"]["families"] and not search["data"]["ambiguous"]
    anchor = search["data"]["families"][0]["anchor"]
    assert anchor["full_name"] == "dws.dws_cust_credit_summary"
    lineage = next(c for c in cards if c["type"] == "lineage")
    hops = lineage["data"]["hops"]
    assert any(h["full_name"] == "dwd.dwd_contract_detail" for h in hops)

    # query_audit:channel='portal',tool_trace 含工具序列
    audit_id = events[-1][1]["audit_id"]
    row = one(conn, "SELECT * FROM query_audit WHERE audit_id=%s", audit_id)
    assert row["channel"] == "portal" and row["user_id"] == "zhangsan"
    assert row["question"] == "dws_cust_credit_summary" and row["status"] == "ok"
    trace = json.loads(row["tool_trace"])
    assert [t["tool"] for t in trace] == ["search_term", "get_lineage"]


def test_chat_no_hit_notice(portal_env):
    resp = portal_env["client"].post("/api/chat",
                                     json={"question": "不存在的神秘表xyz"})
    events = parse_sse(resp.text)
    notices = [p for e, p in events if e == "card" and p["type"] == "notice"]
    assert any("未定位到" in n["data"]["text"] for n in notices)


def test_chat_anonymous_default_user(portal_env):
    """X-User 缺省 → anonymous(SSO 接入前的默认身份)。"""
    resp = portal_env["client"].post("/api/chat", json={"question": "dim_org"})
    audit_id = parse_sse(resp.text)[-1][1]["audit_id"]
    row = one(portal_env["conn"],
              "SELECT user_id FROM query_audit WHERE audit_id=%s", audit_id)
    assert row["user_id"] == "anonymous"


# ---------- 覆盖率大盘(5.6) ----------

def test_dashboard(portal_env):
    d = portal_env["client"].get("/api/dashboard").json()
    assert 0 < d["process_layer_coverage"] <= 1
    layers = {r["layer"] for r in d["by_layer"]}
    assert {"dwd", "dws", "ads"} <= layers
    # 层×域×owner 下钻行含 owner 字段
    assert all({"layer", "domain", "owner"} <= set(r) for r in d["by_dimension"])
    # parse_status 原因码分布:multi-insert 用例按设计失败(5.8)
    failed = [r for r in d["parse_status"] if r["parse_status"] == "failed"]
    assert failed and failed[0]["parse_reason"] == "multi_insert_unsupported"
    assert "medium_edge_ratio" in d and 0 <= d["medium_edge_ratio"] <= 1


# ---------- 术语管理(4.4 CRUD + status 流转 + certified 认领) ----------

def test_glossary_crud(portal_env):
    client = portal_env["client"]
    body = {"term": "授信金额", "aliases": ["授信额度", "credit_amt"],
            "caliber": "客户维度累计授信金额,取 dws_cust_credit_summary.total_credit_amt",
            "domain": "credit", "owner": "zhangsan",
            "ref_table": "dws.dws_cust_credit_summary", "ref_column": "total_credit_amt"}
    r = client.post("/api/glossary", headers={"X-User": "zhangsan"}, json=body)
    assert r.status_code == 200
    term_id = r.json()["term_id"]

    # 新增默认 pending_review(待审批)
    items = client.get("/api/glossary", params={"keyword": "授信金额"}).json()["items"]
    it = next(x for x in items if x["term_id"] == term_id)
    assert it["status"] == "pending_review" and it["aliases"] == ["授信额度", "credit_amt"]

    # 同域重名 → 409(uk_term_domain)
    assert client.post("/api/glossary", json=body).status_code == 409

    # 审批流转 pending_review → active + certified 认领
    r = client.put(f"/api/glossary/{term_id}", headers={"X-User": "li_owner"},
                   json={"status": "active", "certified": 1})
    assert r.status_code == 200
    it = one(portal_env["conn"],
             "SELECT status, certified, updated_by FROM biz_glossary WHERE term_id=%s",
             term_id)
    assert it["status"] == "active" and it["certified"] == 1
    assert it["updated_by"] == "li_owner"

    # 非法 status → 422;不存在 id → 404
    assert client.put(f"/api/glossary/{term_id}",
                      json={"status": "bogus"}).status_code == 422
    assert client.put("/api/glossary/999999",
                      json={"status": "active"}).status_code == 404

    # certified 术语进入检索:search_term 命中且置顶(6.3)
    st = client.post("/api/chat", json={"question": "授信金额"})
    events = parse_sse(st.text)
    search = next(p for e, p in events if e == "card" and p["type"] == "search")
    assert search["data"]["families"][0]["anchor"]["certified"] == 1

    # 流转 deprecated 后按状态过滤可见
    client.put(f"/api/glossary/{term_id}", json={"status": "deprecated"})
    items = client.get("/api/glossary", params={"status": "deprecated"}).json()["items"]
    assert any(x["term_id"] == term_id for x in items)


# ---------- 血缘补录(failed 队列 + manual 画边幂等) ----------

def test_backfill_failed_queue(portal_env):
    d = portal_env["client"].get("/api/backfill/failed").json()
    assert d["total"] >= 1
    it = next(x for x in d["items"] if x["task_id"] == "etl_multi_insert")
    assert it["parse_reason"] == "multi_insert_unsupported"
    assert "INSERT OVERWRITE" in it["sql_preview"]


def test_manual_lineage_write_and_idempotent(portal_env):
    client, conn = portal_env["client"], portal_env["conn"]
    failed = client.get("/api/backfill/failed").json()["items"]
    sql_id = next(x["sql_id"] for x in failed if x["task_id"] == "etl_multi_insert")

    # 首次补录:multi-insert 的其中一个目标表,2 条来源边
    r = client.post("/api/lineage/manual", headers={"X-User": "wangwu"}, json={
        "dst_full_name": "dwd.dwd_repay_agg", "dst_column": "",
        "sql_id": sql_id,
        "sources": [{"full_name": "dwd.dwd_repay_detail",
                     "transform_expr": "SUM(repay_amt) GROUP BY contract_no"},
                    {"full_name": "ods.ods_t_repay"}]})
    assert r.status_code == 200
    out = r.json()
    assert out["edge_level"] == "table" and len(out["edge_ids"]) == 2

    dst_id = out["dst_node_id"]
    node = one(conn, "SELECT * FROM lineage_node WHERE node_id=%s", dst_id)
    assert node["full_name"] == "dwd.dwd_repay_agg" and node["node_type"] == "table"
    edges = all_rows(conn, """SELECT * FROM lineage_edge
                              WHERE dst_node_id=%s AND src_type='manual'""", dst_id)
    assert len(edges) == 2
    assert all(e["confidence"] == "medium" and e["sql_id"] == sql_id for e in edges)
    assert any("SUM(repay_amt)" in (e["transform_expr"] or "") for e in edges)
    # 队列联动:该 SQL 置 manual,退出 failed 队列
    assert one(conn, "SELECT parse_status FROM sql_repository WHERE sql_id=%s",
               sql_id)["parse_status"] == "manual"
    assert all(x["sql_id"] != sql_id
               for x in client.get("/api/backfill/failed").json()["items"])

    # 重复提交(来源改为 1 条)→ 先删旧 manual 边再插,不累积
    r2 = client.post("/api/lineage/manual", json={
        "dst_full_name": "dwd.dwd_repay_agg",
        "sources": [{"full_name": "dwd.dwd_repay_detail"}]})
    out2 = r2.json()
    assert out2["dst_node_id"] == dst_id                # 节点 get-or-create,不重复建
    assert out2["replaced"] == 2 and len(out2["edge_ids"]) == 1
    edges = all_rows(conn, """SELECT e.*, n.full_name AS src_name FROM lineage_edge e
                              JOIN lineage_node n ON n.node_id=e.src_node_id
                              WHERE e.dst_node_id=%s AND e.src_type='manual'""", dst_id)
    assert len(edges) == 1 and edges[0]["src_name"] == "dwd.dwd_repay_detail"

    # 补录边立即可被 get_lineage 查到(闭包表待夜间重建,README 已注明)
    from server import repo
    lin = repo.get_lineage(repo.connect(), "dwd.dwd_repay_agg", direction="upstream",
                           depth=1)
    assert any(h["full_name"] == "dwd.dwd_repay_detail" and h["src_type"] == "manual"
               for h in lin["hops"])


def test_manual_lineage_column_level_and_validation(portal_env):
    client, conn = portal_env["client"], portal_env["conn"]
    # 字段级补录
    r = client.post("/api/lineage/manual", json={
        "dst_full_name": "dwd.dwd_repay_agg", "dst_column": "total_amt",
        "sources": [{"full_name": "dwd.dwd_repay_detail", "column": "repay_amt",
                     "transform_expr": "SUM(repay_amt)"}]})
    assert r.status_code == 200 and r.json()["edge_level"] == "column"
    node = one(conn, """SELECT node_type FROM lineage_node
                        WHERE full_name='dwd.dwd_repay_agg' AND column_name='total_amt'""")
    assert node["node_type"] == "column"
    # 入参校验:非 db.table 全名 / 空来源 → 422
    assert client.post("/api/lineage/manual", json={
        "dst_full_name": "no_db_table", "sources": [{"full_name": "a.b"}]}
    ).status_code == 422
    assert client.post("/api/lineage/manual", json={
        "dst_full_name": "a.b", "sources": []}).status_code == 422


# ---------- 反馈(4.5 feedback) ----------

def test_feedback(portal_env):
    client, conn = portal_env["client"], portal_env["conn"]
    resp = client.post("/api/chat", json={"question": "ads_credit_report"})
    audit_id = parse_sse(resp.text)[-1][1]["audit_id"]

    r = client.post("/api/feedback", headers={"X-User": "zhangsan"},
                    json={"audit_id": audit_id, "rating": 0, "comment": "血缘少了一跳"})
    assert r.status_code == 200
    row = one(conn, "SELECT * FROM feedback WHERE fb_id=%s", r.json()["fb_id"])
    assert row["audit_id"] == audit_id and row["rating"] == 0
    assert row["comment"] == "血缘少了一跳" and row["user_id"] == "zhangsan"
    assert row["triage_status"] == "pending"           # 待运营闭环(14 章)
    # 非法 rating / 缺 audit_id → 422
    assert client.post("/api/feedback",
                       json={"audit_id": audit_id, "rating": 5}).status_code == 422
    assert client.post("/api/feedback", json={"rating": 1}).status_code == 422
