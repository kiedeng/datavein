"""权限用例(设计方案 11/13 章):越权域/敏感维度/行权限,全部必须拦截。"""

import pytest

from gateway.compiler import PermissionDenied, compile_plan
from gateway.executor import ExecutionRejected, validate_select_only
from gateway.metrics_repo import Dimension, Metric, MetricsRepo

repo = MetricsRepo("metrics")


def test_missing_user_orgs_refused():
    plan = {"metric": "apply_cnt", "dimensions": ["org_no"]}
    with pytest.raises(PermissionDenied, match="user_orgs"):
        compile_plan(repo.get("apply_cnt"), plan, {"user_id": "u1"})


def test_empty_user_orgs_refused():
    # 宁严勿松:权限同步失败(空列表)即拒,不是放行全部
    plan = {"metric": "apply_cnt", "dimensions": ["org_no"]}
    with pytest.raises(PermissionDenied):
        compile_plan(repo.get("apply_cnt"), plan,
                     {"user_id": "u1", "user_orgs": []})


def test_l3_without_whitelist_refused():
    plan = {"metric": "total_credit_amt", "dimensions": ["cust_no"]}
    with pytest.raises(PermissionDenied, match="L3"):
        compile_plan(repo.get("total_credit_amt"), plan,
                     {"user_id": "u1", "user_orgs": ["O01"]})


def test_l4_always_refused_even_with_whitelist():
    m = Metric(metric="m", cn_name="x", caliber="x", version=1,
               base_table="t.t", expression="SUM(base.v)",
               dimensions={"id_card": Dimension("id_card", "base.id_card",
                                                sensitivity="L4")},
               row_policy="base.org IN :user_orgs")
    with pytest.raises(PermissionDenied, match="L4"):
        compile_plan(m, {"metric": "m", "dimensions": ["id_card"]},
                     {"user_orgs": ["O01"], "sensitive_whitelist": ["id_card"]})


def test_executor_rejects_dml():
    for sql in ("UPDATE dws.t SET a=1", "DELETE FROM dws.t", "DROP TABLE dws.t",
                "INSERT INTO dws.t VALUES (1)"):
        with pytest.raises(ExecutionRejected):
            validate_select_only(sql)


def test_executor_rejects_multi_statement():
    with pytest.raises(ExecutionRejected, match="单条"):
        validate_select_only("SELECT 1 LIMIT 1; SELECT 2 LIMIT 1")


def test_executor_rejects_blacklist():
    with pytest.raises(ExecutionRejected, match="黑名单"):
        validate_select_only("SELECT * FROM ods.ods_t_cust_secret LIMIT 10")


def test_executor_rejects_missing_limit():
    with pytest.raises(ExecutionRejected, match="LIMIT"):
        validate_select_only("SELECT a FROM dws.t")


# ---- 安全走查回归(2026-07 专项审查发现的三处越权路径) ----

def test_l3_sensitive_dim_as_filter_refused():
    """漏洞:L3 敏感维度作为 WHERE 过滤字段绕过白名单,可精确定位单客户。"""
    plan = {"metric": "total_credit_amt", "dimensions": ["org_no"],
            "filters": [{"field": "cust_no", "op": "eq", "value": "C001"}]}
    with pytest.raises(PermissionDenied, match="L3"):
        compile_plan(repo.get("total_credit_amt"), plan,
                     {"user_id": "u", "user_orgs": ["O01"]})   # 无白名单


def test_l3_sensitive_dim_as_filter_allowed_with_whitelist():
    plan = {"metric": "total_credit_amt", "dimensions": ["org_no"],
            "filters": [{"field": "cust_no", "op": "eq", "value": "C001"}]}
    c = compile_plan(repo.get("total_credit_amt"), plan,
                     {"user_id": "u", "user_orgs": ["O01"],
                      "sensitive_whitelist": ["cust_no"]})
    assert c.is_customer_query          # 白名单内放行但标记按客户查询(合规追查)


def test_l4_sensitive_dim_as_order_refused():
    m = Metric(metric="m", cn_name="x", caliber="x", version=1,
               base_table="t.t", expression="SUM(base.v)",
               dimensions={"org": Dimension("org", "base.org"),
                           "id_card": Dimension("id_card", "base.id_card",
                                                sensitivity="L4")},
               row_policy="base.org IN :user_orgs")
    plan = {"metric": "m", "dimensions": ["org"],
            "order_by": [{"field": "id_card", "dir": "asc"}]}
    with pytest.raises(PermissionDenied, match="L4"):
        compile_plan(m, plan, {"user_orgs": ["O01"],
                               "sensitive_whitelist": ["id_card"]})


def test_executor_rejects_for_update():
    with pytest.raises(ExecutionRejected, match="加锁"):
        validate_select_only("SELECT a FROM dws.t LIMIT 10 FOR UPDATE")


def test_executor_rejects_lock_in_share_mode():
    with pytest.raises(ExecutionRejected, match="加锁"):
        validate_select_only("SELECT a FROM dws.t LIMIT 10 LOCK IN SHARE MODE")


def test_executor_rejects_select_into_var():
    # SELECT ... INTO @var 可被 sqlglot 解析,须命中 into 拒绝分支
    with pytest.raises(ExecutionRejected, match="数据外带"):
        validate_select_only("SELECT a INTO @v FROM dws.t LIMIT 1")


def test_executor_rejects_into_outfile():
    # INTO OUTFILE 被 sqlglot 判为无法解析,同样落到拒绝(纵深:两道都拦)
    with pytest.raises(ExecutionRejected):
        validate_select_only("SELECT a INTO OUTFILE '/tmp/x' FROM dws.t")


def test_executor_blacklist_case_insensitive():
    # 大小写变体不得绕过黑名单
    with pytest.raises(ExecutionRejected, match="黑名单"):
        validate_select_only("SELECT * FROM ODS.ODS_T_CUST_SECRET LIMIT 10")
