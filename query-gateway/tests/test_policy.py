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
