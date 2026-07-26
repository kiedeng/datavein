"""编译器 golden 用例(设计方案 7.2③):plan→SQL 期望全文比对。

指标 YAML 或 sqlglot 版本变更导致本文件失败时:人工确认语义后更新 golden,
并在 PR 中说明——golden 变更即口径变更,需评审。
"""

import pytest

from gateway.compiler import PlanError, compile_plan
from gateway.metrics_repo import MetricsRepo

repo = MetricsRepo("metrics")
CTX = {"user_id": "u1", "user_orgs": ["O01", "O02"],
       "sensitive_whitelist": ["cust_no"]}

GOLDEN = [
    (
        {"metric": "total_credit_amt", "dimensions": ["org_name"],
         "filters": [{"field": "dt", "op": "eq", "value": "2026-07-25"}],
         "order_by": [{"field": "total_credit_amt", "dir": "desc"}], "limit": 10},
        "SELECT o.org_name AS org_name, SUM(base.total_credit_amt) AS total_credit_amt"
        " FROM dws.dws_cust_credit_summary AS base"
        " JOIN dim.dim_org AS o ON o.org_no = base.org_no"
        " WHERE (NOT base.dt IS NULL) AND (base.dt = '2026-07-25')"
        " AND (base.org_no IN ('O01', 'O02'))"
        " GROUP BY o.org_name ORDER BY total_credit_amt DESC LIMIT 10",
    ),
    (
        {"metric": "total_credit_amt", "dimensions": ["org_no", "cust_no"],
         "filters": [{"field": "dt", "op": "between",
                      "value": ["2026-07-01", "2026-07-25"]},
                     {"field": "org_no", "op": "in", "value": ["O01"]}]},
        "SELECT base.org_no AS org_no, base.cust_no AS cust_no,"
        " SUM(base.total_credit_amt) AS total_credit_amt"
        " FROM dws.dws_cust_credit_summary AS base"
        " WHERE (NOT base.dt IS NULL)"
        " AND (base.dt BETWEEN '2026-07-01' AND '2026-07-25')"
        " AND (base.org_no IN ('O01')) AND (base.org_no IN ('O01', 'O02'))"
        " GROUP BY base.org_no, base.cust_no LIMIT 1000",
    ),
    (
        {"metric": "apply_cnt", "dimensions": ["org_name", "dt"], "filters": []},
        "SELECT base.org_name AS org_name, base.dt AS dt,"
        " SUM(base.apply_cnt) AS apply_cnt"
        " FROM dws.dws_org_credit_day AS base"
        " WHERE (base.org_no IN ('O01', 'O02'))"
        " GROUP BY base.org_name, base.dt LIMIT 1000",
    ),
]


@pytest.mark.parametrize("plan,expected", GOLDEN,
                         ids=[p["metric"] + str(i) for i, (p, _) in enumerate(GOLDEN)])
def test_golden(plan, expected):
    c = compile_plan(repo.get(plan["metric"]), plan, CTX)
    assert c.sql == expected


def test_determinism_same_plan_same_sql():
    plan = GOLDEN[1][0]
    a = compile_plan(repo.get("total_credit_amt"), plan, CTX).sql
    b = compile_plan(repo.get("total_credit_amt"), plan, CTX).sql
    assert a == b


def test_determinism_filter_order_irrelevant():
    # 7.2①:plan filters 编译前排序,乱序输入同 SQL
    base = GOLDEN[1][0]
    shuffled = {**base, "filters": list(reversed(base["filters"]))}
    a = compile_plan(repo.get("total_credit_amt"), base, CTX).sql
    b = compile_plan(repo.get("total_credit_amt"), shuffled, CTX).sql
    assert a == b


def test_unknown_dimension_returns_available_fields():
    plan = {"metric": "apply_cnt", "dimensions": ["prod_type"]}
    with pytest.raises(PlanError) as e:
        compile_plan(repo.get("apply_cnt"), plan, CTX)
    assert "可用维度" in str(e.value) and "org_name" in str(e.value)   # Agent 自纠


def test_limit_is_capped():
    plan = {"metric": "apply_cnt", "dimensions": ["org_no"], "limit": 999999}
    c = compile_plan(repo.get("apply_cnt"), plan, CTX)
    assert c.sql.endswith("LIMIT 1000")


def test_customer_query_flagged_via_filter():
    plan = {"metric": "total_credit_amt", "dimensions": ["org_no"],
            "filters": [{"field": "cust_no", "op": "eq", "value": "C001"}]}
    c = compile_plan(repo.get("total_credit_amt"), plan, CTX)
    assert c.is_customer_query        # 按客户查询单独标记(11 章合规)
