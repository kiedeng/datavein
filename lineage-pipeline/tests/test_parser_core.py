"""解析核心单测:覆盖设计方案 5.3 的疑难形态(金标准用例集起点,13 章)。"""

from pipeline.models import ColumnRef
from pipeline.parser.core import parse_sql

SCHEMA = {
    "ods": {"t_apply": {"apply_id": "bigint", "cust_no": "string",
                        "apply_amt": "decimal(18,2)", "dt": "string"},
            "t_cust": {"cust_no": "string", "cust_name": "string"}},
    "dwd": {"d_apply": {"apply_id": "bigint", "cust_no": "string",
                        "credit_amt": "decimal(18,2)"}},
}


def _col_edges(result):
    return [e for e in result.edges if e.edge_level == "column"]


def _table_srcs(result):
    return {e.src.table for e in result.edges if e.edge_level == "table"}


def test_simple_insert_select_with_join_and_where():
    sql = """
    INSERT INTO dwd.d_apply
    SELECT a.apply_id, a.cust_no, a.apply_amt * 100 AS credit_amt
    FROM ods.t_apply a JOIN ods.t_cust c ON a.cust_no = c.cust_no
    WHERE a.dt = '2026-01-01'
    """
    r = parse_sql(sql, schema=SCHEMA)
    assert r.status == "success"
    assert r.target_table == "dwd.d_apply"
    assert _table_srcs(r) == {"ods.t_apply", "ods.t_cust"}
    # 表达式与过滤条件必须落在边上("怎么算的"是本平台与普通血缘图的本质差异)
    credit = [e for e in _col_edges(r) if e.dst.column == "credit_amt"]
    assert credit and credit[0].src == ColumnRef("ods.t_apply", "apply_amt")
    assert "100" in credit[0].transform_expr
    assert "dt" in credit[0].filter_cond


def test_cte_is_penetrated_not_persisted():
    sql = """
    INSERT INTO dwd.d_apply
    WITH base AS (SELECT apply_id, cust_no, apply_amt FROM ods.t_apply WHERE dt = '2026-01-01')
    SELECT apply_id, cust_no, apply_amt AS credit_amt FROM base
    """
    r = parse_sql(sql, schema=SCHEMA)
    assert r.status == "success"
    assert _table_srcs(r) == {"ods.t_apply"}          # CTE 不是物理节点
    srcs = {e.src for e in _col_edges(r)}
    assert ColumnRef("ods.t_apply", "apply_amt") in srcs


def test_self_loop_is_flagged():
    sql = """
    INSERT INTO dwd.d_apply
    SELECT apply_id, cust_no, credit_amt FROM dwd.d_apply WHERE cust_no IS NOT NULL
    """
    r = parse_sql(sql, schema=SCHEMA)
    assert r.status == "success"
    table_edges = [e for e in r.edges if e.edge_level == "table"]
    assert table_edges[0].is_self_loop                # 增量滚动表允许自环(5.3)
    assert all(e.is_self_loop for e in _col_edges(r))


def test_star_without_schema_degrades_to_table_level():
    sql = "INSERT INTO dwd.d_apply SELECT * FROM ods.t_apply"
    r = parse_sql(sql, schema=None)
    assert r.status == "degraded"
    assert r.reason == "star_unresolved"
    assert _table_srcs(r) == {"ods.t_apply"}          # 表级血缘不丢
    assert not _col_edges(r)


def test_star_with_schema_expands():
    sql = "INSERT INTO dwd.d_apply SELECT apply_id, cust_no, apply_amt FROM ods.t_apply"
    r = parse_sql(sql, schema=SCHEMA)
    assert r.status == "success"
    assert {e.dst.column for e in _col_edges(r)} == {"apply_id", "cust_no", "apply_amt"}


def test_non_dml_fails_with_reason():
    r = parse_sql("DROP TABLE ods.t_apply", schema=SCHEMA)
    assert r.status == "failed"
    assert r.reason == "no_target"
