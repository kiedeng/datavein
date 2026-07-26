"""语料库测试:真实风格 Hive/Spark 信贷数仓 SQL 全量过解析核心。

这是 13 章金标准用例集的仓库内起点;上线前应以真实生产 SQL 按同格式扩充。
"""

import pytest

from pipeline.parser.core import parse_sql
from tests.warehouse_fixture import CORPUS, SCHEMA


@pytest.mark.parametrize(
    "case", CORPUS, ids=[f'{c["task_id"]}#{c["node_seq"]}' for c in CORPUS])
def test_corpus(case):
    r = parse_sql(case["sql"], dialect=case["dialect"], schema=SCHEMA)

    assert r.status == case["status"], f"status={r.status} reason={r.reason} msg={r.message}"
    if case["status"] == "failed":
        assert r.reason == case["reason"]
        return

    assert r.target_table == case["target"]
    table_srcs = {e.src.table for e in r.edges if e.edge_level == "table"}
    assert table_srcs == case["sources"], f"table sources: {table_srcs}"

    col_edges = [e for e in r.edges if e.edge_level == "column"]
    if "expect_col_edges" in case:
        assert len(col_edges) == case["expect_col_edges"], \
            f'{len(col_edges)} col edges: {[(e.src, e.dst.column) for e in col_edges]}'

    for dst_col, src_table, src_col, expr_part in case.get("col_checks", []):
        hits = [e for e in col_edges
                if e.dst.column == dst_col
                and e.src.table == src_table and e.src.column == src_col]
        assert hits, (f"缺少列级边 {src_table}.{src_col} -> {dst_col};"
                      f" 实际: {[(str(e.src), e.dst.column) for e in col_edges]}")
        if expr_part:
            assert expr_part.upper() in hits[0].transform_expr.upper(), \
                f"表达式不含 {expr_part}: {hits[0].transform_expr}"

    if "self_loop_src" in case:
        loops = [e for e in r.edges if e.edge_level == "table" and e.is_self_loop]
        assert loops and loops[0].src.table == case["self_loop_src"]
