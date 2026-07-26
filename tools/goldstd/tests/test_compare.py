"""compare.py:对 / 错(表级)/ 缺失边 / 多余边 / 表达式不符 四类判定 + 退出码逻辑。"""

from common import read_annotation, write_annotation
from conftest import run_compare, run_extract


def _verify_all(ws, mutate=None):
    """模拟人工核对:置 verified,可选注入一处修改。"""
    for path in ws["ann_dir"].glob("*.yaml"):
        ann = read_annotation(path)
        ann["verified"] = True
        if mutate:
            mutate(ann)
        write_annotation(path, ann)


def _report(ws):
    return ws["report"].read_text(encoding="utf-8")


def test_all_correct_exit_zero(workspace):
    run_extract(workspace)
    _verify_all(workspace)
    assert run_compare(workspace) == 0
    text = _report(workspace)
    assert "1/1 = 100.00%" in text and "验收结论:通过" in text


def test_no_verified_is_config_error(workspace):
    run_extract(workspace)
    assert run_compare(workspace) == 2           # 全部未核对 → 输入错误而非"通过"


def test_missing_edge_detected(workspace):
    """金标准有、机器没有 → 缺失边,计入错误并导致阈值不达标退出 1。"""
    run_extract(workspace)

    def add_gold_edge(ann):
        ann["column_edges"].append(dict(dst_col="b", src_table="db1.t_src",
                                        src_col="dt", expr=""))
    _verify_all(workspace, add_gold_edge)
    assert run_compare(workspace) == 1           # 2 对 1 缺 → 66.67% < 95%
    text = _report(workspace)
    assert "[缺失边] db1.t_src.dt -> b" in text
    assert "2/3 = 66.67%" in text


def test_extra_edge_detected(workspace):
    """机器有、金标准没有 → 多余边。"""
    run_extract(workspace)

    def drop_gold_edge(ann):
        ann["column_edges"] = [e for e in ann["column_edges"] if e["dst_col"] != "b"]
    _verify_all(workspace, drop_gold_edge)
    assert run_compare(workspace) == 1
    assert "[多余边] db1.t_src.b -> b" in _report(workspace)


def test_wrong_edge_is_missing_plus_extra(workspace):
    """人工改错来源列 → 该边同时表现为 1 缺失 + 1 多余。"""
    run_extract(workspace)

    def rewire(ann):
        for e in ann["column_edges"]:
            if e["dst_col"] == "a":
                e["src_col"] = "b"
                e["expr"] = ""
    _verify_all(workspace, rewire)
    assert run_compare(workspace) == 1
    text = _report(workspace)
    assert "[缺失边] db1.t_src.b -> a" in text
    assert "[多余边] db1.t_src.a -> a" in text


def test_expr_mismatch_detected(workspace):
    """边身份对但金标准表达式子串对不上 → 表达式不符,计入错误。"""
    run_extract(workspace)

    def bad_expr(ann):
        for e in ann["column_edges"]:
            if e["dst_col"] == "a":
                e["expr"] = "COALESCE(a, 0)"
    _verify_all(workspace, bad_expr)
    assert run_compare(workspace) == 1
    assert "[表达式不符]" in _report(workspace)


def test_table_level_error(workspace):
    """sources 集合不全对 → 该用例表级算错,表级准确率跌破阈值退出 1。"""
    run_extract(workspace)

    def add_phantom_source(ann):
        ann["table_sources"].append("db1.t_phantom")
    _verify_all(workspace, add_phantom_source)
    assert run_compare(workspace) == 1
    text = _report(workspace)
    assert "缺失源表: db1.t_phantom" in text
    assert "0/1 = 0.00%" in text


def test_thresholds_configurable(workspace):
    """放低阈值后同样的错误可以通过(退出 0)——阈值逻辑本身可测。"""
    run_extract(workspace)

    def add_gold_edge(ann):
        ann["column_edges"].append(dict(dst_col="b", src_table="db1.t_src",
                                        src_col="dt", expr=""))
    _verify_all(workspace, add_gold_edge)
    assert run_compare(workspace, field_threshold=50) == 0
