"""闭包计算单测:链路/菱形多路径/环路防死循环/环路检出(设计方案 2.1/5.4)。"""

from pipeline.closure import compute_closure, find_cycle_pairs


def _as_dict(rows):
    return {(a, d): (h, c) for a, d, h, c in rows}


def test_chain():
    got = _as_dict(compute_closure({"a": {"b"}, "b": {"c"}}))
    assert got[("a", "b")] == (1, 1)
    assert got[("a", "c")] == (2, 1)
    assert got[("b", "c")] == (1, 1)


def test_diamond_counts_shortest_paths():
    # a→b→d, a→c→d:d 的最短路 2 跳、2 条
    got = _as_dict(compute_closure({"a": {"b", "c"}, "b": {"d"}, "c": {"d"}}))
    assert got[("a", "d")] == (2, 2)


def test_cycle_terminates():
    # a→b→a 非自环环路:遍历必须终止(visited 去重),连通关系仍完整
    got = _as_dict(compute_closure({"a": {"b"}, "b": {"a"}}))
    assert got[("a", "b")] == (1, 1)
    assert got[("b", "a")] == (1, 1)


def test_self_loop_excluded():
    got = _as_dict(compute_closure({"a": {"a", "b"}}))
    assert ("a", "a") not in got
    assert got[("a", "b")] == (1, 1)


def test_cycle_pairs_detected():
    # a↔b 互连通(非自环环路,建模错误信号 5.4);c→d 正常链不误报
    rows = compute_closure({"a": {"b"}, "b": {"a"}, "c": {"d"}})
    pairs = find_cycle_pairs((a, d) for a, d, *_ in rows)
    assert pairs == [("a", "b")]


def test_cycle_pairs_long_loop_and_dedup():
    # a→b→c→a 三节点环:闭包内两两互达,去重后 3 组有序对
    rows = compute_closure({"a": {"b"}, "b": {"c"}, "c": {"a"}})
    pairs = find_cycle_pairs((a, d) for a, d, *_ in rows)
    assert pairs == [("a", "b"), ("a", "c"), ("b", "c")]


def test_no_cycle_no_pairs():
    rows = compute_closure({"a": {"b", "c"}, "b": {"d"}, "c": {"d"}})
    assert find_cycle_pairs((a, d) for a, d, *_ in rows) == []
