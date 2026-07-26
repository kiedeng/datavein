"""闭包计算单测:链路/菱形多路径/环路防死循环(设计方案 2.1/5.4)。"""

from pipeline.closure import compute_closure


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
