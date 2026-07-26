"""临时表穿透折叠单测(设计方案 5.3):单级/二级链/字段级衔接/混合非 tmp 源/防环。"""

from pipeline.derive import fold_level, is_tmp_table

_NODE_IDS: dict = {}


def _nid(key) -> int:
    return _NODE_IDS.setdefault(key, len(_NODE_IDS) + 1)


def _edge(eid, src, dst, sql_id=1, expr="", conf="high", level="table",
          filter_cond="", join_cond="", src_type="sql"):
    return {"edge_id": eid, "src_key": src, "dst_key": dst,
            "src_node_id": _nid(src), "dst_node_id": _nid(dst),
            "edge_level": level, "sql_id": sql_id, "src_type": src_type,
            "confidence": conf, "transform_expr": expr,
            "filter_cond": filter_cond, "join_cond": join_cond}


def _is_tmp_name(name: str) -> bool:
    return is_tmp_table(name, set())


def test_single_level_fold():
    # a → tmp → b:合成 a→b,sql_id 取下游段,confidence 取链上最低
    edges = [_edge(1, "ods.a", "tmp.t1", sql_id=11, expr="inner_expr", conf="medium"),
             _edge(2, "tmp.t1", "dwd.b", sql_id=12, expr="OUTER(x)", conf="high")]
    syn, used = fold_level(edges, _is_tmp_name)
    assert len(syn) == 1
    s = syn[0]
    assert (s["src_key"], s["dst_key"]) == ("ods.a", "dwd.b")
    assert s["sql_id"] == 12                       # 下游段
    assert s["confidence"] == "medium"             # 链上最低
    assert s["transform_expr"] == "OUTER(x)(经 tmp 折叠:inner_expr)"
    assert not s["is_self_loop"]
    assert used == {1, 2}


def test_two_level_tmp_chain():
    # a → tmp1 → tmp2 → b:多级链迭代穿透,内层表达式按链序拼接
    edges = [_edge(1, "ods.a", "tmp.t1", sql_id=1, expr="e1"),
             _edge(2, "tmp.t1", "tmp.t2", sql_id=2, expr="e2", conf="low"),
             _edge(3, "tmp.t2", "ads.b", sql_id=3, expr="e3")]
    syn, used = fold_level(edges, _is_tmp_name)
    assert len(syn) == 1
    s = syn[0]
    assert (s["src_key"], s["dst_key"]) == ("ods.a", "ads.b")
    assert s["sql_id"] == 3
    assert s["confidence"] == "low"
    assert s["transform_expr"] == "e3(经 tmp 折叠:e1 ; e2)"
    assert used == {1, 2, 3}


def test_column_level_link_via_tmp_column():
    # 字段级按 tmp 中转列衔接:(a,c1)→(t,x)→(b,c2) 折叠;(t,y)→(b,c3) 无上游不合成
    edges = [
        _edge(1, ("ods.a", "c1"), ("tmp.t", "x"), sql_id=1, expr="c1", level="column"),
        _edge(2, ("tmp.t", "x"), ("dwd.b", "c2"), sql_id=2,
              expr="CASE WHEN x='P' THEN 1 END", level="column"),
        _edge(3, ("tmp.t", "y"), ("dwd.b", "c3"), sql_id=2, expr="y", level="column"),
    ]
    syn, used = fold_level(edges, lambda k: _is_tmp_name(k[0]))
    assert len(syn) == 1
    s = syn[0]
    assert s["src_key"] == ("ods.a", "c1") and s["dst_key"] == ("dwd.b", "c2")
    assert "经 tmp 折叠:c1" in s["transform_expr"]
    assert used == {1, 2}                          # 断链的 y 边不参与折叠


def test_mixed_non_tmp_sources_untouched():
    # 直连边 a→b 与经 tmp 的 c→tmp→b 并存:只合成 c→b,a→b 不受影响
    edges = [_edge(1, "ods.c", "tmp.t", sql_id=1),
             _edge(2, "tmp.t", "dwd.b", sql_id=2)]
    syn, used = fold_level(edges, _is_tmp_name)
    assert [(s["src_key"], s["dst_key"]) for s in syn] == [("ods.c", "dwd.b")]
    assert used == {1, 2}


def test_tmp_cycle_terminates_with_visited():
    # tmp1↔tmp2 互指:visited 防环(5.4),仍能穿透到非 tmp 终点
    edges = [_edge(1, "ods.a", "tmp.t1", sql_id=1),
             _edge(2, "tmp.t1", "tmp.t2", sql_id=2),
             _edge(3, "tmp.t2", "tmp.t1", sql_id=3),
             _edge(4, "tmp.t2", "dwd.b", sql_id=4)]
    syn, used = fold_level(edges, _is_tmp_name)
    assert [(s["src_key"], s["dst_key"]) for s in syn] == [("ods.a", "dwd.b")]
    assert 1 in used and 2 in used and 4 in used


def test_self_loop_via_tmp():
    # a → tmp → a:合成自环边并打 is_self_loop(5.3)
    edges = [_edge(1, "dws.a", "tmp.t", sql_id=1),
             _edge(2, "tmp.t", "dws.a", sql_id=1)]
    syn, _ = fold_level(edges, _is_tmp_name)
    assert len(syn) == 1 and syn[0]["is_self_loop"]


def test_is_tmp_table_rules():
    # 三重判定:metadata layer / 库名 tmp 前缀 / 表名命名规则(前缀与后缀)
    assert is_tmp_table("dwd.some_table", {"dwd.some_table"})       # layer='tmp'
    assert is_tmp_table("tmp_credit.repay_dedup", set())            # 库名 tmp 开头
    assert is_tmp_table("dwd.tmp_stage1", set())                    # 表名 tmp_ 前缀
    assert is_tmp_table("dwd.temp_stage1", set())                   # 表名 temp_ 前缀
    assert is_tmp_table("dwd.repay_tmp", set())                     # 表名 _tmp 后缀
    assert not is_tmp_table("dwd.dwd_repay_detail", set())
    assert not is_tmp_table("dwd.template_x", set())                # temp_ 带下划线,不误伤 template
