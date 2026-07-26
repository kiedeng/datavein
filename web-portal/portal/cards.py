"""降级模式:结构化直通(设计方案 9.3)。

LLM 不可用/超时时,搜索框输入直连 search_term/get_lineage,返回结构化卡片
(无自然语言组织),前端渲染成卡片。模型恢复后 app.py 自动切回对话模式。
"""

import time
from typing import Any

from server import repo  # 8.2:直接复用 MCP 服务数据访问层


def _timed(trace: list[dict], name: str, args: dict, fn) -> Any:
    """执行并把调用记录追加进 tool_trace(4.5 审计口径与对话模式一致)。"""
    t0 = time.monotonic()
    result = fn()
    trace.append({"tool": name, "args": args,
                  "cost_ms": int((time.monotonic() - t0) * 1000),
                  "ok": not (isinstance(result, dict) and "error" in result)})
    return result


def degraded_answer(conn, keyword: str, trace: list[dict]) -> list[dict]:
    """关键词 → search_term;单族且能定位表时追加上游血缘卡片。

    返回卡片列表:[{"type": "search"|"lineage"|"notice", "data": ...}]。
    """
    keyword = keyword.strip()
    cards: list[dict] = []
    st = _timed(trace, "search_term", {"keyword": keyword},
                lambda: repo.search_term(conn, keyword))
    cards.append({"type": "search", "data": st})

    if not st["families"]:
        cards.append({"type": "notice", "data": {
            "text": "未定位到相关术语/表/字段,请换关键词或提供 db.table 全名(拒答情形①)。"}})
        return cards
    if st["ambiguous"]:
        cards.append({"type": "notice", "data": {
            "text": "命中多个血缘族(异链多义),请从上方候选中选择具体表后再查询(6.2 消歧)。"}})
        return cards

    anchor = st["families"][0]["anchor"]
    table = anchor.get("ref_table") or anchor.get("full_name")
    column = anchor.get("ref_column") or anchor.get("column_name") or ""
    if not table:
        return cards
    edge_level = "column" if column else "table"
    lin = _timed(trace, "get_lineage",
                 {"full_name": table, "column": column, "edge_level": edge_level},
                 lambda: repo.get_lineage(conn, table, column, "upstream", 3, edge_level))
    if "error" in lin:
        cards.append({"type": "notice", "data": {
            "text": f"表 {table} 尚无血缘覆盖,可到「血缘补录」页人工画边(拒答情形②)。"}})
    else:
        cards.append({"type": "lineage", "data": lin})
    return cards
