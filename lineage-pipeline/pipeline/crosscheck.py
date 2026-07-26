"""sqllineage 表级交叉校验(设计方案 5.2):双引擎不一致进人工复核队列。

只做表级来源集合比对;sqllineage 解析失败不阻塞主流程(sqlglot 为准)。
"""

import logging

log = logging.getLogger(__name__)

_DIALECT_MAP = {"hive": "hive", "spark": "sparksql", "mysql": "mysql"}


def _normalize(name: str) -> str:
    return name.lower().removeprefix("<default>.")


def table_sources(sql_text: str, dialect: str = "hive") -> set[str] | None:
    """返回来源表集合;sqllineage 自身失败返回 None(不参与比对)。"""
    try:
        from sqllineage.runner import LineageRunner
        runner = LineageRunner(sql_text, dialect=_DIALECT_MAP.get(dialect, "hive"))
        tables = runner.source_tables
        if callable(tables):          # 版本差异:旧版为方法,1.5+ 为属性
            tables = tables()
        return {_normalize(str(t)) for t in tables}
    except Exception as e:
        log.debug("sqllineage failed (non-blocking): %s", str(e)[:200])
        return None


def mismatch(parse_result, sql_text: str, dialect: str) -> bool:
    """双引擎表级来源不一致 → True(标记 crosscheck_mismatch 进复核队列)。"""
    other = table_sources(sql_text, dialect)
    if other is None:
        return False
    ours = {_normalize(e.src.table) for e in parse_result.edges
            if e.edge_level == "table"}
    return ours != other
