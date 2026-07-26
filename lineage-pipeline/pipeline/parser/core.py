"""解析核心(设计方案 5.2/5.3)。

sqlglot 为主:qualify(schema 展开 select */补前缀)→ 逐输出列 lineage() 取来源与表达式
→ WHERE/JOIN 归边。CTE 由 sqlglot lineage 递归穿透,不落库。
失败降级策略:字段级失败 → 降级表级(status=degraded);整体解析失败 → failed 带原因码。
表级交叉校验(sqllineage)与 60s 超时控制在上层 runner 实现,本模块保持纯函数。
"""

import time

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage as sg_lineage
from sqlglot.optimizer.qualify import qualify

from ..models import ColumnRef, LineageEdge, ParseResult


def _full_name(table: exp.Table, default_db: str) -> str:
    db = table.text("db") or default_db
    return f"{db}.{table.name}" if db else table.name


def _extract_target(tree: exp.Expression) -> exp.Table | None:
    if isinstance(tree, exp.Insert):
        this = tree.this
        if isinstance(this, exp.Schema):
            this = this.this
        return this if isinstance(this, exp.Table) else None
    if isinstance(tree, exp.Create) and tree.kind in ("TABLE", "VIEW"):
        this = tree.this
        if isinstance(this, exp.Schema):
            this = this.this
        return this if isinstance(this, exp.Table) else None
    return None


def _select_of(tree: exp.Expression) -> exp.Expression | None:
    """取 SELECT 部分;WITH 挂在 INSERT 上时重新挂回 select,保证 lineage 能看到 CTE。"""
    select = tree.expression
    if select is None:
        return None
    with_ = tree.args.get("with")
    if with_ is not None and not select.args.get("with"):
        select = select.copy()
        select.set("with", with_.copy())
    return select


def parse_sql(sql_text: str, dialect: str = "hive",
              schema: dict | None = None, default_db: str = "") -> ParseResult:
    """单段 SQL → 血缘边集合。schema 为 {db: {table: {col: type}}} 的解析期快照。"""
    t0 = time.monotonic()

    def _done(result: ParseResult) -> ParseResult:
        result.cost_ms = int((time.monotonic() - t0) * 1000)
        return result

    try:
        tree = parse_one(sql_text, dialect=dialect)
    except SqlglotError as e:
        return _done(ParseResult(status="failed", reason="parse_error",
                                 message=str(e)[:500]))

    target_node = _extract_target(tree)
    if target_node is None:
        return _done(ParseResult(status="failed", reason="no_target",
                                 message="非 INSERT/CTAS/视图定义,无法确定目标表"))
    target = _full_name(target_node, default_db)

    select = _select_of(tree)
    cte_names = {c.alias_or_name for c in tree.find_all(exp.CTE)}

    # ---- 表级边(WHERE/JOIN 条件归到边上)----
    where_sql, join_sql = "", ""
    if isinstance(select, exp.Select):
        where = select.args.get("where")
        where_sql = where.this.sql(dialect=dialect) if where else ""
        join_sql = "; ".join(j.args["on"].sql(dialect=dialect)
                             for j in select.args.get("joins") or []
                             if j.args.get("on"))

    src_tables: dict[str, exp.Table] = {}
    for t in tree.find_all(exp.Table):
        if t is target_node:
            continue                       # 目标表自身;同名再现于 FROM 即自依赖,保留
        if not t.text("db") and t.name in cte_names:
            continue                       # CTE 引用不是物理表
        src_tables.setdefault(_full_name(t, default_db), t)

    edges = [LineageEdge(src=ColumnRef(s), dst=ColumnRef(target),
                         edge_level="table", filter_cond=where_sql,
                         join_cond=join_sql, is_self_loop=(s == target))
             for s in sorted(src_tables)]

    if select is None or not isinstance(select, (exp.Select, exp.SetOperation)):
        return _done(ParseResult(status="degraded", target_table=target, edges=edges,
                                 reason="no_select", message="无 SELECT 体,仅表级血缘"))

    # ---- 字段级边:qualify 展开 * 后逐输出列 lineage ----
    try:
        qualified = qualify(select.copy(), schema=schema, dialect=dialect)
        out_cols = qualified.named_selects
    except SqlglotError as e:
        return _done(ParseResult(status="degraded", target_table=target, edges=edges,
                                 reason="qualify_failed", message=str(e)[:500]))
    if any(c == "*" for c in out_cols):
        return _done(ParseResult(status="degraded", target_table=target, edges=edges,
                                 reason="star_unresolved",
                                 message="select * 无 schema 快照可展开,仅表级血缘"))

    failed_cols: list[str] = []
    for col in out_cols:
        try:
            node = sg_lineage(col, select, schema=schema, dialect=dialect)
        except SqlglotError as e:
            failed_cols.append(f"{col}: {str(e)[:120]}")
            continue
        expr_sql = node.expression.sql(dialect=dialect)
        for leaf in node.walk():
            if leaf.downstream:
                continue
            if not isinstance(leaf.source, exp.Table):
                continue                   # 常量/参数等无物理来源
            src_full = _full_name(leaf.source, default_db)
            edges.append(LineageEdge(
                src=ColumnRef(src_full, leaf.name.split(".")[-1]),
                dst=ColumnRef(target, col),
                edge_level="column",
                transform_expr=expr_sql,
                filter_cond=where_sql,
                is_self_loop=(src_full == target)))

    if failed_cols:
        return _done(ParseResult(
            status="degraded", target_table=target, edges=edges,
            reason="column_lineage_partial",
            message="部分列血缘失败: " + "; ".join(failed_cols)[:500]))
    return _done(ParseResult(status="success", target_table=target, edges=edges))
