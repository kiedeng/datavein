"""解析核心(设计方案 5.2/5.3)。

sqlglot 为主:qualify(schema 展开 select */补前缀)→ 逐输出列 lineage() 取来源与表达式
→ WHERE/JOIN 归边。CTE 由 sqlglot lineage 递归穿透,不落库。
失败降级策略:字段级失败 → 降级表级(status=degraded);整体解析失败 → failed 带原因码。
表级交叉校验(sqllineage)与 60s 超时控制在上层 runner 实现,本模块保持纯函数。
"""

import re
import time

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage as sg_lineage
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import MappingSchema, Schema

from ..models import ColumnRef, LineageEdge, ParseResult

# Hive 多表插入(FROM src INSERT ... INSERT ...)sqlglot 不支持:
# 按登记规范应拆分为单 INSERT 段;单列原因码供覆盖率大盘运营(5.8)
_MULTI_INSERT_RE = re.compile(r"^\s*FROM\b.*\bINSERT\b", re.I | re.S)

# 关键性能优化(压测发现):把原始 dict schema 直接交给 sqlglot 时,qualify 与逐列
# lineage 各自重建一遍 MappingSchema(单段 SQL 重建 N+1 次,大 schema 下每次数秒),
# 解析耗时随 schema 大小放大而非 SQL 复杂度——5 万表规模会击穿全量重建 <1h 承诺。
# 预构建 MappingSchema 一次复用,实测 200~300 倍提速。
# 缓存按 (schema 对象身份, dialect) 命中:全量重建 worker 内 _worker_schema 稳定持有,
# 同一 dialect 只构建一次;每 worker 每 dialect 一份,内存可忽略。
_MS_CACHE: dict[str, tuple] = {}


def _mapping_schema(schema, dialect: str):
    """dict schema → 复用的 MappingSchema;已是 Schema 实例则直接返回。"""
    if schema is None:
        return None
    if isinstance(schema, Schema):
        return schema
    cached = _MS_CACHE.get(dialect)
    if cached is not None and cached[0] is schema:   # 身份比较,worker 内 dict 稳定
        return cached[1]
    ms = MappingSchema(schema, dialect=dialect)
    _MS_CACHE[dialect] = (schema, ms)
    return ms


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


def _dst_columns(out_cols: list[str], explicit_cols: list[str] | None,
                 target_node: exp.Table, schema: dict | None,
                 default_db: str) -> list[str]:
    """目标列名映射(Hive 语义:SELECT 列按位置对应目标表列,而非按别名)。

    优先级:INSERT 显式列清单 > 目标表 schema 位置映射 > SELECT 别名兜底。
    静态分区列不出现在 SELECT 中,而分区列在 schema 快照中恒排最后
    (metadata_sync 保证),故前缀映射即正确语义。
    """
    if explicit_cols and len(explicit_cols) == len(out_cols):
        return explicit_cols
    db = target_node.text("db") or default_db
    target_cols = _schema_columns(schema, db, target_node.name)
    if target_cols and len(out_cols) <= len(target_cols):
        return target_cols[:len(out_cols)]
    return out_cols


def _schema_columns(schema, db: str, table: str) -> list[str]:
    """目标表列序;兼容 dict 与预构建 MappingSchema 两种 schema 形态。"""
    if schema is None:
        return []
    if isinstance(schema, Schema):
        try:
            return list(schema.column_names(exp.table_(table, db=db)))
        except Exception:
            return []
    return list((schema.get(db) or {}).get(table) or {})


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
        reason = ("multi_insert_unsupported" if _MULTI_INSERT_RE.match(sql_text)
                  else "parse_error")
        return _done(ParseResult(status="failed", reason=reason,
                                 message=str(e)[:500]))

    target_node = _extract_target(tree)
    if target_node is None:
        return _done(ParseResult(status="failed", reason="no_target",
                                 message="非 INSERT/CTAS/视图定义,无法确定目标表"))
    target = _full_name(target_node, default_db)
    explicit_cols = None
    if isinstance(tree, exp.Insert) and isinstance(tree.this, exp.Schema):
        explicit_cols = [c.name for c in tree.this.expressions]

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
        if t.find_ancestor(exp.Hint) is not None:
            continue                       # Spark hint 参数(如 BROADCAST(o))不是表
        src_tables.setdefault(_full_name(t, default_db), t)

    edges = [LineageEdge(src=ColumnRef(s), dst=ColumnRef(target),
                         edge_level="table", filter_cond=where_sql,
                         join_cond=join_sql, is_self_loop=(s == target))
             for s in sorted(src_tables)]

    if select is None or not isinstance(select, (exp.Select, exp.SetOperation)):
        return _done(ParseResult(status="degraded", target_table=target, edges=edges,
                                 reason="no_select", message="无 SELECT 体,仅表级血缘"))

    # ---- 字段级边:qualify 展开 * 后逐输出列 lineage ----
    mschema = _mapping_schema(schema, dialect)   # 预构建复用,避免 N+1 次重建
    try:
        qualified = qualify(select.copy(), schema=mschema, dialect=dialect)
        out_cols = qualified.named_selects
    except SqlglotError as e:
        return _done(ParseResult(status="degraded", target_table=target, edges=edges,
                                 reason="qualify_failed", message=str(e)[:500]))
    if any(c == "*" for c in out_cols):
        return _done(ParseResult(status="degraded", target_table=target, edges=edges,
                                 reason="star_unresolved",
                                 message="select * 无 schema 快照可展开,仅表级血缘"))

    dst_names = _dst_columns(out_cols, explicit_cols, target_node, schema, default_db)
    failed_cols: list[str] = []
    for col, dst_col in zip(out_cols, dst_names):
        try:
            node = sg_lineage(col, select, schema=mschema, dialect=dialect)
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
                dst=ColumnRef(target, dst_col),
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
