"""plan→SQL 确定性编译器(设计方案 7.2)。

确定性保障:① join 按 alias、plan 过滤按 (field,op,值序列化) 固定排序;
② sqlglot 版本 lockfile 锁死;③ 每指标 golden 用例全文比对入 CI;
④ 无任何随机性来源。同 plan 恒同 SQL。

LLM 的自由度被限制在"选参数":plan 只能引用指标 YAML 已声明的维度,
越界编译期报错并随错误返回可用字段清单供 Agent 自纠(7.1)。
"""

import json
from dataclasses import dataclass

from sqlglot import exp, parse_one

from . import config
from .metrics_repo import Metric

_OPS = {"eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
        "between": "BETWEEN", "in": "IN", "like": "LIKE"}


class PlanError(ValueError):
    """plan 不合法;message 面向 Agent 自纠,附可用字段清单。"""


class PermissionDenied(ValueError):
    """权限不足;宁严勿松(1.3)。"""


@dataclass
class Compiled:
    sql: str
    metric: str
    metric_version: int
    caliber: str
    row_policy_applied: str
    masked_columns: list[str]        # 结果需掩码的输出列(L3)
    is_customer_query: bool          # 按客户明细查询标记(11 章合规)


def _lit(value) -> str:
    return exp.convert(value).sql(dialect=config.WAREHOUSE_DIALECT)


def _filter_sql(dim_column: str, op: str, value) -> str:
    if op == "between":
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise PlanError("between 需要 [低值, 高值] 两元素")
        return f"{dim_column} BETWEEN {_lit(value[0])} AND {_lit(value[1])}"
    if op == "in":
        if not isinstance(value, (list, tuple)) or not value:
            raise PlanError("in 需要非空数组")
        return f"{dim_column} IN ({', '.join(_lit(v) for v in value)})"
    return f"{dim_column} {_OPS[op]} {_lit(value)}"


def _render_row_policy(policy: str, user_ctx: dict) -> str:
    """':var' 占位符以 user_ctx 同名变量渲染;变量缺失/为空即拒绝(宁严勿松)。"""
    out = policy
    for token in {t for t in policy.split() if t.startswith(":")}:
        var = token[1:]
        vals = user_ctx.get(var)
        if not vals:
            raise PermissionDenied(f"行权限变量 {var} 缺失或为空,拒绝执行")
        if isinstance(vals, (list, tuple)):
            rendered = "(" + ", ".join(_lit(v) for v in vals) + ")"
        else:
            rendered = _lit(vals)
        out = out.replace(token, rendered)
    return out


_SENSITIVE_CUSTOMER_DIMS = {"cust_no", "cust_name", "id_card", "mobile"}


def compile_plan(metric: Metric, plan: dict, user_ctx: dict) -> Compiled:
    dims_req = list(plan.get("dimensions") or [])
    unknown = [d for d in dims_req if d not in metric.dimensions]
    if unknown:
        raise PlanError(f"未声明的维度 {unknown};该指标可用维度: {sorted(metric.dimensions)}")

    # 敏感维度权限(11 章):L4 一律拒出;L3 需白名单,结果掩码
    masked = []
    whitelist = set(user_ctx.get("sensitive_whitelist") or [])
    for d in dims_req:
        sens = metric.dimensions[d].sensitivity
        if sens == "L4":
            raise PermissionDenied(f"维度 {d} 为 L4 级,任何场景拒绝输出")
        if sens == "L3":
            if d not in whitelist:
                raise PermissionDenied(f"维度 {d} 为 L3 敏感维度,不在你的白名单内")
            masked.append(d)

    # SELECT 列:维度按 plan 顺序(输出列序是 plan 的一部分)+ 指标表达式
    select_cols = [f"{metric.dimensions[d].column} AS {d}" for d in dims_req]
    select_cols.append(f"{metric.expression} AS {metric.metric}")

    # JOIN:去重后按 alias 排序(确定性①)
    joins = {}
    for d in dims_req:
        j = metric.dimensions[d].join
        if j:
            joins[j["alias"]] = j
    join_sql = "".join(f' JOIN {j["table"]} AS {j["alias"]} ON {j["on"]}'
                       for _, j in sorted(joins.items()))

    # WHERE:default_filters(定义序)→ plan filters(排序)→ row_policy(恒最后)
    conds = list(metric.default_filters)
    plan_filters = plan.get("filters") or []
    norm = []
    for f in plan_filters:
        field_, op = f.get("field"), f.get("op")
        if field_ not in metric.dimensions:
            raise PlanError(f"过滤字段 {field_} 未声明;可用: {sorted(metric.dimensions)}")
        if op not in _OPS:
            raise PlanError(f"不支持的操作符 {op};可用: {sorted(_OPS)}")
        norm.append((field_, op, json.dumps(f.get("value"), ensure_ascii=False,
                                            sort_keys=True)))
    for field_, op, value_json in sorted(norm):
        conds.append(_filter_sql(metric.dimensions[field_].column, op,
                                 json.loads(value_json)))
    row_policy = _render_row_policy(metric.row_policy, user_ctx)
    conds.append(row_policy)

    group_by = ", ".join(metric.dimensions[d].column for d in dims_req)
    order_items = []
    for ob in plan.get("order_by") or []:
        f, direction = ob.get("field"), (ob.get("dir") or "asc").lower()
        if f != metric.metric and f not in metric.dimensions:
            raise PlanError(f"排序字段 {f} 未声明")
        if direction not in ("asc", "desc"):
            raise PlanError("dir 只能是 asc/desc")
        col = metric.metric if f == metric.metric else metric.dimensions[f].column
        order_items.append(f"{col} {direction.upper()}")

    limit = min(int(plan.get("limit") or config.HARD_LIMIT), config.HARD_LIMIT)

    sql = (f"SELECT {', '.join(select_cols)}"
           f" FROM {metric.base_table} AS base{join_sql}"
           f" WHERE {' AND '.join(f'({c})' for c in conds)}")
    if group_by:
        sql += f" GROUP BY {group_by}"
    if order_items:
        sql += f" ORDER BY {', '.join(order_items)}"
    sql += f" LIMIT {limit}"

    # 末次规范化:经 sqlglot 往返,消除手拼空格差异(版本锁死保证稳定)
    sql = parse_one(sql, dialect=config.WAREHOUSE_DIALECT).sql(
        dialect=config.WAREHOUSE_DIALECT)

    return Compiled(
        sql=sql, metric=metric.metric, metric_version=metric.version,
        caliber=metric.caliber, row_policy_applied=row_policy,
        masked_columns=masked,
        is_customer_query=bool(_SENSITIVE_CUSTOMER_DIMS
                               & (set(dims_req)
                                  | {f[0] for f in norm})))
