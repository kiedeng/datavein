"""goldstd 共用逻辑:标注文件读写、解析结果 → 预填标注、单用例比对判定。

金标准抽检工作流(设计方案 13 章)的最小共享核:
  extract.py  机器预填标注(降低人工标注成本)
  compare.py  仅取 verified: true 的标注重新解析比对,按 13 章口径出报告
两者对"边"的口径必须一致,故集中在本模块:
  边身份 = (dst_col, src_table, src_col);表达式做归一化子串匹配(留空跳过)。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from pipeline.parser.core import parse_sql  # noqa: F401  (re-export 给 CLI 用)

EXPR_MAX_LEN = 160          # 预填表达式摘要截断长度
ELLIPSIS = "…"              # 截断标记,比对前会被剥掉
DIALECT_MANIFEST = "dialects.json"   # SQL 目录内可选的 {case_id: dialect} 清单

ANNOTATION_HEADER = """\
# ============================ 金标准标注文件(机器预填) ============================
# 人工核对:确认无误改 verified: true,有错直接改数据(改完同样置 verified: true)。
#   1. 对照同名 .sql 文件逐项检查 target_table / table_sources / column_edges;
#   2. dst_col 以目标表 schema 的位置映射为准(Hive 语义,非 SELECT 别名);
#   3. CTE / 子查询别名不是物理源表,table_sources 只留真实库表;
#   4. expr 是表达式摘要,比对时做"归一化子串"匹配;拿不准可清空(留空=跳过表达式比对);
#   5. machine_status: failed 且确认该 SQL 确实无法/不应解析(如 Hive 多表插入应拆分)时,
#      保持 target_table: null 直接置 verified: true;若人工能标出真实血缘,请补全数据。
# machine_* 字段是解析器原始输出快照,仅供参考,不参与比对。
# ==================================================================================
"""


# ---------------------------------------------------------------- 基础 IO ----

def load_schema(path: str | Path) -> dict:
    """读 schema 快照,格式同 pipeline:{db: {table: {col: type}}}。"""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_dialect_manifest(sql_dir: str | Path) -> dict:
    """SQL 目录下可选的 dialects.json:{case_id: dialect};没有则返回空。"""
    p = Path(sql_dir) / DIALECT_MANIFEST
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def read_annotation(path: str | Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def write_annotation(path: str | Path, ann: dict) -> None:
    """带核对说明头注释写出 YAML(键序即人工核对顺序)。"""
    body = yaml.safe_dump(ann, allow_unicode=True, sort_keys=False,
                          default_flow_style=False, width=100)
    Path(path).write_text(ANNOTATION_HEADER + body, encoding="utf-8")


# ------------------------------------------------- 解析结果 → 标注/比对视图 ----

def machine_view(result) -> tuple[str, list[str], dict[tuple, str]]:
    """ParseResult → (target, 源表有序列表, {边身份: 表达式})。

    边身份 = (dst_col, src_table, src_col),同身份多条(如 UNION 分支同表达式)去重,
    保留首个表达式 —— 与 13 章"字段级按边计"的口径一致。
    """
    target = result.target_table or ""
    sources = sorted({e.src.table for e in result.edges if e.edge_level == "table"})
    edges: dict[tuple, str] = {}
    for e in result.edges:
        if e.edge_level != "column":
            continue
        edges.setdefault((e.dst.column, e.src.table, e.src.column),
                         e.transform_expr or "")
    return target, sources, edges


def truncate_expr(expr: str) -> str:
    expr = (expr or "").strip()
    if len(expr) > EXPR_MAX_LEN:
        return expr[:EXPR_MAX_LEN] + ELLIPSIS
    return expr


def annotation_from_result(case_id: str, sql_file: str, dialect: str,
                           result) -> dict:
    target, sources, edges = machine_view(result)
    return {
        "case_id": case_id,
        "sql_file": sql_file,
        "dialect": dialect,
        "machine_status": result.status,       # success/degraded/failed
        "machine_reason": result.reason or "",
        "verified": False,
        "notes": "",
        "target_table": target or None,
        "table_sources": sources,
        "column_edges": [
            {"dst_col": d, "src_table": t, "src_col": c, "expr": truncate_expr(x)}
            for (d, t, c), x in edges.items()
        ],
    }


# ------------------------------------------------------------- 比对判定 ----

def normalize_expr(s: str) -> str:
    """表达式归一化:去截断标记、压空白、大写 —— 人工微调格式不应算错。"""
    s = (s or "").replace(ELLIPSIS, " ")
    return re.sub(r"\s+", " ", s).strip().upper()


def expr_ok(gold_expr: str, machine_expr: str) -> bool:
    """金标准表达式留空 = 跳过;否则归一化后须为机器表达式的子串(相等是特例)。"""
    g = normalize_expr(gold_expr)
    if not g:
        return True
    return g in normalize_expr(machine_expr)


@dataclass
class CaseDiff:
    """单用例比对结果。字段级四类:匹配 / 缺失(金标准有机器无)/
    多余(机器有金标准无)/ 表达式不符(边在但表达式对不上)。"""
    case_id: str
    machine_status: str
    gold_edge_count: int
    table_ok: bool
    matched: int = 0
    missing: list = field(default_factory=list)        # [(dst_col, src_table, src_col)]
    extra: list = field(default_factory=list)
    expr_mismatch: list = field(default_factory=list)  # [(边身份, 金标准expr, 机器expr)]
    table_errors: list = field(default_factory=list)   # [str]

    @property
    def field_error_count(self) -> int:
        return len(self.missing) + len(self.extra) + len(self.expr_mismatch)


def compare_case(ann: dict, result) -> CaseDiff:
    """金标准标注 vs 机器解析结果。

    表级口径(13 章):target + sources 集合全对才算该用例表级正确。
    金标准 target_table 为 null 表示"人工确认此 SQL 无法/不应解析出血缘"
    (如多表插入按登记规范应拆分),此时机器同样失败即算表级正确。
    """
    target, sources, m_edges = machine_view(result)
    gold_target = ann.get("target_table") or ""
    gold_sources = sorted(ann.get("table_sources") or [])
    gold_edges: dict[tuple, str] = {}
    for e in ann.get("column_edges") or []:
        gold_edges[(e["dst_col"], e["src_table"], e["src_col"])] = e.get("expr") or ""

    table_errors: list[str] = []
    if not gold_target:
        if target:
            table_errors.append(f"金标准判定不可解析,但机器解析出目标表 {target}")
    else:
        if target != gold_target:
            table_errors.append(
                f"目标表不符: 机器={target or '(解析失败)'} 金标准={gold_target}")
        miss_t = sorted(set(gold_sources) - set(sources))
        extra_t = sorted(set(sources) - set(gold_sources))
        if miss_t:
            table_errors.append("缺失源表: " + ", ".join(miss_t))
        if extra_t:
            table_errors.append("多余源表: " + ", ".join(extra_t))

    diff = CaseDiff(case_id=ann.get("case_id", "?"),
                    machine_status=result.status,
                    gold_edge_count=len(gold_edges),
                    table_ok=not table_errors,
                    table_errors=table_errors)

    for key, gx in gold_edges.items():
        if key not in m_edges:
            diff.missing.append(key)
        elif expr_ok(gx, m_edges[key]):
            diff.matched += 1
        else:
            diff.expr_mismatch.append((key, gx, m_edges[key]))
    diff.extra = [k for k in sorted(m_edges) if k not in gold_edges]
    return diff


def fmt_edge(key: tuple) -> str:
    dst_col, src_table, src_col = key
    return f"{src_table}.{src_col} -> {dst_col}"
