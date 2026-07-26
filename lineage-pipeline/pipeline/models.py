from dataclasses import dataclass, field


@dataclass(frozen=True)
class ColumnRef:
    table: str                    # db.table 全名
    column: str = ""              # 空串表示表级节点


@dataclass
class LineageEdge:
    src: ColumnRef
    dst: ColumnRef
    edge_level: str               # table/column
    transform_expr: str = ""
    filter_cond: str = ""
    join_cond: str = ""
    is_derived: bool = False      # 临时表穿透合成边
    is_self_loop: bool = False


@dataclass
class ParseResult:
    status: str                   # success/degraded/failed
    target_table: str = ""
    edges: list[LineageEdge] = field(default_factory=list)
    reason: str = ""              # 原因码:dialect_unsupported/timeout/qualify_failed/...
    message: str = ""
    cost_ms: int = 0
