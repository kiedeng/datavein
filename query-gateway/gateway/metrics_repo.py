"""指标 YAML 仓库(设计方案 7.1):加载、校验、版本。

指标文件经 Git 评审合入 metrics/ 目录,服务启动时加载;version 变更须与内容
变更同 PR(golden 用例同步更新),审计中的 plan 带此版本号保证历史可追溯(7.3)。
"""

import os
from dataclasses import dataclass, field

import yaml

_REQUIRED = ("metric", "cn_name", "caliber", "version", "base_table",
             "expression", "dimensions", "row_policy")


@dataclass
class Dimension:
    name: str
    column: str
    sensitivity: str = ""            # ''/L1/L2/L3/L4
    join: dict | None = None         # {alias, table, on}


@dataclass
class Metric:
    metric: str
    cn_name: str
    caliber: str
    version: int
    base_table: str
    expression: str
    dimensions: dict[str, Dimension]
    default_filters: list[str] = field(default_factory=list)
    row_policy: str = ""
    verified_questions: list[str] = field(default_factory=list)


def _load_one(path: str) -> Metric:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    missing = [k for k in _REQUIRED if k not in raw]
    if missing:
        raise ValueError(f"{path}: 缺少必填字段 {missing}")
    dims = {}
    for name, d in raw["dimensions"].items():
        if "column" not in d:
            raise ValueError(f"{path}: 维度 {name} 缺少 column")
        join = d.get("join")
        if join and not all(k in join for k in ("alias", "table", "on")):
            raise ValueError(f"{path}: 维度 {name} 的 join 需含 alias/table/on")
        dims[name] = Dimension(name=name, column=d["column"],
                               sensitivity=d.get("sensitivity", ""), join=join)
    return Metric(metric=raw["metric"], cn_name=raw["cn_name"],
                  caliber=raw["caliber"], version=int(raw["version"]),
                  base_table=raw["base_table"], expression=raw["expression"],
                  dimensions=dims,
                  default_filters=list(raw.get("default_filters") or []),
                  row_policy=raw["row_policy"],
                  verified_questions=list(raw.get("verified_questions") or []))


class MetricsRepo:
    def __init__(self, metrics_dir: str):
        self._metrics: dict[str, Metric] = {}
        for fname in sorted(os.listdir(metrics_dir)):
            if fname.endswith((".yaml", ".yml")):
                m = _load_one(os.path.join(metrics_dir, fname))
                if m.metric in self._metrics:
                    raise ValueError(f"指标重名: {m.metric}")
                self._metrics[m.metric] = m

    def get(self, name: str) -> Metric | None:
        return self._metrics.get(name)

    def list(self) -> list[dict]:
        return [{"metric": m.metric, "cn_name": m.cn_name, "version": m.version,
                 "dimensions": sorted(m.dimensions),
                 "caliber": m.caliber, "verified_questions": m.verified_questions}
                for m in self._metrics.values()]
