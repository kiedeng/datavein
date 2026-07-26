"""generator 轻量测试:可复现性 + 语料可解析率(不依赖 MySQL)。

运行:cd tools/benchmark && python -m pytest tests -q
解析测试借用 run_benchmark 的 ensure_schema 缓存补丁提速(仅影响耗时,
不影响 parse_sql 的 status 判定)。
"""

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import generator  # noqa: E402
import run_benchmark  # noqa: E402


def test_same_seed_identical():
    a = generator.generate(tables=120, sqls=150, seed=7)
    b = generator.generate(tables=120, sqls=150, seed=7)
    assert a == b                       # 全量结构逐字节一致(schema/表/语料)


def test_diff_seed_differs():
    a = generator.generate(tables=120, sqls=150, seed=7)
    c = generator.generate(tables=120, sqls=150, seed=8)
    assert a != c


def test_counts_and_shapes():
    data = generator.generate(tables=200, sqls=200, seed=42)
    assert len(data["corpus"]) == 200                 # 段数精确(tmp 链按段计)
    shapes = {c["shape"] for c in data["corpus"]}
    assert {"simple", "join", "cte_window", "agg",
            "self_dep", "tmp_chain", "lateral"} <= shapes
    # 每层都有表,层前缀可被 config.derive_layer 识别
    layers = {t["layer"] for t in data["tables"]}
    assert {"ods", "dwd", "dws", "ads", "dim", "tmp"} <= layers
    for t in data["tables"]:
        ncols = len(data["schema"][t["db"]][t["table"]])
        assert (10 <= ncols <= 40) or t["layer"] == "tmp"


def test_sample_50_parsable_rate_ge_95():
    data = generator.generate(tables=200, sqls=200, seed=42)
    run_benchmark._install_schema_cache()             # 提速,不改判定
    from pipeline.parser.core import parse_sql

    sample = random.Random(0).sample(data["corpus"], 50)
    ok = sum(1 for c in sample
             if parse_sql(c["sql"], dialect=c["dialect"],
                          schema=data["schema"]).status != "failed")
    assert ok / len(sample) >= 0.95
