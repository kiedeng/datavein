#!/usr/bin/env python3
"""金标准抽检 · 第 0 步:从 sql_repository 分层抽样导出 .sql 文件目录。

按 层(目标表前缀推断)× 形态(启发式)分层,比例分配(每个非空层至少 1 条,
最大余数法),把抽中的 SQL 导出为 <out>/<case_id>.sql + dialects.json 方言清单,
可直接接 extract.py。

形态启发式(优先级从高到低,取第一个命中):
  multi_insert(FROM ... INSERT ...)> self_dep(目标表出现在 FROM/JOIN)
  > lateral_view > cte(WITH)> window(OVER)> join > simple

用法:
  python tools/goldstd/sample.py --out sample_sql [--total 100] [--seed 42] \
      [--host 127.0.0.1] [--port 3306] [--user datavein] [--password test_pw] [--db datavein]
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.config import derive_layer

_MULTI_INSERT_RE = re.compile(r"^\s*FROM\b.*\bINSERT\b", re.I | re.S)
_CTE_RE = re.compile(r"\bWITH\s+[A-Za-z0-9_]+\s+AS\s*\(", re.I)
_SRC_TABLE_RE = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z0-9_.]+)", re.I)


def classify_shape(sql: str, target_table: str) -> str:
    """SQL 形态启发式判定(仅用于分层,不追求解析级精确)。"""
    if _MULTI_INSERT_RE.match(sql):
        return "multi_insert"
    target = (target_table or "").strip().lower()
    if target:
        srcs = {m.group(1).lower() for m in _SRC_TABLE_RE.finditer(sql)}
        if target in srcs or target.split(".")[-1] in srcs:
            return "self_dep"
    upper = sql.upper()
    if "LATERAL VIEW" in upper:
        return "lateral_view"
    if _CTE_RE.search(sql):
        return "cte"
    if re.search(r"\bOVER\s*\(", upper):
        return "window"
    if re.search(r"\bJOIN\b", upper):
        return "join"
    return "simple"


def classify_layer(target_table: str) -> str:
    name = (target_table or "").split(".")[-1]
    return derive_layer(name) or "other"


def allocate(strata: dict[tuple, list], total: int) -> dict[tuple, int]:
    """比例分配(最大余数法),每个非空层至少 1 条;总量不足层数时按层大小取前 total 层。"""
    sizes = {k: len(v) for k, v in strata.items()}
    n_all = sum(sizes.values())
    if total >= n_all:
        return dict(sizes)
    if total < len(sizes):
        picked = sorted(sizes, key=lambda k: (-sizes[k], k))[:total]
        return {k: 1 for k in picked}
    quotas, remainders = {}, []
    budget = total - len(sizes)                     # 先给每层保底 1
    for k, n in sizes.items():
        exact = budget * (n / n_all)
        quotas[k] = 1 + int(exact)
        remainders.append((exact - int(exact), k))
    left = total - sum(quotas.values())
    for _, k in sorted(remainders, reverse=True)[:max(left, 0)]:
        quotas[k] += 1
    for k in quotas:                                # 不超过层内实际数量,余量回补
        quotas[k] = min(quotas[k], sizes[k])
    while sum(quotas.values()) < total:
        grown = False
        for k in sorted(sizes, key=lambda k: -sizes[k]):
            if quotas[k] < sizes[k]:
                quotas[k] += 1
                grown = True
                break
        if not grown:
            break
    return quotas


def _sanitize(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", s or "unknown").strip("_")[:60]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="sql_repository 分层抽样导出")
    ap.add_argument("--out", required=True, help="导出目录(.sql + dialects.json)")
    ap.add_argument("--total", type=int, default=100, help="抽样总量(默认 100,13 章口径)")
    ap.add_argument("--seed", type=int, default=42, help="随机种子(可复现抽样)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="datavein")
    ap.add_argument("--password", default="test_pw")
    ap.add_argument("--db", default="datavein")
    args = ap.parse_args(argv)

    import pymysql
    try:
        conn = pymysql.connect(host=args.host, port=args.port, user=args.user,
                               password=args.password, database=args.db,
                               charset="utf8mb4",
                               cursorclass=pymysql.cursors.DictCursor,
                               connect_timeout=5)
    except Exception as e:
        print(f"[sample] 错误: 连接 MySQL 失败({args.user}@{args.host}:{args.port}/{args.db}):{e}\n"
              f"  请确认:① MySQL 已启动(docker-compose up mysql);② 账号口令正确;"
              f"③ sql_repository 已由 sql_collector 灌数。无库环境可跳过本步,"
              f"手工准备 .sql 目录后直接走 extract.py。", file=sys.stderr)
        return 2

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sql_id, task_id, node_seq, target_table, sql_text, "
                        "COALESCE(dialect,'hive') AS dialect "
                        "FROM sql_repository WHERE is_active=1 AND sql_text IS NOT NULL")
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        print("[sample] 错误: sql_repository 无 active SQL,请先运行收集(sql_collector)",
              file=sys.stderr)
        return 2

    strata: dict[tuple, list] = defaultdict(list)
    for r in rows:
        key = (classify_layer(r["target_table"]), classify_shape(r["sql_text"],
                                                                 r["target_table"]))
        strata[key].append(r)

    rng = random.Random(args.seed)
    quotas = allocate(strata, args.total)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, str] = {}
    print(f"[sample] 库内 active SQL {len(rows)} 条,层×形态 共 {len(strata)} 层,"
          f"抽样 {sum(quotas.values())} 条(seed={args.seed}):")
    for key in sorted(strata):
        picked = rng.sample(strata[key], quotas.get(key, 0))
        print(f"  {key[0]:>6} × {key[1]:<12} 总 {len(strata[key]):>4} 抽 {len(picked)}")
        for r in picked:
            case_id = f"{r['sql_id']:06d}_{_sanitize(r['task_id'])}_n{r['node_seq']}"
            (out_dir / f"{case_id}.sql").write_text(r["sql_text"], encoding="utf-8")
            manifest[case_id] = r["dialect"]

    (out_dir / "dialects.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8")
    print(f"[sample] 已导出 {len(manifest)} 段到 {out_dir}(含 dialects.json)。"
          f"下一步:python tools/goldstd/extract.py {out_dir} --schema <schema.json> --out <ann_dir>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
