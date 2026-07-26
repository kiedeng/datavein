#!/usr/bin/env python3
"""金标准抽检 · 第 3 步:自动比对与验收报告(设计方案 13 章口径)。

只取标注目录中 verified: true 的文件,按标注记录的方言重新解析对应 SQL,比对:
  - 表级准确率:target + sources 集合全对才算该用例正确(按用例计);
  - 字段级准确率:列边逐条按边计,accuracy = 匹配 / (匹配+缺失+多余+表达式不符);
  - 错误明细:缺失边 / 多余边 / 表达式不符,逐用例列出。

退出码:0 达标;1 表级 <99% 或字段级 <95%(可直接接 CI);2 输入/配置错误。

用法:
  python tools/goldstd/compare.py <ann_dir> --sql-dir <sql_dir> --schema schema.json \
      [--report gold_report.md] [--table-threshold 99] [--field-threshold 95]
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (CaseDiff, compare_case, fmt_edge, load_schema, parse_sql,  # noqa: E402
                    read_annotation)


def _pct(num: int, den: int) -> float:
    return 100.0 if den == 0 else 100.0 * num / den


def render_report(diffs: list[CaseDiff], unverified: int,
                  table_acc: float, field_acc: float,
                  table_thr: float, field_thr: float, passed: bool) -> str:
    table_ok = sum(1 for d in diffs if d.table_ok)
    matched = sum(d.matched for d in diffs)
    missing = sum(len(d.missing) for d in diffs)
    extra = sum(len(d.extra) for d in diffs)
    mismatch = sum(len(d.expr_mismatch) for d in diffs)
    lines = [
        "# 血缘金标准抽检报告",
        "",
        f"- 生成时间:{datetime.now():%Y-%m-%d %H:%M:%S}",
        f"- 口径:设计方案 13 章 —— 表级准确率 ≥{table_thr:g}%、字段级 ≥{field_thr:g}% 方可上线",
        f"- 参与比对用例(verified: true):{len(diffs)};未核对被忽略:{unverified}",
        "",
        "## 汇总",
        "",
        "| 指标 | 数值 | 阈值 | 结论 |",
        "| --- | --- | --- | --- |",
        f"| 表级准确率(按用例,target+sources 全对) | {table_ok}/{len(diffs)} = {table_acc:.2f}% "
        f"| ≥{table_thr:g}% | {'达标' if table_acc >= table_thr else '不达标'} |",
        f"| 字段级准确率(按边) | {matched}/{matched + missing + extra + mismatch} = {field_acc:.2f}% "
        f"| ≥{field_thr:g}% | {'达标' if field_acc >= field_thr else '不达标'} |",
        "",
        f"字段级明细:匹配 {matched} / 缺失边 {missing} / 多余边 {extra} / 表达式不符 {mismatch}"
        f"(分母 = 四者之和,即金标准边与机器边的并集口径)",
        "",
        f"**验收结论:{'通过' if passed else '不通过'}**",
        "",
        "## 用例明细",
        "",
        "| 用例 | 机器状态 | 表级 | 金标准边数 | 匹配 | 缺失 | 多余 | 表达式不符 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for d in diffs:
        lines.append(
            f"| {d.case_id} | {d.machine_status} | {'对' if d.table_ok else '错'} "
            f"| {d.gold_edge_count} | {d.matched} | {len(d.missing)} "
            f"| {len(d.extra)} | {len(d.expr_mismatch)} |")

    bad = [d for d in diffs if not d.table_ok or d.field_error_count]
    lines += ["", "## 错误明细", ""]
    if not bad:
        lines.append("无。")
    for d in bad:
        lines.append(f"### {d.case_id}")
        lines.append("")
        for msg in d.table_errors:
            lines.append(f"- [表级] {msg}")
        for key in d.missing:
            lines.append(f"- [缺失边] {fmt_edge(key)}(金标准有,机器未解析出)")
        for key in d.extra:
            lines.append(f"- [多余边] {fmt_edge(key)}(机器解析出,金标准无)")
        for key, gx, mx in d.expr_mismatch:
            lines.append(f"- [表达式不符] {fmt_edge(key)}:金标准含 `{gx}`,机器为 `{mx}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="金标准比对与验收报告(13 章口径)")
    ap.add_argument("ann_dir", help="标注 YAML 目录(仅取 verified: true)")
    ap.add_argument("--sql-dir", required=True, help="SQL 文件目录(标注内 sql_file 的相对根)")
    ap.add_argument("--schema", required=True, help="schema 快照 JSON")
    ap.add_argument("--report", default="gold_report.md", help="markdown 报告输出路径")
    ap.add_argument("--table-threshold", type=float, default=99.0)
    ap.add_argument("--field-threshold", type=float, default=95.0)
    args = ap.parse_args(argv)

    ann_dir, sql_dir = Path(args.ann_dir), Path(args.sql_dir)
    ann_files = sorted(ann_dir.glob("*.yaml")) + sorted(ann_dir.glob("*.yml"))
    if not ann_files:
        print(f"[compare] 错误: {ann_dir} 下没有标注文件", file=sys.stderr)
        return 2
    schema = load_schema(args.schema)

    diffs: list[CaseDiff] = []
    unverified = 0
    for path in ann_files:
        ann = read_annotation(path)
        if not ann.get("verified"):
            unverified += 1
            continue
        sql_path = sql_dir / (ann.get("sql_file") or f"{path.stem}.sql")
        if not sql_path.exists():
            print(f"[compare] 错误: 标注 {path.name} 指向的 SQL 不存在: {sql_path}",
                  file=sys.stderr)
            return 2
        result = parse_sql(sql_path.read_text(encoding="utf-8"),
                           dialect=ann.get("dialect") or "hive", schema=schema)
        diffs.append(compare_case(ann, result))

    if not diffs:
        print("[compare] 错误: 没有任何 verified: true 的标注,先完成人工核对", file=sys.stderr)
        return 2

    table_acc = _pct(sum(1 for d in diffs if d.table_ok), len(diffs))
    matched = sum(d.matched for d in diffs)
    errors = sum(d.field_error_count for d in diffs)
    field_acc = _pct(matched, matched + errors)
    passed = table_acc >= args.table_threshold and field_acc >= args.field_threshold

    report = render_report(diffs, unverified, table_acc, field_acc,
                           args.table_threshold, args.field_threshold, passed)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")

    print(f"[compare] 用例 {len(diffs)}(忽略未核对 {unverified})| "
          f"表级 {table_acc:.2f}%(阈值 {args.table_threshold:g}%)| "
          f"字段级 {field_acc:.2f}%(阈值 {args.field_threshold:g}%)| "
          f"{'通过' if passed else '不通过'} | 报告: {report_path}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
