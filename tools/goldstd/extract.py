#!/usr/bin/env python3
"""金标准抽检 · 第 1 步:机器预填标注。

输入:SQL 文件目录(每段一个 .sql,文件名即用例 id)+ schema 快照(JSON,
格式同 pipeline 的 {db: {table: {col: type}}});逐段跑解析器,把结果预填成
标注 YAML(一 SQL 一文件),供人工核对改错(第 2 步 compare 只取 verified: true)。

幂等保护:
  - 已 verified: true 的标注文件永不覆盖(含 --force);
  - 已存在但未 verified 的默认跳过(保护人工改到一半的文件),--force 才重新生成。

方言:优先取 SQL 目录内 dialects.json 的 {case_id: dialect},否则用 --dialect。

用法:
  python tools/goldstd/extract.py <sql_dir> --schema schema.json --out <ann_dir> [--dialect hive] [--force]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (annotation_from_result, load_dialect_manifest, load_schema,  # noqa: E402
                    parse_sql, read_annotation, write_annotation)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="金标准标注机器预填")
    ap.add_argument("sql_dir", help="SQL 文件目录(每段一个 .sql,文件名即用例 id)")
    ap.add_argument("--schema", required=True, help="schema 快照 JSON({db:{table:{col:type}}})")
    ap.add_argument("--out", required=True, help="标注 YAML 输出目录")
    ap.add_argument("--dialect", default="hive", help="默认方言(dialects.json 可按用例覆盖)")
    ap.add_argument("--force", action="store_true",
                    help="重新生成未 verified 的已有标注(verified: true 永不覆盖)")
    args = ap.parse_args(argv)

    sql_dir, out_dir = Path(args.sql_dir), Path(args.out)
    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files:
        print(f"[extract] 错误: {sql_dir} 下没有 .sql 文件", file=sys.stderr)
        return 2
    schema = load_schema(args.schema)
    dialects = load_dialect_manifest(sql_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    created = skipped_verified = skipped_existing = failed = 0
    for sql_file in sql_files:
        case_id = sql_file.stem
        ann_path = out_dir / f"{case_id}.yaml"
        if ann_path.exists():
            old = read_annotation(ann_path)
            if old.get("verified"):
                skipped_verified += 1
                print(f"  [skip] {case_id}: 已 verified,永不覆盖")
                continue
            if not args.force:
                skipped_existing += 1
                print(f"  [skip] {case_id}: 标注已存在(未 verified),--force 可重新生成")
                continue
        dialect = dialects.get(case_id, args.dialect)
        result = parse_sql(sql_file.read_text(encoding="utf-8"),
                           dialect=dialect, schema=schema)
        ann = annotation_from_result(case_id, sql_file.name, dialect, result)
        write_annotation(ann_path, ann)
        created += 1
        if result.status == "failed":
            failed += 1
        print(f"  [pre ] {case_id}: {result.status}"
              f"{'/' + result.reason if result.reason else ''}"
              f" 边数={len(ann['column_edges'])}")

    print(f"[extract] 完成: 预填 {created}(其中机器解析失败 {failed}),"
          f"跳过已 verified {skipped_verified},跳过已存在 {skipped_existing}。"
          f"下一步:人工核对 {out_dir} 下的 YAML,确认后置 verified: true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
