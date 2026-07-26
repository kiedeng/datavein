#!/usr/bin/env python3
"""端到端自验/演示:用 warehouse_fixture 的 13 段 SQL 走完整金标准工作流。

流程(产物写 tools/goldstd/example/,可重复执行,先清空再生成):
  1. 导出 13 段 SQL 为 example/sql/*.sql + dialects.json + schema.json;
  2. extract.py 机器预填 example/annotations/*.yaml;
  3. 模拟"人工核对":全部置 verified: true,其中 1 段(02_etl_dwd_contract_n1)
     故意把 is_active 的来源列 end_date 改成 start_date —— 用于验证 compare
     能抓出错误(表现为 1 条缺失边 + 1 条多余边)并计入字段级准确率;
  4. compare.py 出 example/report.md。

用法:python tools/goldstd/make_example.py
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO / "lineage-pipeline" / "tests"))

import compare  # noqa: E402
import extract  # noqa: E402
from common import read_annotation, write_annotation  # noqa: E402
from warehouse_fixture import CORPUS, SCHEMA  # noqa: E402

# 故意改错的用例与边:is_active 的来源列 end_date → start_date
TAMPER_CASE = "02_etl_dwd_contract_n1"
TAMPER_DST, TAMPER_FROM, TAMPER_TO = "is_active", "end_date", "start_date"


def main() -> int:
    example = HERE / "example"
    if example.exists():
        shutil.rmtree(example)
    sql_dir = example / "sql"
    ann_dir = example / "annotations"
    sql_dir.mkdir(parents=True)

    # 1. 导出语料
    manifest = {}
    for i, case in enumerate(CORPUS, 1):
        case_id = f"{i:02d}_{case['task_id']}_n{case['node_seq']}"
        (sql_dir / f"{case_id}.sql").write_text(case["sql"].strip() + "\n",
                                                encoding="utf-8")
        manifest[case_id] = case["dialect"]
    (sql_dir / "dialects.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8")
    schema_path = example / "schema.json"
    schema_path.write_text(json.dumps(SCHEMA, ensure_ascii=False, indent=2),
                           encoding="utf-8")
    print(f"[example] 导出 {len(manifest)} 段 SQL -> {sql_dir}")

    # 2. 机器预填
    rc = extract.main([str(sql_dir), "--schema", str(schema_path),
                       "--out", str(ann_dir)])
    if rc != 0:
        return rc

    # 3. 模拟人工核对:全部 verified,1 段故意改错一条边
    for path in sorted(ann_dir.glob("*.yaml")):
        ann = read_annotation(path)
        ann["verified"] = True
        if ann["case_id"] == TAMPER_CASE:
            for edge in ann["column_edges"]:
                if edge["dst_col"] == TAMPER_DST and edge["src_col"] == TAMPER_FROM:
                    edge["src_col"] = TAMPER_TO
                    edge["expr"] = ""      # 表达式拿不准就留空(演示跳过表达式比对)
                    ann["notes"] = (f"演示用故意改错:{TAMPER_DST} 来源列 "
                                    f"{TAMPER_FROM} -> {TAMPER_TO},compare 应报"
                                    f" 1 缺失边 + 1 多余边")
        write_annotation(path, ann)
    print(f"[example] 模拟人工核对完成(全部 verified,{TAMPER_CASE} 故意改错一条边)")

    # 4. 比对出报告
    rc = compare.main([str(ann_dir), "--sql-dir", str(sql_dir),
                       "--schema", str(schema_path),
                       "--report", str(example / "report.md")])
    print(f"[example] compare 退出码 = {rc}(本例故意错 1 条边,字段级仍 >=95%,故为 0)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
