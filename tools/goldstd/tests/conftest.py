import sys
from pathlib import Path

import pytest

# 让 tests 能直接 import extract/compare/common(脚本目录,非包)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

SCHEMA = {
    "db1": {
        "t_src": {"a": "int", "b": "int", "dt": "string"},
        "t_dst": {"a": "int", "b": "int"},
    }
}
SQL = "INSERT OVERWRITE TABLE db1.t_dst SELECT a, b FROM db1.t_src WHERE dt = '2026-07-25'\n"


@pytest.fixture
def workspace(tmp_path):
    """一个可用的最小工作区:1 段 SQL + schema 快照 + 空标注目录。"""
    import json

    sql_dir = tmp_path / "sql"
    ann_dir = tmp_path / "ann"
    sql_dir.mkdir()
    (sql_dir / "case1.sql").write_text(SQL, encoding="utf-8")
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(SCHEMA), encoding="utf-8")
    return dict(sql_dir=sql_dir, ann_dir=ann_dir, schema=schema_path,
                report=tmp_path / "report.md")


def run_extract(ws, *flags):
    import extract
    return extract.main([str(ws["sql_dir"]), "--schema", str(ws["schema"]),
                         "--out", str(ws["ann_dir"]), *flags])


def run_compare(ws, **kw):
    import compare
    argv = [str(ws["ann_dir"]), "--sql-dir", str(ws["sql_dir"]),
            "--schema", str(ws["schema"]), "--report", str(ws["report"])]
    for k, v in kw.items():
        argv += [f"--{k.replace('_', '-')}", str(v)]
    return compare.main(argv)
