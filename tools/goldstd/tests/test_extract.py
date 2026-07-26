"""extract.py:预填内容正确性 + 幂等保护(verified 永不覆盖)。"""

from common import read_annotation, write_annotation
from conftest import run_extract


def test_prefill_content(workspace):
    assert run_extract(workspace) == 0
    ann = read_annotation(workspace["ann_dir"] / "case1.yaml")
    assert ann["verified"] is False
    assert ann["machine_status"] == "success"
    assert ann["target_table"] == "db1.t_dst"
    assert ann["table_sources"] == ["db1.t_src"]
    edges = {(e["dst_col"], e["src_table"], e["src_col"]) for e in ann["column_edges"]}
    assert edges == {("a", "db1.t_src", "a"), ("b", "db1.t_src", "b")}


def test_header_comment_present(workspace):
    run_extract(workspace)
    text = (workspace["ann_dir"] / "case1.yaml").read_text(encoding="utf-8")
    assert text.startswith("#")
    assert "verified: true" in text        # 头注释里说明了核对方法
    assert "直接改数据" in text


def test_verified_never_overwritten(workspace):
    run_extract(workspace)
    path = workspace["ann_dir"] / "case1.yaml"
    ann = read_annotation(path)
    ann["verified"] = True
    ann["target_table"] = "db1.human_edited"     # 人工改过的数据
    write_annotation(path, ann)

    assert run_extract(workspace) == 0           # 重跑
    assert run_extract(workspace, "--force") == 0  # 连 --force 也不能覆盖 verified
    ann2 = read_annotation(path)
    assert ann2["verified"] is True
    assert ann2["target_table"] == "db1.human_edited"


def test_existing_unverified_skipped_unless_force(workspace):
    run_extract(workspace)
    path = workspace["ann_dir"] / "case1.yaml"
    ann = read_annotation(path)
    ann["notes"] = "人工改到一半"
    write_annotation(path, ann)

    run_extract(workspace)                       # 默认不动未 verified 的已有文件
    assert read_annotation(path)["notes"] == "人工改到一半"
    run_extract(workspace, "--force")            # --force 重新生成
    assert read_annotation(path)["notes"] == ""


def test_empty_sql_dir_errors(workspace):
    for f in workspace["sql_dir"].glob("*.sql"):
        f.unlink()
    assert run_extract(workspace) == 2
