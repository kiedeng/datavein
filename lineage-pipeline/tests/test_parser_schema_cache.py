"""解析器 schema 复用回归(压测发现的性能缺陷修复,不得回退)。

背景:parse_sql 把原始 dict schema 直接交给 sqlglot 时,qualify 与逐列 lineage
各自重建 MappingSchema,单段 SQL 重建 N+1 次,解析耗时随 schema 大小放大——
5 万表规模会击穿全量重建 <1h 承诺。修复为按 (schema 身份, dialect) 缓存复用。
本测试锁定:① 复用不改变解析结果;② 缓存确实命中;③ 传入预构建 MappingSchema 等价。
"""

from sqlglot.schema import MappingSchema

from pipeline.parser import core
from pipeline.parser.core import parse_sql

SCHEMA = {"ods": {"t_apply": {"apply_id": "bigint", "cust_no": "string",
                              "apply_amt": "decimal(18,2)", "dt": "string"}}}
SQL = ("INSERT INTO dwd.d_apply SELECT apply_id, cust_no, apply_amt*100 AS amt "
       "FROM ods.t_apply WHERE dt='2026-01-01'")


def _edges_key(r):
    return sorted((e.edge_level, e.src.table, e.src.column, e.dst.column,
                   e.transform_expr) for e in r.edges)


def test_cache_hit_same_schema_object():
    core._MS_CACHE.clear()
    parse_sql(SQL, dialect="hive", schema=SCHEMA)
    cached = core._MS_CACHE.get("hive")
    assert cached is not None and cached[0] is SCHEMA        # 身份缓存已建立
    parse_sql(SQL, dialect="hive", schema=SCHEMA)
    assert core._MS_CACHE["hive"][0] is SCHEMA               # 第二次复用同一实例


def test_result_identical_dict_vs_prebuilt_mappingschema():
    r_dict = parse_sql(SQL, dialect="hive", schema=SCHEMA)
    r_ms = parse_sql(SQL, dialect="hive",
                     schema=MappingSchema(SCHEMA, dialect="hive"))
    assert r_dict.status == r_ms.status == "success"
    assert _edges_key(r_dict) == _edges_key(r_ms)            # 结果零差异


def test_cache_keyed_by_dialect():
    core._MS_CACHE.clear()
    parse_sql(SQL, dialect="hive", schema=SCHEMA)
    parse_sql(SQL, dialect="spark", schema=SCHEMA)
    assert {"hive", "spark"} <= set(core._MS_CACHE)          # 每方言各一份
