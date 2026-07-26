"""向量构建流水线单测(设计方案 6.4):准入过滤/确定性嵌入/跨包一致性/diff 增量。

chromadb 相关用例在其不可用时 skip(内网可能没有,代码为 optional import)。
"""

import sys
from pathlib import Path

import pytest

from pipeline.vectorize import (GLOSSARY_COLLECTION, SCHEMA_COLLECTION,
                                HashEmbedder, admission_reason, build)

try:
    import chromadb
except ImportError:
    chromadb = None

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------- 入库准入过滤(6.4) ----------

def test_admission_filter():
    assert admission_reason("cust_no", None) == "empty"
    assert admission_reason("cust_no", "   ") == "empty"
    assert admission_reason("cust_no", "客户号") == "too_short"          # <4 字
    assert admission_reason("cust_no", "abc") == "too_short"
    # 注释=字段名或其简单大小写/分隔变体:无增量信息
    assert admission_reason("cust_no", "cust_no") == "no_increment"
    assert admission_reason("cust_no", "Cust_No") == "no_increment"
    assert admission_reason("cust_no", "CUST NO") == "no_increment"
    # 有增量信息的正常注释准入
    assert admission_reason("cust_no", "客户唯一编号") is None
    assert admission_reason("cust_no", "客户在核心系统的唯一编号") is None


# ---------- HashEmbedder:确定性 + 跨包规格一致(6.4) ----------

def test_hash_embedder_deterministic_unit_vector():
    emb = HashEmbedder()
    v1, v2 = emb.embed(["还款金额 还款明细宽表 dwd credit"])[0], \
        emb.embed(["还款金额 还款明细宽表 dwd credit"])[0]
    assert v1 == v2                                    # 确定性
    assert len(v1) == 256
    assert abs(sum(x * x for x in v1) - 1.0) < 1e-9    # 单位向量(cosine 空间)
    assert emb.embed(["完全不同的文本"])[0] != v1


def test_hash_embedder_matches_server_side():
    # 查询侧(mcp-server)与入库侧(pipeline)哈希规格必须一致,否则向量不可比
    sys.path.insert(0, str(REPO_ROOT / "lineage-mcp-server"))
    from server import vector as server_vector
    texts = ["授信金额", "客户在核心系统的唯一编号", "abc DEF 123"]
    assert HashEmbedder().embed(texts) == server_vector.HashEmbedder().embed(texts)
    assert GLOSSARY_COLLECTION == server_vector.GLOSSARY_COLLECTION
    assert SCHEMA_COLLECTION == server_vector.SCHEMA_COLLECTION


# ---------- 构建 + diff 增量(chromadb 内存模式) ----------

class _FakeCursor:
    def __init__(self, data):
        self._data = data
        self._rows = []

    def execute(self, sql, args=None):
        self._rows = (self._data["glossary"] if "biz_glossary" in sql
                      else self._data["columns"])

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    """只喂 build() 所需两条 SELECT 的桩连接(单测不依赖 MySQL)。"""

    def __init__(self, data):
        self._data = data

    def cursor(self):
        return _FakeCursor(self._data)


def _seed_data():
    return {
        "glossary": [
            dict(term_id=1, term="授信金额", caliber="当日审批通过申请金额合计",
                 domain="credit", owner="o1", ref_table="dws.dws_org_credit_day",
                 ref_column="credit_amt", certified=1),
            dict(term_id=2, term="还款金额", caliber="实收还款金额",
                 domain="credit", owner="o1", ref_table="dwd.dwd_repay_detail",
                 ref_column="repay_amt", certified=0),
        ],
        "columns": [
            dict(full_name="dwd.dwd_repay_detail", column_name="repay_amt",
                 comment="实际还款金额,含提前还款", table_comment="还款明细宽表",
                 layer="dwd", domain="credit", owner="o1"),
            dict(full_name="dwd.dwd_repay_detail", column_name="repay_id",
                 comment="", table_comment="还款明细宽表",
                 layer="dwd", domain="credit", owner="o1"),                # empty
            dict(full_name="dwd.dwd_repay_detail", column_name="dt",
                 comment="分区", table_comment="还款明细宽表",
                 layer="dwd", domain="credit", owner="o1"),                # too_short
            dict(full_name="dwd.dwd_repay_detail", column_name="contract_no",
                 comment="Contract_No", table_comment="还款明细宽表",
                 layer="dwd", domain="credit", owner="o1"),                # no_increment
        ],
    }


@pytest.mark.skipif(chromadb is None, reason="chromadb 不可用(optional 依赖)")
def test_build_admission_and_diff_incremental():
    client = chromadb.EphemeralClient()
    emb = HashEmbedder()
    data = _seed_data()

    stats = build(_FakeConn(data), client, emb)
    assert stats["glossary"] == {"upserted": 2, "deleted": 0, "unchanged": 0}
    assert stats["schema"] == {"upserted": 1, "deleted": 0, "unchanged": 0}
    assert stats["schema_skipped"] == {"empty": 1, "too_short": 1, "no_increment": 1}

    # 再跑一遍:注释 hash 未变,全部 unchanged(diff 增量)
    stats = build(_FakeConn(data), client, emb)
    assert stats["glossary"]["upserted"] == 0 and stats["glossary"]["unchanged"] == 2
    assert stats["schema"]["upserted"] == 0 and stats["schema"]["unchanged"] == 1

    # 改注释 → 仅该条重算;删除术语 → 从向量库删除
    data["columns"][0]["comment"] = "实际还款金额(口径修订:不含费用冲抵)"
    data["glossary"].pop(1)
    stats = build(_FakeConn(data), client, emb)
    assert stats["schema"]["upserted"] == 1
    assert stats["glossary"] == {"upserted": 0, "deleted": 1, "unchanged": 1}


@pytest.mark.skipif(chromadb is None, reason="chromadb 不可用(optional 依赖)")
def test_server_l3_search_over_built_collections():
    # 入库(pipeline)→ 查询(server L3):同 HashEmbedder 下相同语义文本应命中
    sys.path.insert(0, str(REPO_ROOT / "lineage-mcp-server"))
    from server import vector as server_vector

    client = chromadb.EphemeralClient()
    emb = HashEmbedder()
    build(_FakeConn(_seed_data()), client, emb)

    # HashEmbedder 是字面 n-gram 重合度(生产为 bge 语义向量),查询取近似口径文本
    hits = server_vector.search_similar("当日审批通过的授信金额合计",
                                        client=client, embedder=emb)
    assert hits and hits[0]["match_type"] == "vector_glossary"
    assert hits[0]["ref_table"] == "dws.dws_org_credit_day"
    assert hits[0]["confidence"] >= server_vector.MIN_CONFIDENCE
    assert "content_hash" not in hits[0]

    # 语义无关的查询:距离阈值(confidence≥0.6)拦截,不产生噪音候选
    assert server_vector.search_similar("完全无关的机房巡检记录",
                                        client=client, embedder=emb) == []
