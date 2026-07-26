"""三级检索之 L3 向量层(设计方案 6.1/6.4)。

Embedder 抽象:
- HashEmbedder:确定性字符 n-gram 哈希嵌入(测试/无模型环境),规格必须与
  lineage-pipeline/pipeline/vectorize.py 完全一致——查询向量与入库向量同源
  才可比(lineage-pipeline/tests/test_vectorize.py 有跨包一致性单测);
- HttpEmbedder:bge 服务化端点(行内部署),POST EMBEDDING_URL
  {"texts": [...]} → {"vectors": [[...]]}。

Chroma 客户端 optional import(行内内网可能没有 chromadb):不可用/未配置时
返回 None,search_similar 静默返回空——L3 是最后兜底,不影响 L1/L2(6.4)。
双 collection(glossary/schema)各取 top5,cosine 距离转置信度
(confidence = 1 - distance),≥0.6 才采纳(6.1)。

环境变量:EMBEDDING_URL / CHROMA_HOST / CHROMA_PORT。
"""

import hashlib
import logging
import math
import os

log = logging.getLogger(__name__)

try:                                        # optional import(6.4)
    import chromadb
except ImportError:                         # pragma: no cover
    chromadb = None

GLOSSARY_COLLECTION = "dv_glossary"
SCHEMA_COLLECTION = "dv_schema"
TOP_K = 5                                   # 双 collection 各 top5(6.1)
MIN_CONFIDENCE = 0.6                        # 距离阈值转置信度下限(6.1)

# ---- Embedder(与 pipeline/vectorize.py 规格保持一致) ----

_DIM = 256
_NGRAMS = (1, 2, 3)


def _hash_vec(text: str) -> list[float]:
    """确定性字符 n-gram 哈希 → 256 维单位向量(签名哈希,cosine 空间)。"""
    vec = [0.0] * _DIM
    t = "".join((text or "").lower().split())
    for n in _NGRAMS:
        for i in range(len(t) - n + 1):
            h = int.from_bytes(
                hashlib.md5(t[i:i + n].encode("utf-8")).digest()[:8], "big")
            vec[h % _DIM] += 1.0 if (h >> 8) % 2 == 0 else -1.0
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0:
        vec[0] = 1.0
        norm = 1.0
    return [v / norm for v in vec]


class HashEmbedder:
    """确定性哈希嵌入:测试与无模型环境用。"""

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [_hash_vec(t) for t in texts]


class HttpEmbedder:
    """bge 服务化端点(行内部署)。"""

    def __init__(self, url: str, timeout: int = 30):
        self.url = url
        self.timeout = timeout

    def embed(self, texts: list[str]) -> list[list[float]]:
        import requests
        resp = requests.post(self.url, json={"texts": texts}, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()["vectors"]


def get_embedder():
    url = os.environ.get("EMBEDDING_URL", "")
    return HttpEmbedder(url) if url else HashEmbedder()


def get_chroma_client():
    """Chroma HTTP 客户端;chromadb 未安装/未配置/不可达时返回 None 静默跳过。"""
    if chromadb is None:
        return None
    host = os.environ.get("CHROMA_HOST", "")
    if not host:
        return None
    try:
        return chromadb.HttpClient(host=host,
                                   port=int(os.environ.get("CHROMA_PORT", "8000")))
    except Exception as e:
        log.debug("Chroma 不可用,L3 跳过: %s", e)
        return None


def search_similar(keyword: str, client=None, embedder=None) -> list[dict]:
    """L3 向量检索:glossary/schema 双 collection top5,confidence≥0.6 采纳(6.1)。

    返回候选 dict(携带入库 metadata:ref_table/full_name/column_name/domain 等),
    可直接进 repo._merge_families 归族。Chroma 不可用返回空列表。
    """
    client = client or get_chroma_client()
    if client is None:
        return []
    embedder = embedder or get_embedder()
    try:
        qvec = embedder.embed([keyword])[0]
    except Exception as e:                  # embedding 端点故障不炸检索,降级 L1/L2
        log.warning("embedding 失败,L3 跳过: %s", e)
        return []
    out: list[dict] = []
    for cname, mtype in ((GLOSSARY_COLLECTION, "vector_glossary"),
                         (SCHEMA_COLLECTION, "vector_schema")):
        try:
            col = client.get_collection(cname)
            res = col.query(query_embeddings=[qvec], n_results=TOP_K,
                            include=["metadatas", "distances"])
        except Exception as e:              # collection 未构建视为无候选
            log.debug("collection %s 查询跳过: %s", cname, e)
            continue
        for meta, dist in zip(res["metadatas"][0], res["distances"][0]):
            confidence = 1.0 - float(dist)  # cosine 距离 → 置信度
            if confidence >= MIN_CONFIDENCE:
                cand = {k: v for k, v in (meta or {}).items() if k != "content_hash"}
                out.append({**cand, "match_type": mtype,
                            "confidence": round(confidence, 4)})
    return out
