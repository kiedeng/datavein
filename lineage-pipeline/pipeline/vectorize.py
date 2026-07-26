"""向量构建流水线(设计方案 6.4)。

biz_glossary(term+caliber)与 column_metadata 注释 → Chroma 双 collection
(glossary/schema,6.1):
- 入库准入过滤:注释为空/去空格后<4字/与字段名无增量信息(等于字段名或其
  简单大小写变体)的字段跳过并计数——百万级噪音向量不进库,L1/L2 仍可命中;
- embedding 输入拼接:字段注释+表注释+layer+domain,提升短注释语义区分度;
- 按 (full_name, column, 注释hash) 做 diff 增量:hash 未变不重算,
  条目消失或不再准入则从向量库删除。

chromadb 为 optional import(行内内网可能没有):不可用时 run() 静默跳过。
注意:HashEmbedder 的 n-gram 哈希规格必须与 lineage-mcp-server/server/vector.py
完全一致,否则查询向量与入库向量不可比(tests/test_vectorize.py 有跨包一致性单测)。
"""

import hashlib
import logging
import math

from . import config

log = logging.getLogger(__name__)

try:                                        # optional import(6.4)
    import chromadb
except ImportError:                         # pragma: no cover
    chromadb = None

GLOSSARY_COLLECTION = "dv_glossary"
SCHEMA_COLLECTION = "dv_schema"

# ---- Embedder(与 server/vector.py 规格保持一致) ----

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
    """确定性哈希嵌入:测试与无模型环境用(6.4)。"""

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [_hash_vec(t) for t in texts]


class HttpEmbedder:
    """bge 服务化端点(行内部署):POST {"texts": [...]} → {"vectors": [[...]]}。"""

    def __init__(self, url: str, timeout: int = 30):
        self.url = url
        self.timeout = timeout

    def embed(self, texts: list[str]) -> list[list[float]]:
        import requests
        resp = requests.post(self.url, json={"texts": texts}, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()["vectors"]


# ---- 入库准入(6.4) ----

def _norm_name(s: str) -> str:
    return s.replace("_", "").replace("-", "").replace(" ", "").lower()


def admission_reason(column_name: str, comment: str) -> str | None:
    """字段注释准入判定:返回 None 表示准入,否则返回跳过原因码。"""
    c = (comment or "").strip()
    if not c:
        return "empty"
    if len(c) < 4:
        return "too_short"
    if _norm_name(c) == _norm_name(column_name or ""):
        return "no_increment"                # 注释=字段名(或大小写/分隔变体),无增量信息
    return None


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


# ---- 构建与 diff 增量 ----

def _sync_collection(col, desired: dict, embedder, stat: dict, batch: int):
    """desired: {id: (embed_text, metadata)};按 content_hash diff 增量写入。"""
    got = col.get(include=["metadatas"])
    existing = {cid: (meta or {}).get("content_hash", "")
                for cid, meta in zip(got["ids"], got["metadatas"] or [])}
    to_delete = [cid for cid in existing if cid not in desired]
    changed = [cid for cid, (_, meta) in desired.items()
               if existing.get(cid) != meta["content_hash"]]
    for i in range(0, len(to_delete), batch):
        col.delete(ids=to_delete[i:i + batch])
    for i in range(0, len(changed), batch):
        ids = changed[i:i + batch]
        texts = [desired[c][0] for c in ids]
        col.upsert(ids=ids, embeddings=embedder.embed(texts), documents=texts,
                   metadatas=[desired[c][1] for c in ids])
    stat.update(upserted=len(changed), deleted=len(to_delete),
                unchanged=len(desired) - len(changed))


def build(conn, client, embedder, batch: int = 200) -> dict:
    """全量构建入口(首次离线跑,此后按元数据 diff 增量,6.1/6.4)。"""
    stats = {"glossary": {}, "schema": {},
             "schema_skipped": {"empty": 0, "too_short": 0, "no_increment": 0}}
    g_col = client.get_or_create_collection(
        GLOSSARY_COLLECTION, metadata={"hnsw:space": "cosine"})
    s_col = client.get_or_create_collection(
        SCHEMA_COLLECTION, metadata={"hnsw:space": "cosine"})

    # glossary:term + caliber
    with conn.cursor() as cur:
        cur.execute("""SELECT term_id, term, COALESCE(caliber,'') AS caliber,
                              COALESCE(domain,'') AS domain, COALESCE(owner,'') AS owner,
                              COALESCE(ref_table,'') AS ref_table,
                              COALESCE(ref_column,'') AS ref_column,
                              COALESCE(certified,0) AS certified
                       FROM biz_glossary WHERE status='active'""")
        g_rows = cur.fetchall()
    desired_g = {}
    for r in g_rows:
        text = f"{r['term']} {r['caliber']}".strip()
        desired_g[f"g{r['term_id']}"] = (text, {
            "term": r["term"], "caliber": r["caliber"][:500], "domain": r["domain"],
            "owner": r["owner"], "ref_table": r["ref_table"],
            "ref_column": r["ref_column"], "certified": int(r["certified"]),
            "content_hash": _sha1(text)})
    _sync_collection(g_col, desired_g, embedder, stats["glossary"], batch)

    # schema:准入过滤 + 拼接(字段注释+表注释+layer+domain)
    with conn.cursor() as cur:
        cur.execute("""SELECT c.full_name, c.column_name,
                              COALESCE(c.comment,'') AS comment,
                              COALESCE(t.comment,'') AS table_comment,
                              COALESCE(t.layer,'') AS layer,
                              COALESCE(t.domain,'') AS domain,
                              COALESCE(t.owner,'') AS owner
                       FROM column_metadata c
                       JOIN table_metadata t ON t.full_name = c.full_name
                       WHERE t.is_online = 1""")
        c_rows = cur.fetchall()
    desired_s = {}
    for r in c_rows:
        reason = admission_reason(r["column_name"], r["comment"])
        if reason:
            stats["schema_skipped"][reason] += 1
            continue
        comment = r["comment"].strip()
        text = " ".join(x for x in (comment, r["table_comment"].strip(),
                                    r["layer"], r["domain"]) if x)
        cid = f"{r['full_name']}::{r['column_name']}"
        desired_s[cid] = (text, {
            "full_name": r["full_name"], "column_name": r["column_name"],
            "comment": comment[:500], "layer": r["layer"], "domain": r["domain"],
            "owner": r["owner"],
            "content_hash": _sha1(f"{r['full_name']}|{r['column_name']}|{comment}")})
    _sync_collection(s_col, desired_s, embedder, stats["schema"], batch)
    return stats


def get_chroma_client():
    """Chroma HTTP 客户端;chromadb 未安装或未配置时返回 None 静默跳过(6.4)。"""
    if chromadb is None:
        log.info("chromadb 不可用,vectorize 跳过")
        return None
    if not config.CHROMA_HOST:
        log.info("CHROMA_HOST 未配置,vectorize 跳过")
        return None
    try:
        return chromadb.HttpClient(host=config.CHROMA_HOST, port=config.CHROMA_PORT)
    except Exception as e:                  # 服务不可达同样静默跳过,不阻塞流水线
        log.warning("Chroma 连接失败,vectorize 跳过: %s", e)
        return None


def get_embedder():
    if config.EMBEDDING_URL:
        return HttpEmbedder(config.EMBEDDING_URL)
    return HashEmbedder()


def run(conn) -> dict:
    """cli vectorize 子命令入口。"""
    client = get_chroma_client()
    if client is None:
        return {"skipped": "chroma_unavailable",
                "hint": "安装 chromadb 并配置 CHROMA_HOST/CHROMA_PORT"}
    return build(conn, client, get_embedder())
