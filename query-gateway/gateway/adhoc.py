"""兜底通道:受控 Text-to-SQL(设计方案 7.3)。

调用方(MCP 层)负责检索圈定 2-5 表并传入;本模块负责:LLM 生成 →
静态校验(仅 SELECT/表集受限/黑名单/注入 LIMIT)→ EXPLAIN 预检 → 执行。
结果强制标注"自由查询",与语义层认证答案视觉区分。
LLM 客户端可插拔:行内为 new-api 的 OpenAI 兼容端点。
"""

import os
from typing import Protocol

import requests
from sqlglot import exp, parse_one

from . import config, executor


class LLMClient(Protocol):
    def generate_sql(self, question: str, context: str) -> str: ...


class HttpLLM:
    """OpenAI 兼容 /chat/completions(env:ADHOC_LLM_BASE_URL/KEY/MODEL)。"""

    def __init__(self):
        self.base = os.environ.get("ADHOC_LLM_BASE_URL", "")
        self.key = os.environ.get("ADHOC_LLM_API_KEY", "")
        self.model = os.environ.get("ADHOC_LLM_MODEL", "deepseek-chat")

    def generate_sql(self, question: str, context: str) -> str:
        if not self.base:
            raise RuntimeError("ADHOC_LLM_BASE_URL 未配置,兜底通道不可用")
        resp = requests.post(
            f"{self.base.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {self.key}"},
            json={"model": self.model, "temperature": 0,
                  "messages": [
                      {"role": "system",
                       "content": "你是取数 SQL 生成器。只输出一条 SELECT 语句,"
                                  "不加解释不加 markdown。只允许使用给定表。"},
                      {"role": "user", "content": f"{context}\n\n问题:{question}"}]},
            timeout=60)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip().strip("`")


def _build_context(conn, tables: list[str]) -> str:
    """DDL+注释上下文(few-shot 由 MCP 层按域追加)。"""
    parts = []
    with conn.cursor() as cur:
        for t in tables:
            cur.execute("""SELECT column_name, data_type, comment
                           FROM column_metadata WHERE full_name=%s""", (t,))
            cols = cur.fetchall()
            col_lines = "\n".join(
                f"  {c['column_name']} {c['data_type']} -- {c['comment'] or ''}"
                for c in cols)
            parts.append(f"表 {t}:\n{col_lines}")
    return "可用表结构:\n" + "\n\n".join(parts)


def run_adhoc(question: str, tables: list[str], user_ctx: dict,
              llm: LLMClient, platform_conn) -> dict:
    if not tables or len(tables) > 5:
        raise executor.ExecutionRejected("兜底通道需圈定 1-5 张表")
    for t in tables:
        if t in config.BLACKLIST_TABLES:
            raise executor.ExecutionRejected(f"表 {t} 在敏感黑名单")

    sql = llm.generate_sql(question, _build_context(platform_conn, tables))

    # 静态校验:表集合闭包在圈定范围内。
    # 收集全部表引用,含无库名前缀者——无前缀表若被默认库解析可读到圈外表,
    # 必须要求全限定名并纳入范围校验,不能因 t.text("db") 为空而漏检(安全走查修复)。
    tree = parse_one(sql, dialect=config.WAREHOUSE_DIALECT)
    if not isinstance(tree, exp.Select):
        raise executor.ExecutionRejected("生成结果不是 SELECT,已拒绝")
    unqualified = sorted({t.name for t in tree.find_all(exp.Table)
                          if not t.text("db")})
    if unqualified:
        raise executor.ExecutionRejected(
            f"生成的 SQL 含无库名前缀的表 {unqualified},拒绝执行(须使用 db.table 全限定名)")
    used = {f'{t.text("db")}.{t.name}' for t in tree.find_all(exp.Table)}
    illegal = used - set(tables)
    if illegal:
        raise executor.ExecutionRejected(f"SQL 使用了圈定范围外的表: {sorted(illegal)}")
    # 强制 LIMIT(无则注入)
    if tree.args.get("limit") is None:
        tree = tree.limit(config.HARD_LIMIT)
    sql = tree.sql(dialect=config.WAREHOUSE_DIALECT)

    result = executor.execute(sql)
    return {**result, "sql": sql,
            "channel_label": "自由查询",           # UI 必须视觉区分(7.3)
            "disclaimer": "此结果由受控 Text-to-SQL 生成,未经语义层口径认证,仅供参考"}
