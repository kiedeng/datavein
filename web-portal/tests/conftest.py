"""门户集成测试环境(真实 MySQL 8,DV_IT_MYSQL=1 门控)。

做法与 lineage-pipeline/tests/integration/test_e2e_mysql.py 一致:
建独立测试库 datavein_portal_test(避免与现有 E2E 的 datavein_test 冲突)
→ 执行 sql/ 全部 DDL → 用 warehouse_fixture 种子元数据与 SQL 语料
→ full_rebuild 生成真实血缘边/闭包 → TestClient 打门户 API。
LLM_BASE_URL 强制置空,覆盖降级模式(9.3)。
"""

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "web-portal"))      # `import portal` 与 cwd 无关

IT_ENABLED = os.environ.get("DV_IT_MYSQL") == "1"
TEST_DB = "datavein_portal_test"
DB_CONF = dict(
    host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "datavein"),
    password=os.environ.get("MYSQL_PASSWORD", "test_pw"),
    database=TEST_DB,
    charset="utf8mb4",
)

requires_mysql = pytest.mark.skipif(not IT_ENABLED,
                                    reason="需要 MySQL:DV_IT_MYSQL=1 开启")


def _load_warehouse_fixture():
    """按文件路径加载 lineage-pipeline 的种子语料,避免 tests 包名冲突。"""
    path = REPO_ROOT / "lineage-pipeline" / "tests" / "warehouse_fixture.py"
    spec = importlib.util.spec_from_file_location("dv_warehouse_fixture", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _seed(conn, fixture, pconfig) -> None:
    """种子元数据/schema 快照/SQL 仓库(与 E2E 的 _seed 同口径)。"""
    with conn.cursor() as cur:
        for database, tables in fixture.SCHEMA.items():
            for table, cols in tables.items():
                full = f"{database}.{table}"
                layer = pconfig.derive_layer(table) or (
                    "tmp" if database.startswith("tmp") else "")
                cur.execute(
                    """REPLACE INTO table_metadata
                       (full_name, db_name, table_name, table_type, layer, domain,
                        comment, owner, is_online, synced_at)
                       VALUES (%s,%s,%s,'table',%s,%s,%s,'tester',1,NOW())""",
                    (full, database, table, layer, database, f"{table} 测试表"))
                for col, typ in cols.items():
                    cur.execute(
                        """REPLACE INTO column_metadata
                           (full_name, column_name, data_type, comment)
                           VALUES (%s,%s,%s,%s)""", (full, col, typ, f"{col} 注释"))
                cur.execute(
                    """REPLACE INTO schema_snapshot (full_name, snap_date, columns_json)
                       VALUES (%s, CURDATE(), %s)""",
                    (full, json.dumps([{"name": c, "type": t}
                                       for c, t in cols.items()])))
        for c in fixture.CORPUS:
            cur.execute(
                """INSERT INTO sql_repository
                   (task_id, task_name, node_seq, target_table, sql_text, sql_hash,
                    dialect, owner, domain, parse_status, is_active, updated_at)
                   VALUES (%s,%s,%s,%s,%s,SHA2(%s,256),%s,'tester','credit','pending',1,NOW())""",
                (c["task_id"], c["task_id"], c["node_seq"], c["target"] or "",
                 c["sql"], c["sql"], c["dialect"]))
    conn.commit()


@pytest.fixture(scope="session")
def portal_env():
    """建库 + 种子 + full_rebuild + TestClient;整个测试会话复用一套环境。"""
    if not IT_ENABLED:
        pytest.skip("需要 MySQL:DV_IT_MYSQL=1 开启")
    os.environ.pop("LLM_BASE_URL", None)               # 强制降级模式(9.3)

    import pymysql
    admin = pymysql.connect(**{**DB_CONF, "database": None}, autocommit=True)
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB} CHARACTER SET utf8mb4")
    admin.close()

    import portal  # noqa: F401  触发 sys.path 注入 server/pipeline(portal/__init__.py)
    from pipeline import config as pconfig
    pconfig.MYSQL.update(DB_CONF)
    from server import config as sconfig
    from server import repo
    sconfig.MYSQL.update(DB_CONF)
    repo._pool = None                                  # 丢弃可能指向旧库的连接池

    from pipeline import db as pdb
    conn = pdb.connect()
    with conn.cursor() as cur:
        for ddl_file in sorted((REPO_ROOT / "sql").glob("0*.sql")):
            for stmt in ddl_file.read_text().split(";"):
                if stmt.strip():
                    cur.execute(stmt)
    conn.commit()

    _seed(conn, _load_warehouse_fixture(), pconfig)
    from pipeline import rebuild
    stats = rebuild.full_rebuild()
    assert stats["edges"] > 0, "血缘重建失败,无法继续门户测试"

    from portal import config as portal_config
    portal_config.LLM_BASE_URL = ""                    # 双保险:确保降级模式
    from fastapi.testclient import TestClient

    from portal.app import app
    client = TestClient(app)
    yield {"client": client, "conn": conn}
    conn.close()


def parse_sse(text: str) -> list[tuple[str, dict]]:
    """把 SSE 响应体解析为 [(event, payload)]。"""
    events = []
    for block in text.strip().split("\n\n"):
        event, data = "message", ""
        for line in block.split("\n"):
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data += line[5:].strip()
        if data:
            events.append((event, json.loads(data)))
    return events


def one(conn, sql: str, *args):
    conn.rollback()          # 刷新 REPEATABLE READ 快照,可见 API 侧连接的新提交
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()


def all_rows(conn, sql: str, *args):
    conn.rollback()          # 同上
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()
