"""M1 全链路 E2E 集成测试(真实 MySQL 8)。

链路:建库(sql/ 全部 DDL)→ 种子元数据/schema快照/SQL仓库(warehouse_fixture)
→ full_rebuild(并行解析+影子表切换+闭包)→ 血缘/闭包/影响分析/MCP查询验证
→ 增量解析 → 闭包修补验证。

运行:DV_IT_MYSQL=1 python -m pytest tests/integration -q
连接:MYSQL_HOST/MYSQL_USER/MYSQL_PASSWORD 环境变量,默认本机 datavein/test_pw。
"""

import json
import os
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("DV_IT_MYSQL") != "1",
                                reason="需要 MySQL:DV_IT_MYSQL=1 开启")

REPO_ROOT = Path(__file__).resolve().parents[3]
TEST_DB = os.environ.get("MYSQL_DB", "datavein_test")
DB_CONF = dict(
    host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "datavein"),
    password=os.environ.get("MYSQL_PASSWORD", "test_pw"),
    database=TEST_DB,
    charset="utf8mb4",
)


@pytest.fixture(scope="module")
def env():
    """建库 + 打 DDL + 种子数据;返回已配置好的 pipeline 模块集。"""
    import pymysql

    admin = pymysql.connect(**{**DB_CONF, "database": None}, autocommit=True)
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB} CHARACTER SET utf8mb4")
    admin.close()

    from pipeline import config
    config.MYSQL.update(DB_CONF)
    config.PARALLEL_WORKERS = 4
    config.CROSSCHECK_ENABLED = True

    sys.path.insert(0, str(REPO_ROOT / "lineage-mcp-server"))
    from server import config as server_config
    server_config.MYSQL.update(DB_CONF)

    from pipeline import db
    conn = db.connect()
    with conn.cursor() as cur:
        for ddl_file in sorted((REPO_ROOT / "sql").glob("0*.sql")):
            for stmt in ddl_file.read_text().split(";"):
                if stmt.strip():
                    cur.execute(stmt)
    conn.commit()

    _seed(conn)
    yield {"conn": conn, "db": db}


def _seed(conn):
    from tests.warehouse_fixture import CORPUS, SCHEMA

    from pipeline import config as pconfig
    with conn.cursor() as cur:
        for database, tables in SCHEMA.items():
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
        for c in CORPUS:
            cur.execute(
                """INSERT INTO sql_repository
                   (task_id, task_name, node_seq, target_table, sql_text, sql_hash,
                    dialect, owner, domain, parse_status, is_active, updated_at)
                   VALUES (%s,%s,%s,%s,%s,SHA2(%s,256),%s,'tester','credit','pending',1,NOW())""",
                (c["task_id"], c["task_id"], c["node_seq"], c["target"] or "",
                 c["sql"], c["sql"], c["dialect"]))
    conn.commit()


def _one(conn, sql, *args):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone()


def test_10_full_rebuild(env):
    from pipeline import rebuild
    stats = rebuild.full_rebuild()
    assert stats["parse"]["success"] == 12
    assert stats["parse"]["failed"] == 1          # multi-insert 按设计失败
    assert stats["edges"] > 0 and stats["closure_rows"] > 0

    conn = env["conn"]
    # multi-insert 原因码单列(5.8)
    row = _one(conn, """SELECT parse_reason FROM sql_repository
                        WHERE task_id='etl_multi_insert'""")
    assert row["parse_reason"] == "multi_insert_unsupported"
    # 影子表切换完成且保留上一代
    assert _one(conn, "SHOW TABLES LIKE 'lineage_edge_prev'")
    assert _one(conn, "SHOW TABLES LIKE 'table_closure_prev'")
    # 运行记录落库
    run = _one(conn, """SELECT status FROM pipeline_run
                        WHERE run_type='full' ORDER BY run_id DESC LIMIT 1""")
    assert run["status"] == "success"


def test_20_closure_facts(env):
    conn = env["conn"]
    # ODS 申请表 → ADS 报表:3 跳(ods→dwd_apply→dws_org_day→ads_report)
    row = _one(conn, """SELECT min_hops FROM table_closure
                        WHERE ancestor='ods.ods_t_apply' AND descendant='ads.ads_credit_report'""")
    assert row and row["min_hops"] == 3
    # 还款表到明细:直连 1 跳(tmp 链路为 2 跳,min 取 1)
    row = _one(conn, """SELECT min_hops FROM table_closure
                        WHERE ancestor='ods.ods_t_repay' AND descendant='dwd.dwd_repay_detail'""")
    assert row and row["min_hops"] == 1


def test_30_multi_writer_coexist(env):
    # 5.3:两个任务写 dwd_repay_detail,边按 sql_id 独立共存
    conn = env["conn"]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT e.sql_id FROM lineage_edge e
            JOIN lineage_node n ON n.node_id = e.dst_node_id
            WHERE n.full_name='dwd.dwd_repay_detail' AND n.column_name=''
              AND e.edge_level='table'""")
        assert len(cur.fetchall()) == 2


def test_40_self_loop(env):
    row = _one(env["conn"], """
        SELECT e.is_self_loop FROM lineage_edge e
        JOIN lineage_node s ON s.node_id=e.src_node_id
        JOIN lineage_node d ON d.node_id=e.dst_node_id
        WHERE s.full_name='dws.dws_cust_credit_summary'
          AND d.full_name='dws.dws_cust_credit_summary'
          AND e.edge_level='table'""")
    assert row and row["is_self_loop"] == 1


def test_50_mcp_queries(env):
    from server import repo
    conn = repo.connect()
    # S1 溯源:ADS 报表上游 3 层
    lin = repo.get_lineage(conn, "ads.ads_credit_report", direction="upstream", depth=5)
    names_by_level = {}
    for h in lin["hops"]:
        names_by_level.setdefault(h["level"], set()).add(h["full_name"])
    assert "dws.dws_org_credit_day" in names_by_level[1]
    assert "dwd.dwd_apply_detail" in names_by_level[2]
    assert "ods.ods_t_apply" in names_by_level[3]
    # S2 影响分析:申请表全下游 8 张
    imp = repo.impact_analysis(conn, "ods.ods_t_apply")
    assert imp["total_downstream"] == 8
    # 字段级下钻:total_credit_amt 上游含 apply_amt,表达式含 SUM
    col = repo.get_lineage(conn, "dws.dws_cust_credit_summary",
                           column="total_credit_amt",
                           direction="upstream", depth=1, edge_level="column")
    srcs = {(h["full_name"], h["column_name"]) for h in col["hops"]}
    assert ("dwd.dwd_apply_detail", "apply_amt") in srcs
    assert any("SUM" in (h["transform_expr"] or "").upper() for h in col["hops"])
    # 检索:精确表名命中单族
    st = repo.search_term(conn, "dws_cust_credit_summary")
    assert st["families"] and not st["ambiguous"]


def test_60_incremental_update(env):
    from pipeline import rebuild, sql_collector
    conn = env["conn"]
    from tests.warehouse_fixture import CORPUS
    t3 = next(c for c in CORPUS if c["task_id"] == "etl_dwd_repay")
    changed_sql = t3["sql"].replace("WHERE dt = '2026-07-25'",
                                    "WHERE dt = '2026-07-25' AND repay_amt > 0")
    sql_id, changed = sql_collector.upsert_sql(conn, {
        "task_id": "etl_dwd_repay", "task_name": "etl_dwd_repay", "node_seq": 1,
        "target_table": "dwd.dwd_repay_detail", "sql_text": changed_sql,
        "dialect": "hive"})
    assert changed
    out = rebuild.incremental(sql_id)
    assert out["parse"] == "success" and out["closure"] == "patched"
    # 新过滤条件已落到边上
    row = _one(conn, """SELECT e.filter_cond FROM lineage_edge e
                        WHERE e.sql_id=%s AND e.edge_level='table' LIMIT 1""", sql_id)
    assert "repay_amt > 0" in row["filter_cond"]
    # 闭包仍完整
    row = _one(conn, """SELECT min_hops FROM table_closure
                        WHERE ancestor='ods.ods_t_repay' AND descendant='dwd.dwd_repay_detail'""")
    assert row and row["min_hops"] == 1
