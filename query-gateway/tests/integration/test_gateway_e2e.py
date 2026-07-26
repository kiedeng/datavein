"""网关 E2E(真实 MySQL):语义层通道执行+掩码+审计;兜底通道 mock LLM 全链路。

运行:DV_IT_MYSQL=1 python -m pytest tests/integration -q
"""

import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("DV_IT_MYSQL") != "1",
                                reason="需要 MySQL:DV_IT_MYSQL=1 开启")

TEST_DB = "datavein_gw_test"
DB_CONF = dict(
    host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "datavein"),
    password=os.environ.get("MYSQL_PASSWORD", "test_pw"),
    database=TEST_DB, charset="utf8mb4",
)
CTX = {"user_id": "analyst01", "user_orgs": ["O01"], "channel": "test",
       "sensitive_whitelist": ["cust_no"]}


@pytest.fixture(scope="module")
def client():
    import pymysql
    admin = pymysql.connect(**{**DB_CONF, "database": None}, autocommit=True)
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB} CHARACTER SET utf8mb4")
        cur.execute(f"USE {TEST_DB}")
        # 数仓侧:语义层用到的两张表 + 数据(MySQL 充当数仓 stand-in)
        cur.execute("""CREATE TABLE dws_cust_credit_summary
                       (cust_no VARCHAR(32), org_no VARCHAR(16),
                        total_credit_amt DECIMAL(18,2), total_repay_amt DECIMAL(18,2),
                        contract_cnt BIGINT, dt VARCHAR(10))""")
        cur.execute("""CREATE TABLE dim_org
                       (org_no VARCHAR(16), org_name VARCHAR(64),
                        parent_org_no VARCHAR(16))""")
        cur.executemany(
            "INSERT INTO dws_cust_credit_summary VALUES (%s,%s,%s,%s,%s,%s)",
            [("C001", "O01", 100000, 20000, 2, "2026-07-25"),
             ("C002", "O01", 50000, 10000, 1, "2026-07-25"),
             ("C003", "O02", 88888, 0, 1, "2026-07-25")])   # O02:行权限外
        cur.executemany("INSERT INTO dim_org VALUES (%s,%s,%s)",
                        [("O01", "城东支行", ""), ("O02", "城西支行", "")])
        # 平台侧:审计表 + 兜底通道的字段注释
        for ddl_file in ("05_audit.sql", "01_metadata.sql"):
            path = os.path.join(os.path.dirname(__file__), "../../..", "sql", ddl_file)
            for stmt in open(path, encoding="utf-8").read().split(";"):
                if stmt.strip():
                    cur.execute(stmt)
        cur.executemany(
            """INSERT INTO column_metadata (full_name, column_name, data_type, comment)
               VALUES (%s,%s,%s,%s)""",
            [(f"{TEST_DB}.dws_cust_credit_summary", c, t, c)
             for c, t in [("cust_no", "varchar"), ("org_no", "varchar"),
                          ("total_credit_amt", "decimal"), ("dt", "varchar")]])
    admin.close()

    os.environ["MYSQL_DB"] = TEST_DB
    from gateway import audit, config, executor
    config.PLATFORM_MYSQL.update(DB_CONF)
    config.WAREHOUSE_MYSQL.update(DB_CONF)
    audit._pool = None
    executor._pool = None

    # 测试库无 schema 前缀:指标 YAML 用测试专用目录
    import pathlib
    import textwrap
    mdir = pathlib.Path(__file__).parent / "metrics_test"
    mdir.mkdir(exist_ok=True)
    (mdir / "total_credit_amt.yaml").write_text(textwrap.dedent(f"""\
        metric: total_credit_amt
        cn_name: 授信总额
        caliber: 测试口径
        version: 1
        base_table: {TEST_DB}.dws_cust_credit_summary
        expression: SUM(base.total_credit_amt)
        dimensions:
          org_no: {{column: base.org_no}}
          org_name:
            column: o.org_name
            join: {{alias: o, table: {TEST_DB}.dim_org, "on": o.org_no = base.org_no}}
          cust_no: {{column: base.cust_no, sensitivity: L3}}
          dt: {{column: base.dt}}
        default_filters: []
        row_policy: base.org_no IN :user_orgs
        """), encoding="utf-8")
    config.METRICS_DIR = str(mdir)

    from fastapi.testclient import TestClient

    from gateway import app as app_module
    app_module.repo = app_module.MetricsRepo(str(mdir))
    return TestClient(app_module.app)


def test_metric_query_data_and_row_policy(client):
    r = client.post("/query/metric", json={
        "plan": {"metric": "total_credit_amt", "dimensions": ["org_name"],
                 "filters": [{"field": "dt", "op": "eq", "value": "2026-07-25"}]},
        "user_ctx": CTX}).json()
    assert r.get("error") is None, r
    assert r["channel_label"] == "语义层认证"
    assert r["metric_version"] == "total_credit_amt@v1"
    rows = r["data"]["rows"]
    # 行权限:只见 O01(150000),O02 的 88888 被过滤
    assert len(rows) == 1 and float(rows[0]["total_credit_amt"]) == 150000.0
    assert "IN ('O01')" in r["sql"]


def test_metric_query_l3_masked(client):
    r = client.post("/query/metric", json={
        "plan": {"metric": "total_credit_amt", "dimensions": ["cust_no"]},
        "user_ctx": CTX}).json()
    assert r.get("error") is None, r
    assert all(row["cust_no"].endswith("***") for row in r["data"]["rows"])


def test_metric_query_permission_refused_and_audited(client):
    r = client.post("/query/metric", json={
        "plan": {"metric": "total_credit_amt", "dimensions": ["cust_no"]},
        "user_ctx": {"user_id": "outsider", "user_orgs": ["O01"]}}).json()
    assert r["error"] == "permission_denied"
    import pymysql
    conn = pymysql.connect(**DB_CONF, autocommit=True,
                           cursorclass=pymysql.cursors.DictCursor)
    with conn.cursor() as cur:
        cur.execute("""SELECT status, refuse_reason FROM query_audit
                       WHERE user_id='outsider' ORDER BY audit_id DESC LIMIT 1""")
        row = cur.fetchone()
    assert row["status"] == "refused"        # 拒绝也留痕(11 章)


def test_adhoc_with_mock_llm(client):
    from gateway import app as app_module

    class MockLLM:
        def generate_sql(self, question, context):
            return (f"SELECT org_no, SUM(total_credit_amt) AS amt"
                    f" FROM {TEST_DB}.dws_cust_credit_summary GROUP BY org_no")

    app_module._llm = MockLLM()
    r = client.post("/query/adhoc", json={
        "question": "各机构授信", "tables": [f"{TEST_DB}.dws_cust_credit_summary"],
        "user_ctx": CTX}).json()
    assert r.get("error") is None, r
    assert r["channel_label"] == "自由查询"          # 强制标注(7.3)
    assert "LIMIT" in r["sql"]                       # 无 LIMIT 自动注入


def test_adhoc_rejects_out_of_scope_table(client):
    from gateway import app as app_module

    class EvilLLM:
        def generate_sql(self, question, context):
            return f"SELECT * FROM {TEST_DB}.dim_org LIMIT 10"   # 圈定范围外

    app_module._llm = EvilLLM()
    r = client.post("/query/adhoc", json={
        "question": "x", "tables": [f"{TEST_DB}.dws_cust_credit_summary"],
        "user_ctx": CTX}).json()
    assert r["error"] == "rejected" and "范围外" in r["hint"]
