"""检索消歧评估集(设计方案 13 章):50 组问法,Top1 命中率 ≥80%,families 归并零错并。

数据:warehouse_fixture 的表/字段元数据 + 术语表种子(约 15 条,含别名/口语叫法/
certified/跨域同名),问法覆盖 别名命中/口语叫法/跨域同名歧义/拼写近似/注释关键词
(见 disambiguation_cases.json)。闭包由语料库任务依赖直接推导(tmp 按 5.3 折叠视角
穿透),不依赖解析流水线,保证评估只考核检索层(6.1-6.3)。

判定口径(13 章):Top1 命中 = 首个 family 的锚点表落在期望集合内(同链内按 6.2/6.3
锚点规则可落任一);零错并 = 同链候选必须合为一族、异链候选绝不合族
(异链误并同链为严重缺陷,专项用例单独断言)。

运行:DV_IT_MYSQL=1 python -m pytest tests/evals -q(独立库 datavein_eval_test)。
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("DV_IT_MYSQL") != "1",
                                reason="需要 MySQL:DV_IT_MYSQL=1 开启")

REPO_ROOT = Path(__file__).resolve().parents[3]
EVAL_DB = "datavein_eval_test"
DB_CONF = dict(
    host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    user=os.environ.get("MYSQL_USER", "datavein"),
    password=os.environ.get("MYSQL_PASSWORD", "test_pw"),
    charset="utf8mb4",
)

CASES = json.loads(
    (Path(__file__).parent / "disambiguation_cases.json").read_text())["cases"]

# 表注释:注释关键词类问法(L2 ngram 全文)的语料;关键词经设计彼此不串台
TABLE_COMMENTS = {
    "ods.ods_t_apply": "贷款申请流水贴源表,记录进件申请信息",
    "ods.ods_t_contract": "贷款合同贴源表,记录签约合同信息",
    "ods.ods_t_repay": "还款流水贴源表",
    "ods.ods_t_cust": "客户基本信息贴源表,含证件号与手机号",
    "dim.dim_org": "机构维表,含网点层级关系",
    "dwd.dwd_apply_detail": "申请明细宽表,窗口去重后的进件明细",
    "dwd.dwd_apply_snapshot": "申请快照宽表,全列快照",
    "dwd.dwd_contract_detail": "合同明细宽表,含合同生效状态",
    "dwd.dwd_repay_detail": "还款明细宽表,含提前还款标识",
    "dws.dws_cust_credit_summary": "客户授信与还款累计汇总表",
    "dws.dws_org_credit_day": "机构维度日授信统计表,按机构口径计算当日授信金额与申请笔数",
    "ads.ads_credit_report": "机构授信日报,按授信金额排名",
    "ads.ads_cust_risk_tag": "客户风险标签拉链结果",
    "ads.ads_apply_cube": "申请多维立方结果",
    "tmp_credit.tmp_repay_dedup": "还款去重临时表",
    "dep.dwd_deposit_balance": "存款余额明细宽表",
}

# 跨域同名歧义用:存款域表(与信贷链完全异链)
EXTRA_SCHEMA = {
    "dep": {"dwd_deposit_balance": {"cust_no": "string",
                                    "balance_amt": "decimal(18,2)", "dt": "string"}},
}

# 术语表种子:term, aliases, caliber, domain, ref_table, ref_column, certified
GLOSSARY_SEED = [
    ("授信金额", ["批核金额", "授信额度"], "机构日授信金额=当日审批通过的申请金额合计",
     "credit", "dws.dws_org_credit_day", "credit_amt", 1),
    ("还款金额", ["还款额"], "实收还款金额,含提前还款",
     "credit", "dwd.dwd_repay_detail", "repay_amt", 1),
    ("提前还款标志", ["提前还款", "是否提前还款"], "还款类型为 PRE 记 1",
     "credit", "dwd.dwd_repay_detail", "is_prepay", 0),
    ("客户号", ["客户编号", "客户ID"], "客户在核心系统的唯一编号",
     "credit", "ods.ods_t_cust", "cust_no", 1),
    ("机构名称", ["网点名称", "分支机构"], "机构全称,取机构维表",
     "credit", "dim.dim_org", "org_name", 1),
    ("授信排名", ["机构授信排名"], "按机构日授信金额降序排名",
     "credit", "ads.ads_credit_report", "credit_rank", 1),
    ("风险标签", ["风控标签"], "客户风险标签,逗号分隔展开",
     "risk", "ads.ads_cust_risk_tag", "risk_tag", 0),
    ("申请金额", ["进件金额", "申请额度"], "客户提交的申请金额",
     "credit", "ods.ods_t_apply", "apply_amt", 1),
    ("合同金额", ["签约金额"], "合同签约金额",
     "credit", "ods.ods_t_contract", "contract_amt", 0),
    ("客户授信汇总", ["客户额度汇总"], "客户维度授信/还款累计汇总",
     "credit", "dws.dws_cust_credit_summary", "total_credit_amt", 1),
    ("余额", [], "授信余额=累计授信-累计还款",
     "credit", "dws.dws_cust_credit_summary", "total_credit_amt", 1),
    ("余额", [], "存款账户日终余额",
     "deposit", "dep.dwd_deposit_balance", "balance_amt", 1),
    ("申请状态", ["进件状态"], "申请审批状态码",
     "credit", "ods.ods_t_apply", "apply_status", 0),
    ("授信报表", ["机构授信报表"], "机构授信日报输出",
     "credit", "ads.ads_credit_report", None, 1),
    ("累计还款", ["还款总额"], "客户累计还款金额",
     "credit", "dws.dws_cust_credit_summary", "total_repay_amt", 1),
]


def _closure_rows():
    """语料库任务依赖 → 表级邻接 → 闭包;tmp 表按 5.3 折叠视角穿透后剔除。"""
    from pipeline.closure import compute_closure
    from tests.warehouse_fixture import CORPUS

    adj: dict[str, set] = defaultdict(set)
    for c in CORPUS:
        if c.get("status") != "success" or not c["target"]:
            continue
        for s in c["sources"]:
            adj[s].add(c["target"])
    nodes = set(adj) | {d for ds in adj.values() for d in ds}
    for t in [n for n in nodes if n.split(".", 1)[0].startswith("tmp")]:
        succs = adj.pop(t, set())
        for preds in adj.values():
            if t in preds:
                preds.discard(t)
                preds.update(succs)
    return compute_closure(dict(adj))


@pytest.fixture(scope="module")
def conn():
    """独立库 datavein_eval_test:DDL + 元数据/闭包/术语种子。"""
    import pymysql

    from pipeline import config as pconfig
    from tests.warehouse_fixture import SCHEMA

    admin = pymysql.connect(**DB_CONF, autocommit=True)
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {EVAL_DB}")
        cur.execute(f"CREATE DATABASE {EVAL_DB} CHARACTER SET utf8mb4")
    admin.close()

    c = pymysql.connect(**DB_CONF, database=EVAL_DB, autocommit=True,
                        cursorclass=pymysql.cursors.DictCursor)
    with c.cursor() as cur:
        for ddl_file in sorted((REPO_ROOT / "sql").glob("0*.sql")):
            for stmt in ddl_file.read_text().split(";"):
                if stmt.strip():
                    cur.execute(stmt)
        for database, tables in {**SCHEMA, **EXTRA_SCHEMA}.items():
            domain = "deposit" if database == "dep" else "credit"
            for table, cols in tables.items():
                full = f"{database}.{table}"
                layer = pconfig.derive_layer(table) or (
                    "tmp" if database.startswith("tmp") else "")
                cur.execute(
                    """REPLACE INTO table_metadata
                       (full_name, db_name, table_name, table_type, layer, domain,
                        comment, owner, is_online, synced_at)
                       VALUES (%s,%s,%s,'table',%s,%s,%s,'tester',1,NOW())""",
                    (full, database, table, layer, domain,
                     TABLE_COMMENTS.get(full, f"{table} 数据表")))
                for col, typ in cols.items():
                    cur.execute(
                        """REPLACE INTO column_metadata
                           (full_name, column_name, data_type, comment)
                           VALUES (%s,%s,%s,%s)""",
                        (full, col, typ, f"{col} 字段说明"))
        cur.executemany("INSERT INTO table_closure VALUES (%s,%s,%s,%s)",
                        _closure_rows())
        for term, aliases, caliber, domain, ref_table, ref_column, certified in GLOSSARY_SEED:
            cur.execute(
                """INSERT INTO biz_glossary
                   (term, aliases, caliber, domain, owner, ref_table, ref_column,
                    certified, status, updated_by, updated_at)
                   VALUES (%s,%s,%s,%s,'tester',%s,%s,%s,'active','tester',NOW())""",
                (term, json.dumps(aliases, ensure_ascii=False), caliber, domain,
                 ref_table, ref_column, certified))

    sys.path.insert(0, str(REPO_ROOT / "lineage-mcp-server"))
    yield c
    c.close()


def _anchor_table(result: dict) -> str:
    if not result["families"]:
        return ""
    anchor = result["families"][0]["anchor"]
    return anchor.get("ref_table") or anchor.get("full_name") or ""


def test_top1_hit_rate_ge_80(conn):
    """13 章:50 组问法 Top1 命中率 ≥80%。"""
    from server import repo

    assert len(CASES) == 50
    misses = []
    for case in CASES:
        result = repo.search_term(conn, case["q"], case.get("domain"))
        top1 = _anchor_table(result)
        if top1 not in case["expect"]:
            misses.append((case["kind"], case["q"], top1 or "<empty>"))
    rate = (len(CASES) - len(misses)) / len(CASES)
    assert rate >= 0.8, f"Top1 命中率 {rate:.0%} < 80%,未命中: {misses}"


def test_same_chain_must_merge(conn):
    """零错并之一:同链候选(互为上下游)必须合为一族(6.2 流转不是歧义)。"""
    from server import repo

    # repay_amt: ods_t_repay→dwd_repay_detail 直链;credit_amt: dws→ads 直链;
    # apply_amt: dwd_apply_detail 与 dwd_apply_snapshot 无直接连通,
    # 但同以 ods_t_apply 为上游,经共同候选桥接归并为一族
    for keyword in ("repay_amt", "credit_amt", "apply_amt"):
        result = repo.search_term(conn, keyword)
        assert len(result["families"]) == 1 and not result["ambiguous"], \
            f"{keyword} 同链候选被错误分族: {result['families']}"


def test_cross_chain_must_split(conn):
    """零错并之二:异链候选(不同口径)绝不合族,ambiguous 必须置位(6.2/6.3)。"""
    from server import repo

    # 跨域同名术语:信贷"余额" vs 存款"余额"
    result = repo.search_term(conn, "余额")
    assert result["ambiguous"] and len(result["families"]) == 2
    anchors = {f["anchor"]["ref_table"] for f in result["families"]}
    assert anchors == {"dws.dws_cust_credit_summary", "dep.dwd_deposit_balance"}

    # 同名字段跨链:信贷链 cust_no 一族 + 存款表 cust_no 一族,不多不少
    result = repo.search_term(conn, "cust_no")
    assert result["ambiguous"] and len(result["families"]) == 2
    tables = {c.get("full_name")
              for f in result["families"]
              for c in [f["anchor"], *f["members"]]}
    assert "dep.dwd_deposit_balance" in tables

    # 上下文域参与排序(6.3):同术语在不同语境下 Top1 各归其域
    assert _anchor_table(repo.search_term(conn, "余额", "credit")) \
        == "dws.dws_cust_credit_summary"
    assert _anchor_table(repo.search_term(conn, "余额", "deposit")) \
        == "dep.dwd_deposit_balance"
