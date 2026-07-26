"""合成数仓生成器:为全量重建压测生成可复现的表结构与 SQL 语料。

用途(README 详述):在拿到行内真实 SQL 之前,按 lineage-pipeline/tests/
warehouse_fixture.py 的写法风格合成一个分层数仓(ods/dwd/dws/ads/dim)与
M 段 ETL SQL,灌入目标库的 table_metadata / column_metadata / schema_snapshot /
sql_repository,供 run_benchmark.py 驱动 pipeline.rebuild.full_rebuild() 压测。

可复现性:同 --seed 下 generate() 输出完全一致(纯函数,不含时间戳/机器信息)。

SQL 形态混合(近似真实 ETL 分布,按任务书比例):
    简单清洗 40% / 多表 JOIN 25% / CTE+窗口 15% / 聚合 10% /
    自依赖 5% / tmp 链 3%(成对:ods→tmp、tmp→dwd 两段) / LATERAL VIEW 2%

命令行:
    python generator.py --tables 500 --sqls 500 --seed 42 --database datavein_bench
连接取 MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD 环境变量,
默认 127.0.0.1 datavein/test_pw(与 tests/integration/test_e2e_mysql.py 一致)。
"""

import argparse
import json
import os
import random

DT = "2026-07-25"
PREV_DT = "2026-07-24"

# 分层表数量比例(真实数仓 ods 最大、往上逐层收敛;dim 独立维表)
LAYER_RATIOS = (("ods", 0.32), ("dwd", 0.28), ("dws", 0.18),
                ("ads", 0.12), ("dim", 0.10))

# SQL 形态比例(任务书口径;tmp_chain 每条链 2 段,计数按段)
SQL_MIX = (("simple", 0.40), ("join", 0.25), ("cte_window", 0.15),
           ("agg", 0.10), ("self_dep", 0.05), ("tmp_chain", 0.03),
           ("lateral", 0.02))

WORDS = ["apply", "contract", "repay", "cust", "loan", "card", "acct", "txn",
         "risk", "coll", "org", "prod", "quota", "overdue", "guar", "rate",
         "limit", "credit", "settle", "channel", "branch", "fund", "bill",
         "asset", "pledge"]

_EXTRA_POOL = (("amt", "decimal(18,2)"), ("cnt", "bigint"),
               ("rate", "decimal(9,4)"), ("flag", "tinyint"),
               ("attr", "string"), ("code", "string"),
               ("remark", "string"), ("days", "int"))


def _alloc(total: int, ratios) -> dict:
    """按比例分配 total,最大余数法保证总和精确、每档至少 1(total 足够时)。"""
    raw = [(name, total * r) for name, r in ratios]
    counts = {name: int(v) for name, v in raw}
    rest = total - sum(counts.values())
    for name, v in sorted(raw, key=lambda x: x[1] - int(x[1]), reverse=True):
        if rest <= 0:
            break
        counts[name] += 1
        rest -= 1
    if total >= len(ratios):
        for name, _ in ratios:      # 每档保底 1,从最大档扣
            if counts[name] == 0:
                counts[name] += 1
                counts[max(counts, key=counts.get)] -= 1
    return counts


def _table_cols(rng: random.Random, layer: str, word: str) -> dict:
    """单表列结构(10-40 列,列序即物理序;分区列 dt 恒排最后,同 fixture)。"""
    ncols = rng.randint(10, 40)
    if layer == "dim":
        cols = {"org_no": "string", "org_name": "string",
                "parent_org_no": "string"}
        extra = ncols - len(cols)
        has_dt = False
    else:
        cols = {f"{word}_id": "bigint", "cust_no": "string", "org_no": "string",
                "prod_cd": "string", "status_cd": "string", "biz_date": "string"}
        if layer == "ods":
            cols["tags"] = "string"          # LATERAL VIEW explode 素材
        extra = ncols - len(cols) - 1        # 留 1 给 dt
        has_dt = True
    for k in range(max(extra, 2)):
        base, typ = _EXTRA_POOL[k % len(_EXTRA_POOL)]
        cols[f"{base}_{k // len(_EXTRA_POOL) + 1}"] = typ
    if has_dt:
        cols["dt"] = "string"
    return cols


def _data_cols(cols: dict) -> list:
    return [c for c in cols if c != "dt"]


def _numeric_cols(cols: dict) -> list:
    picked = [c for c in cols
              if c.split("_")[0] in ("amt", "cnt", "rate", "days") and c != "dt"]
    return picked or _data_cols(cols)


def _pick(rng: random.Random, pool: list):
    """幂次偏斜采样:低下标为热点表(制造 ODS 核心表大下游的真实形态)。"""
    return pool[int(len(pool) * rng.random() ** 2.2)]


def _gen_tables(rng: random.Random, n: int):
    counts = _alloc(n, LAYER_RATIOS)
    schema: dict = {}
    tables_meta: list = []
    pools: dict = {}
    for layer, cnt in counts.items():
        pools[layer] = []
        for i in range(cnt):
            word = rng.choice(WORDS)
            table = f"{layer}_{word}_{i:05d}"      # 前缀匹配 config.derive_layer
            full = f"{layer}.{table}"
            cols = _table_cols(rng, layer, word)
            schema.setdefault(layer, {})[table] = cols
            tables_meta.append({"full_name": full, "db": layer, "table": table,
                                "layer": layer, "domain": word,
                                "comment": f"{layer} {word} 合成表 {i}"})
            pools[layer].append(full)
    return counts, schema, tables_meta, pools


# ---------------- SQL 形态模板 ----------------

def _expr(rng: random.Random, alias: str, cols: list, j: int) -> str:
    a = f"{alias}." if alias else ""
    col = rng.choice(cols)
    variants = [
        lambda: f"{a}{col}",
        lambda: f"{a}{col}",
        lambda: f"{a}{col}",
        lambda: f"CASE WHEN {a}status_cd = 'A' THEN 1 ELSE 0 END",
        lambda: f"to_date({a}biz_date)",
        lambda: f"coalesce({a}{col}, 0)",
        lambda: f"substr({a}cust_no, 1, 4)",
        lambda: f"upper({a}prod_cd)",
    ]
    if not {"status_cd", "biz_date", "cust_no", "prod_cd"} <= set(cols):
        # 来源缺业务基列(dim/tmp/CTE 投影)时退化为直取,保证引用列必存在
        variants = variants[:3] + [lambda: f"coalesce({a}{col}, 0)"]
    return f"{variants[rng.randrange(len(variants))]()} AS c{j}"


def _k_out(rng: random.Random, target_cols: dict, lo=6, hi=12) -> int:
    """SELECT 输出列数:不超过目标表数据列数(Hive 位置前缀映射语义)。"""
    limit = len(_data_cols(target_cols))
    return max(2, min(rng.randint(lo, hi), limit))


def _sql_simple(rng, src, src_cols, tgt, tgt_cols):
    k = _k_out(rng, tgt_cols)
    exprs = ",\n       ".join(_expr(rng, "", _data_cols(src_cols), j)
                              for j in range(k))
    return (f"INSERT OVERWRITE TABLE {tgt} PARTITION (dt = '{DT}')\n"
            f"SELECT {exprs}\nFROM {src}\nWHERE dt = '{DT}'\n")


def _sql_join(rng, srcs, schema_of, tgt, tgt_cols, dialect):
    k = _k_out(rng, tgt_cols)
    aliases = ["a", "b", "c"][:len(srcs)]
    parts = []
    for j in range(k):
        i = rng.randrange(len(srcs))
        parts.append(_expr(rng, aliases[i], _data_cols(schema_of(srcs[i])), j))
    hint = "/*+ BROADCAST(b) */ " if dialect == "spark" else ""
    lines = [f"INSERT OVERWRITE TABLE {tgt} PARTITION (dt = '{DT}')",
             f"SELECT {hint}" + ",\n       ".join(parts),
             f"FROM {srcs[0]} a"]
    for i, s in enumerate(srcs[1:], start=1):
        al = aliases[i]
        cols = schema_of(s)
        if "dt" in cols:
            lines.append(f"JOIN {s} {al} ON {al}.cust_no = a.cust_no"
                         f" AND {al}.dt = '{DT}'")
        else:                                   # 维表无分区
            lines.append(f"LEFT JOIN {s} {al} ON {al}.org_no = a.org_no")
    lines.append(f"WHERE a.dt = '{DT}'")
    return "\n".join(lines) + "\n"


def _sql_cte_window(rng, src, src_cols, tgt, tgt_cols):
    k = _k_out(rng, tgt_cols)
    data = _data_cols(src_cols)
    inner = ", ".join(dict.fromkeys([rng.choice(data) for _ in range(k + 2)]
                                    + ["cust_no", "biz_date"]))
    exprs = ",\n       ".join(_expr(rng, "", inner.split(", "), j)
                              for j in range(k))
    win = ("row_number()" if rng.random() < 0.7 else "rank()")
    return (f"WITH base AS (\n"
            f"  SELECT {inner},\n"
            f"         {win} OVER (PARTITION BY cust_no ORDER BY biz_date DESC) AS rn\n"
            f"  FROM {src}\n  WHERE dt = '{DT}'\n)\n"
            f"INSERT OVERWRITE TABLE {tgt} PARTITION (dt = '{DT}')\n"
            f"SELECT {exprs}\nFROM base\nWHERE rn = 1\n")


def _sql_agg(rng, srcs, schema_of, tgt, tgt_cols, dialect):
    k = max(2, _k_out(rng, tgt_cols) - 2)
    a_cols = _numeric_cols(schema_of(srcs[0]))
    aggs = []
    for j in range(k):
        fn = rng.choice(["SUM", "SUM", "MAX", "COUNT"])
        col = rng.choice(a_cols)
        aggs.append(f"COUNT(1) AS m{j}" if fn == "COUNT"
                    else f"{fn}(a.{col}) AS m{j}")
    lines = [f"INSERT OVERWRITE TABLE {tgt} PARTITION (dt = '{DT}')",
             "SELECT a.cust_no, a.org_no,",
             "       " + ",\n       ".join(aggs),
             f"FROM {srcs[0]} a"]
    if len(srcs) > 1:
        lines.append(f"JOIN {srcs[1]} b ON b.cust_no = a.cust_no"
                     f" AND b.dt = '{DT}'")
    lines.append(f"WHERE a.dt = '{DT}' AND a.status_cd = 'A'")
    lines.append("GROUP BY a.cust_no, a.org_no")
    if dialect == "spark":
        lines.append("DISTRIBUTE BY org_no")
    return "\n".join(lines) + "\n"


def _sql_self_dep(rng, tgt, tgt_cols, src, src_cols):
    t_num = _numeric_cols(tgt_cols)
    v1, v2 = rng.choice(t_num), rng.choice(t_num)
    s_amt = rng.choice(_numeric_cols(src_cols))
    return (f"INSERT OVERWRITE TABLE {tgt} PARTITION (dt = '{DT}')\n"
            f"SELECT cust_no, org_no,\n"
            f"       SUM(v1) AS total_1,\n       SUM(v2) AS total_2\n"
            f"FROM (\n"
            f"  SELECT cust_no, org_no, {v1} AS v1, {v2} AS v2\n"
            f"  FROM {tgt}\n  WHERE dt = '{PREV_DT}'\n"
            f"  UNION ALL\n"
            f"  SELECT cust_no, org_no, {s_amt}, 0\n"
            f"  FROM {src}\n  WHERE dt = '{DT}'\n"
            f") m\nGROUP BY cust_no, org_no\n")


def _sql_tmp_pair(rng, src, src_cols, tmp_full, tgt, tgt_cols):
    """tmp 链两段:① ods→tmp 窗口去重;② tmp→dwd 清洗。返回 (tmp列, seg1, seg2)。"""
    data = _data_cols(src_cols)
    k = min(rng.randint(5, 8), len(data), len(_data_cols(tgt_cols)) - 1)
    picked = list(dict.fromkeys(["cust_no", "status_cd", "biz_date"]
                                + [rng.choice(data) for _ in range(k)]))[:max(k, 4)]
    cols_sql = ", ".join(picked)
    seg1 = (f"INSERT OVERWRITE TABLE {tmp_full}\n"
            f"SELECT {cols_sql}\nFROM (\n"
            f"  SELECT {cols_sql},\n"
            f"         row_number() OVER (PARTITION BY {picked[0]}"
            f" ORDER BY biz_date DESC) AS rn\n"
            f"  FROM {src} WHERE dt = '{DT}'\n"
            f") t WHERE rn = 1\n")
    out = [c for c in picked if c != "status_cd"]
    seg2 = (f"INSERT OVERWRITE TABLE {tgt} PARTITION (dt = '{DT}')\n"
            f"SELECT {', '.join(out)},\n"
            f"       CASE WHEN status_cd = 'A' THEN 1 ELSE 0 END AS f_flag\n"
            f"FROM {tmp_full}\n")
    tmp_cols = {c: src_cols[c] for c in picked}
    return tmp_cols, seg1, seg2


def _sql_lateral(rng, src, tgt):
    return (f"INSERT OVERWRITE TABLE {tgt} PARTITION (dt = '{DT}')\n"
            f"SELECT cust_no, tag AS risk_tag\n"
            f"FROM {src}\n"
            f"LATERAL VIEW explode(split(tags, ',')) x AS tag\n"
            f"WHERE dt = '{DT}'\n")


# ---------------- 主生成流程 ----------------

def generate(tables: int, sqls: int, seed: int) -> dict:
    """纯函数:同参数同 seed 输出逐字节一致。

    返回 {schema, tables, corpus, layer_counts, shape_counts};
    tmp 表在 N 之外少量追加(每条 tmp 链 1 张,库名 tmp_bench,layer=tmp)。
    """
    rng = random.Random(seed)
    layer_counts, schema, tables_meta, pools = _gen_tables(rng, tables)

    shape_counts = _alloc(sqls, SQL_MIX)
    if shape_counts.get("tmp_chain", 0) % 2 == 1:     # tmp 链按段成对
        shape_counts["tmp_chain"] -= 1
        shape_counts["simple"] = shape_counts.get("simple", 0) + 1

    def cols_of(full: str) -> dict:
        db, t = full.split(".", 1)
        return schema[db][t]

    corpus: list = []
    seq = 0
    plan: list = []
    for shape, cnt in shape_counts.items():
        n = cnt // 2 if shape == "tmp_chain" else cnt
        plan.extend([shape] * n)
    rng.shuffle(plan)

    for shape in plan:
        seq += 1
        task_id = f"etl_{shape}_{seq:05d}"
        owner = f"user{rng.randint(1, 40):02d}"
        if shape == "simple":
            src = _pick(rng, pools["ods"])
            tgt = rng.choice(pools["dwd"])
            sql = _sql_simple(rng, src, cols_of(src), tgt, cols_of(tgt))
            items = [(1, tgt, "hive", sql)]
        elif shape == "join":
            dialect = "spark" if rng.random() < 0.4 else "hive"
            if rng.random() < 0.5:
                tgt = rng.choice(pools["dwd"])
                srcs = [_pick(rng, pools["ods"]), _pick(rng, pools["ods"])]
            else:
                tgt = rng.choice(pools["dws"])
                srcs = [_pick(rng, pools["dwd"]), _pick(rng, pools["dwd"])]
            if rng.random() < 0.5:
                srcs.append(_pick(rng, pools["dim"]))
            srcs = list(dict.fromkeys(srcs))
            sql = _sql_join(rng, srcs, cols_of, tgt, cols_of(tgt), dialect)
            items = [(1, tgt, dialect, sql)]
        elif shape == "cte_window":
            if rng.random() < 0.5:
                src, tgt = _pick(rng, pools["ods"]), rng.choice(pools["dwd"])
            else:
                src, tgt = _pick(rng, pools["dwd"]), rng.choice(pools["dws"])
            sql = _sql_cte_window(rng, src, cols_of(src), tgt, cols_of(tgt))
            items = [(1, tgt, "hive", sql)]
        elif shape == "agg":
            dialect = "spark" if rng.random() < 0.5 else "hive"
            if rng.random() < 0.6:
                tgt = rng.choice(pools["dws"])
                srcs = [_pick(rng, pools["dwd"])]
                if rng.random() < 0.5:
                    srcs.append(_pick(rng, pools["dwd"]))
            else:
                tgt = rng.choice(pools["ads"])
                srcs = [_pick(rng, pools["dws"])]
            srcs = list(dict.fromkeys(srcs))
            sql = _sql_agg(rng, srcs, cols_of, tgt, cols_of(tgt), dialect)
            items = [(1, tgt, dialect, sql)]
        elif shape == "self_dep":
            tgt = rng.choice(pools["dws"])
            src = _pick(rng, pools["dwd"])
            sql = _sql_self_dep(rng, tgt, cols_of(tgt), src, cols_of(src))
            items = [(1, tgt, "hive", sql)]
        elif shape == "tmp_chain":
            src = _pick(rng, pools["ods"])
            tgt = rng.choice(pools["dwd"])
            tmp_table = f"tmp_{shape}_{seq:05d}"
            tmp_full = f"tmp_bench.{tmp_table}"
            tmp_cols, seg1, seg2 = _sql_tmp_pair(
                rng, src, cols_of(src), tmp_full, tgt, cols_of(tgt))
            schema.setdefault("tmp_bench", {})[tmp_table] = tmp_cols
            tables_meta.append({"full_name": tmp_full, "db": "tmp_bench",
                                "table": tmp_table, "layer": "tmp",
                                "domain": "tmp",
                                "comment": f"tmp 链中转表 {seq}"})
            items = [(1, tmp_full, "hive", seg1), (2, tgt, "hive", seg2)]
        else:  # lateral
            src = _pick(rng, pools["ods"])
            tgt = rng.choice(pools["ads"])
            sql = _sql_lateral(rng, src, tgt)
            items = [(1, tgt, "hive", sql)]

        for node_seq, target, dialect, sql in items:
            corpus.append({"task_id": task_id, "node_seq": node_seq,
                           "target": target, "dialect": dialect,
                           "shape": shape, "owner": owner, "sql": sql})

    return {"seed": seed, "tables_arg": tables, "sqls_arg": sqls,
            "schema": schema, "tables": tables_meta, "corpus": corpus,
            "layer_counts": layer_counts, "shape_counts": shape_counts}


# ---------------- 落库 ----------------

def load(conn, data: dict, batch: int = 2000) -> dict:
    """写入 table_metadata / column_metadata / schema_snapshot / sql_repository。

    conn:pymysql 连接(需 DictCursor 非必须;autocommit 关闭,本函数自行 commit)。
    """
    schema, tables_meta, corpus = data["schema"], data["tables"], data["corpus"]
    tm_rows, cm_rows, ss_rows, sr_rows = [], [], [], []
    for t in tables_meta:
        cols = schema[t["db"]][t["table"]]
        tm_rows.append((t["full_name"], t["db"], t["table"], "table",
                        t["layer"], t["domain"], t["comment"]))
        cm_rows.extend((t["full_name"], c, typ, f"{c} 合成列")
                       for c, typ in cols.items())
        ss_rows.append((t["full_name"],
                        json.dumps([{"name": c, "type": typ}
                                    for c, typ in cols.items()])))
    for c in corpus:
        sr_rows.append((c["task_id"], c["task_id"], c["node_seq"], c["target"],
                        c["sql"], c["sql"], c["dialect"], c["owner"],
                        c["target"].split(".", 1)[0]))

    with conn.cursor() as cur:
        for i in range(0, len(tm_rows), batch):
            cur.executemany(
                """REPLACE INTO table_metadata
                   (full_name, db_name, table_name, table_type, layer, domain,
                    comment, owner, is_online, synced_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,'bench',1,NOW())""",
                tm_rows[i:i + batch])
        for i in range(0, len(cm_rows), batch):
            cur.executemany(
                """REPLACE INTO column_metadata
                   (full_name, column_name, data_type, comment)
                   VALUES (%s,%s,%s,%s)""", cm_rows[i:i + batch])
        for i in range(0, len(ss_rows), batch):
            cur.executemany(
                """REPLACE INTO schema_snapshot (full_name, snap_date, columns_json)
                   VALUES (%s, CURDATE(), %s)""", ss_rows[i:i + batch])
        for i in range(0, len(sr_rows), batch):
            cur.executemany(
                """INSERT INTO sql_repository
                   (task_id, task_name, node_seq, target_table, sql_text, sql_hash,
                    dialect, owner, domain, parse_status, is_active, updated_at)
                   VALUES (%s,%s,%s,%s,%s,SHA2(%s,256),%s,%s,%s,'pending',1,NOW())""",
                sr_rows[i:i + batch])
    conn.commit()
    return {"tables": len(tm_rows), "columns": len(cm_rows),
            "snapshots": len(ss_rows), "sqls": len(sr_rows)}


def _connect(database: str):
    import pymysql
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "datavein"),
        password=os.environ.get("MYSQL_PASSWORD", "test_pw"),
        database=database, charset="utf8mb4")


def main():
    ap = argparse.ArgumentParser(description="合成数仓生成器(写入目标库)")
    ap.add_argument("--tables", type=int, default=500)
    ap.add_argument("--sqls", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--database", default="datavein_bench",
                    help="目标库(需已建库并打过 sql/ DDL)")
    args = ap.parse_args()

    data = generate(args.tables, args.sqls, args.seed)
    conn = _connect(args.database)
    counts = load(conn, data)
    conn.close()
    print(json.dumps({"loaded": counts, "layer_counts": data["layer_counts"],
                      "shape_counts": data["shape_counts"]},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
