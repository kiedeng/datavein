import os


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


# 平台库(审计/元数据);凭据与数仓分离
PLATFORM_MYSQL = dict(
    host=_env("MYSQL_HOST", "127.0.0.1"),
    port=int(_env("MYSQL_PORT", "3306")),
    database=_env("MYSQL_DB", "datavein"),
    user=_env("MYSQL_USER", "datavein"),
    password=_env("MYSQL_PASSWORD", ""),
    charset="utf8mb4",
)

# 数仓只读账号:红线——全系统仅本服务持有(11 章);默认回落平台库便于开发联调
WAREHOUSE_MYSQL = dict(
    host=_env("WAREHOUSE_HOST", _env("MYSQL_HOST", "127.0.0.1")),
    port=int(_env("WAREHOUSE_PORT", _env("MYSQL_PORT", "3306"))),
    database=_env("WAREHOUSE_DB", _env("MYSQL_DB", "datavein")),
    user=_env("WAREHOUSE_USER", _env("MYSQL_USER", "datavein")),
    password=_env("WAREHOUSE_PASSWORD", _env("MYSQL_PASSWORD", "")),
    charset="utf8mb4",
)
WAREHOUSE_DIALECT = _env("WAREHOUSE_DIALECT", "mysql")   # 行内为 hive/spark 时经对应网关

METRICS_DIR = _env("METRICS_DIR",
                   os.path.join(os.path.dirname(__file__), "..", "metrics"))

GATEWAY_HOST = _env("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(_env("GATEWAY_PORT", "8090"))
GATEWAY_TOKEN = _env("GATEWAY_TOKEN", "")        # 服务间共享密钥,空=不校验(仅开发)

HARD_LIMIT = int(_env("QUERY_HARD_LIMIT", "1000"))       # 强制 LIMIT(11 章)
QUERY_TIMEOUT_MS = int(_env("QUERY_TIMEOUT_MS", "30000"))
MAX_CONCURRENCY = int(_env("QUERY_MAX_CONCURRENCY", "10"))

# 敏感表黑名单:网关硬编码 + 环境变量追加(11 章红线)
BLACKLIST_TABLES = frozenset(
    t.strip() for t in
    ("ods.ods_t_cust_secret," + _env("BLACKLIST_TABLES", "")).split(",") if t.strip())
