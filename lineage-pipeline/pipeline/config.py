import json
import os


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


MYSQL = dict(
    host=_env("MYSQL_HOST", "127.0.0.1"),
    port=int(_env("MYSQL_PORT", "3306")),
    database=_env("MYSQL_DB", "datavein"),
    user=_env("MYSQL_USER", "datavein"),
    password=_env("MYSQL_PASSWORD", ""),
    charset="utf8mb4",
)
DB_POOL_SIZE = int(_env("DB_POOL_SIZE", "10"))

# Hive Metastore 后端库直连(生产采集方式:读 TBLS/DBS/COLUMNS_V2,只读账号)
METASTORE_DB = dict(
    host=_env("METASTORE_DB_HOST", ""),
    port=int(_env("METASTORE_DB_PORT", "3306")),
    database=_env("METASTORE_DB_NAME", "hive"),
    user=_env("METASTORE_DB_USER", ""),
    password=_env("METASTORE_DB_PASSWORD", ""),
    charset="utf8mb4",
)
# 表 owner 取自 TABLE_PARAMS 的哪个 PARAM_KEY(各行 Metastore 习惯不同)
METASTORE_OWNER_PARAM_KEYS = tuple(
    _env("METASTORE_OWNER_PARAM_KEYS", "owner,created_by").split(","))

# 调度平台通用 REST 适配:端点 + 字段映射(JSON,平台字段名 -> 标准字段名)
SCHEDULER_API_BASE = _env("SCHEDULER_API_BASE")
SCHEDULER_API_TOKEN = _env("SCHEDULER_API_TOKEN")
SCHEDULER_SQL_ENDPOINT = _env("SCHEDULER_SQL_ENDPOINT", "/api/v1/task-sqls")
SCHEDULER_PAGE_SIZE = int(_env("SCHEDULER_PAGE_SIZE", "200"))
SCHEDULER_FIELD_MAP = json.loads(_env("SCHEDULER_FIELD_MAP", "{}")) or {
    "task_id": "task_id", "task_name": "task_name", "node_seq": "node_seq",
    "target_table": "target_table", "sql_text": "sql_text",
    "dialect": "dialect", "owner": "owner", "domain": "domain",
}

SQL_DIALECT_DEFAULT = _env("SQL_DIALECT_DEFAULT", "hive")
PARSE_TIMEOUT_SECONDS = int(_env("PARSE_TIMEOUT_SECONDS", "60"))
PARALLEL_WORKERS = int(_env("PARALLEL_WORKERS", "8"))
CROSSCHECK_ENABLED = _env("CROSSCHECK_ENABLED", "1") == "1"   # sqllineage 表级交叉校验(5.2)

# 闭包增量修补退化阈值:|ancestors|×|descendants| 超过则转异步全量(2.2)
CLOSURE_INCR_DEGRADE_THRESHOLD = int(_env("CLOSURE_INCR_DEGRADE_THRESHOLD", "5000000"))

# 影子表切换校验:行数波动超阈值则中止切换,保持旧版血缘服务(4.3)
REBUILD_EDGE_DELTA_MAX = float(_env("REBUILD_EDGE_DELTA_MAX", "0.20"))
REBUILD_CLOSURE_DELTA_MAX = float(_env("REBUILD_CLOSURE_DELTA_MAX", "0.10"))

# healthcheck 阈值(cron 调用,退出码非 0 触发告警)
HEALTH_MAX_FULL_RUN_AGE_HOURS = int(_env("HEALTH_MAX_FULL_RUN_AGE_HOURS", "26"))
HEALTH_MAX_FAILED_SQL = int(_env("HEALTH_MAX_FAILED_SQL", "200"))

# L3 向量构建(6.4):bge 服务化端点与 Chroma 服务地址;
# EMBEDDING_URL 未配置时用确定性 HashEmbedder(测试/无模型环境),
# CHROMA_HOST 未配置或 chromadb 不可用时 vectorize 静默跳过
EMBEDDING_URL = _env("EMBEDDING_URL")
CHROMA_HOST = _env("CHROMA_HOST")
CHROMA_PORT = int(_env("CHROMA_PORT", "8000"))

# 层级推断规则:表名前缀 -> layer;domain 默认取库名,可被调度平台登记覆盖
LAYER_PREFIX_RULES = (
    ("ods_", "ods"), ("dwd_", "dwd"), ("d_", "dwd"), ("dws_", "dws"),
    ("ads_", "ads"), ("dim_", "dim"), ("tmp_", "tmp"), ("temp_", "tmp"),
)
TMP_TABLE_PATTERNS = ("tmp_", "temp_", "_tmp")


def derive_layer(table_name: str) -> str:
    for prefix, layer in LAYER_PREFIX_RULES:
        if table_name.startswith(prefix):
            return layer
    return ""
