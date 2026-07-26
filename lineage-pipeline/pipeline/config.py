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

SQL_DIALECT_DEFAULT = _env("SQL_DIALECT_DEFAULT", "hive")
PARSE_TIMEOUT_SECONDS = int(_env("PARSE_TIMEOUT_SECONDS", "60"))
PARALLEL_WORKERS = int(_env("PARALLEL_WORKERS", "8"))

# 闭包增量修补退化阈值:|ancestors|×|descendants| 超过则转异步全量(设计方案 2.2)
CLOSURE_INCR_DEGRADE_THRESHOLD = int(_env("CLOSURE_INCR_DEGRADE_THRESHOLD", "5000000"))

HIVE_METASTORE_URI = _env("HIVE_METASTORE_URI")
SCHEDULER_API_BASE = _env("SCHEDULER_API_BASE")
SCHEDULER_API_TOKEN = _env("SCHEDULER_API_TOKEN")

# tmp 表命名规则,穿透折叠用(5.3);layer=tmp 之外的兜底判定
TMP_TABLE_PATTERNS = ("tmp_", "temp_", "_tmp")
