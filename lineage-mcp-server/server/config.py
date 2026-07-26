import os

MYSQL = dict(
    host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
    port=int(os.environ.get("MYSQL_PORT", "3306")),
    database=os.environ.get("MYSQL_DB", "datavein"),
    user=os.environ.get("MYSQL_USER", "datavein"),
    password=os.environ.get("MYSQL_PASSWORD", ""),
    charset="utf8mb4",
)
DB_POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "20"))

REDIS_URL = os.environ.get("REDIS_URL", "")

MCP_HOST = os.environ.get("MCP_HOST", "0.0.0.0")
MCP_PORT = int(os.environ.get("MCP_PORT", "8080"))

MAX_DEPTH = 5                 # get_lineage 深度上限(8.1)
MAX_NODES = 200               # 单响应节点上限,超限截断分页并提示缩深(8.1)

# 热点缓存 TTL 到次日重建时刻(设计方案 12 章:血缘日更,TTL 到日级)
REBUILD_HOUR = int(os.environ.get("REBUILD_HOUR", "3"))
