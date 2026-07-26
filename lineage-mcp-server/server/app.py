"""lineage-mcp-server:FastMCP streamable-http,7 工具(设计方案 8.1)。

通用约定:全部返回带 refs(sql_id 等)供溯源;工具只读幂等;
单响应节点 ≤200(config.MAX_NODES),超限截断并提示缩深。
query_metric / run_adhoc_sql 为 M4 占位,内部将调 query-gateway。
"""

import logging

from fastmcp import FastMCP

from . import config, repo
from .cache import cached

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")

mcp = FastMCP("lineage-mcp-server")


def _conn():
    return repo.connect()          # 池化连接,用完自动归还


@mcp.tool
def search_term(keyword: str, context_domain: str | None = None) -> dict:
    """术语/表/字段定位。返回血缘族归并结构:families 内 anchor 为推荐锚点,
    ambiguous=true 时表示存在异链多义,应向用户反问口径而非猜测。"""
    return repo.search_term(_conn(), keyword, context_domain)


@mcp.tool
def get_lineage(full_name: str, column: str = "", direction: str = "upstream",
                depth: int = 3, edge_level: str = "table") -> dict:
    """血缘遍历(S1)。默认表级聚合;字段级传 edge_level=column 并给 column。
    边内嵌 transform_expr/filter_cond(每跳怎么算的);confidence 低于 high 的跳
    在回答中必须提示用户核对(4.6)。depth 上限 5。"""
    args = (full_name, column, direction, depth, edge_level)
    return cached("lineage", args,
                  lambda: repo.get_lineage(_conn(), *args))


@mcp.tool
def get_lineage_path(from_table: str, to_table: str) -> dict:
    """两点间链路:先闭包判连通,连通再走边表取明细路径(M2 实现明细,先返回连通性)。"""
    conn = _conn()
    connected = repo._connected(conn, from_table, to_table)
    return {"from": from_table, "to": to_table, "connected": connected,
            "detail": "M2: 边表按最短路取逐跳明细" if connected else None}


@mcp.tool
def impact_analysis(full_name: str, group_by: str = "table",
                    page: int = 1, page_size: int = 100) -> dict:
    """影响分析(S2):闭包表一跳出全量下游,支持 group_by=task 按任务聚合与分页。"""
    args = (full_name, group_by, page, page_size)
    return cached("impact", args,
                  lambda: repo.impact_analysis(_conn(), *args))


@mcp.tool
def get_table_info(full_name: str) -> dict:
    """表元数据+字段+产出任务(S5)。"""
    return repo.get_table_info(_conn(), full_name)


@mcp.tool
def query_metric(plan: dict, user_ctx: dict) -> dict:
    """可信取数(S3,M4):plan 经语义层编译执行,返回数据+SQL+口径+指标版本。"""
    return {"error": "not_implemented", "milestone": "M4",
            "hint": "语义层与查询网关未上线,请引导用户走血缘/口径类问题"}


@mcp.tool
def run_adhoc_sql(question: str, user_ctx: dict) -> dict:
    """兜底取数(M4):受控 Text-to-SQL,结果强制标注"自由查询"。"""
    return {"error": "not_implemented", "milestone": "M4"}


if __name__ == "__main__":
    mcp.run(transport="http", host=config.MCP_HOST, port=config.MCP_PORT)
