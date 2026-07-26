"""query-gateway 服务入口(设计方案 8.2/11 章)。

POST /query/metric  语义层通道:plan + user_ctx → 数据+SQL+口径+指标版本
POST /query/adhoc   兜底通道:question + tables + user_ctx → 强制标注"自由查询"
GET  /metrics       指标清单(Agent 组织 plan 用)
服务间认证:X-Gateway-Token 共享密钥(外层另有 nginx;数据权限在编译期,与此无关)。
"""

import logging

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel

from . import adhoc, audit, compiler, config, executor
from .metrics_repo import MetricsRepo

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("gateway")

app = FastAPI(title="datavein-query-gateway")
repo = MetricsRepo(config.METRICS_DIR)
_llm = adhoc.HttpLLM()


def _auth(x_gateway_token: str = Header(default="")):
    if config.GATEWAY_TOKEN and x_gateway_token != config.GATEWAY_TOKEN:
        raise HTTPException(401, "invalid gateway token")


class MetricQuery(BaseModel):
    plan: dict
    user_ctx: dict


class AdhocQuery(BaseModel):
    question: str
    tables: list[str]
    user_ctx: dict


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/metrics", dependencies=[Depends(_auth)])
def list_metrics():
    return {"metrics": repo.list()}


@app.post("/query/metric", dependencies=[Depends(_auth)])
def query_metric(q: MetricQuery):
    metric = repo.get(q.plan.get("metric", ""))
    if metric is None:
        return {"error": "unknown_metric",
                "hint": f"可用指标: {[m['metric'] for m in repo.list()]}"}
    try:
        c = compiler.compile_plan(metric, q.plan, q.user_ctx)
        result = executor.execute(c.sql, c.masked_columns)
    except compiler.PlanError as e:
        audit.record(q.user_ctx, str(q.plan), "semantic", status="refused",
                     refuse_reason="plan_error")
        return {"error": "plan_error", "hint": str(e)}       # Agent 凭 hint 自纠(7.1)
    except (compiler.PermissionDenied, executor.ExecutionRejected) as e:
        audit.record(q.user_ctx, str(q.plan), "semantic", status="refused",
                     refuse_reason=type(e).__name__)
        log.warning("refused metric query user=%s: %s",
                    q.user_ctx.get("user_id"), e)
        return {"error": "permission_denied", "hint": str(e)}
    audit.record(q.user_ctx, str(q.plan), "semantic", final_sql=c.sql,
                 metric_version=f"{c.metric}@v{c.metric_version}",
                 row_policy_applied=c.row_policy_applied,
                 result_rows=result["row_count"], cost_ms=result["cost_ms"],
                 is_customer_query=c.is_customer_query)
    return {"data": result, "sql": c.sql, "caliber": c.caliber,
            "metric_version": f"{c.metric}@v{c.metric_version}",
            "channel_label": "语义层认证"}


@app.post("/query/adhoc", dependencies=[Depends(_auth)])
def query_adhoc(q: AdhocQuery):
    try:
        result = adhoc.run_adhoc(q.question, q.tables, q.user_ctx,
                                 _llm, audit._connect())
    except executor.ExecutionRejected as e:
        audit.record(q.user_ctx, q.question, "adhoc", status="refused",
                     refuse_reason="rejected")
        return {"error": "rejected", "hint": str(e)}
    audit.record(q.user_ctx, q.question, "adhoc", final_sql=result["sql"],
                 result_rows=result["row_count"], cost_ms=result["cost_ms"])
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=config.GATEWAY_HOST, port=config.GATEWAY_PORT)
