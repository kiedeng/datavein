"""web-portal:FastAPI 门户应用(设计方案 10 章;M3+M5 纯开发部分)。

- POST /api/chat:SSE 流式对话;LLM 可用走 function-calling 工具循环(9 章),
  不可用/异常自动降级为结构化直通卡片(9.3);每次对话落 query_audit(4.5)。
- 大盘/术语/补录为 REST 直调(8.2:非对话功能不经模型)。
- 用户身份:读 X-User 头,缺省 'anonymous'。
  TODO(行内任务):接入行内 SSO 网关,校验签名票据后覆写 X-User,门户不自建登录。
"""

import json
import logging
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from server import repo  # sys.path 由 portal/__init__.py 注入(8.2)

from . import cards, config, llm, store

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("portal")

BASE_DIR = Path(__file__).resolve().parent
app = FastAPI(title="datavein web-portal")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


def _user(request: Request) -> str:
    """用户身份:X-User 头,缺省 anonymous。TODO:行内 SSO 票据校验(见模块注释)。"""
    return (request.headers.get("X-User") or "anonymous").strip()[:64] or "anonymous"


def _sse(event: str, payload: dict) -> str:
    """SSE 组帧:event + 单行 JSON data。"""
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"


# ---------- 页面 ----------

@app.get("/", response_class=HTMLResponse)
def page_chat(request: Request):
    return templates.TemplateResponse(request, "chat.html",
                                      {"llm_enabled": llm.enabled()})


@app.get("/dashboard", response_class=HTMLResponse)
def page_dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html", {})


@app.get("/glossary", response_class=HTMLResponse)
def page_glossary(request: Request):
    return templates.TemplateResponse(request, "glossary.html", {})


@app.get("/backfill", response_class=HTMLResponse)
def page_backfill(request: Request):
    return templates.TemplateResponse(request, "backfill.html", {})


# ---------- 对话(SSE) ----------

@app.post("/api/chat")
async def api_chat(request: Request):
    body = await request.json()
    question = (body.get("question") or "").strip()
    if not question:
        raise HTTPException(422, "question 不能为空")
    session_id = body.get("session_id") or uuid.uuid4().hex
    history = body.get("history") or []                # 多轮:前端回传近几轮消息(9.4)
    user = _user(request)

    def gen() -> Iterator[str]:
        t0 = time.monotonic()
        trace: list[dict] = []
        status, refuse_reason = "ok", ""
        degraded = not llm.enabled()
        yield _sse("meta", {"session_id": session_id, "degraded": degraded})
        try:
            if not degraded:
                try:
                    for event, payload in llm.run_conversation(question, history, trace):
                        yield _sse(event, payload)
                except Exception as exc:               # LLM 不可用/超时 → 自动降级(9.3)
                    log.warning("LLM 调用失败,切换降级模式: %s", exc)
                    degraded = True
                    yield _sse("notice", {"text": "模型暂不可用,已切换结构化直通模式(9.3),"
                                                  "以下为检索/血缘原始结果。"})
            if degraded:
                conn = repo.connect()
                try:
                    for card in cards.degraded_answer(conn, question, trace):
                        yield _sse("card", card)
                finally:
                    conn.close()
        except Exception as exc:
            status, refuse_reason = "error", "portal_exception"
            log.exception("chat 处理异常")
            yield _sse("error", {"text": f"系统异常:{exc}"})
        cost_ms = int((time.monotonic() - t0) * 1000)
        audit_id = store.write_audit(user, session_id, question, trace,
                                     status, cost_ms, refuse_reason)
        yield _sse("done", {"audit_id": audit_id, "degraded": degraded,
                            "cost_ms": cost_ms})

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


# ---------- 反馈(4.5 feedback) ----------

@app.post("/api/feedback")
async def api_feedback(request: Request):
    body = await request.json()
    rating = body.get("rating")
    if rating not in (0, 1):
        raise HTTPException(422, "rating 必须为 1(好评)或 0(差评)")
    if not body.get("audit_id"):
        raise HTTPException(422, "audit_id 不能为空")
    fb_id = store.write_feedback(_user(request), int(body["audit_id"]),
                                 int(rating), body.get("comment") or "")
    return {"fb_id": fb_id}


# ---------- 覆盖率大盘(5.6) ----------

@app.get("/api/dashboard")
def api_dashboard():
    return store.dashboard()


# ---------- 术语管理(4.4) ----------

@app.get("/api/glossary")
def api_glossary_list(status: str | None = None, keyword: str | None = None,
                      domain: str | None = None):
    return {"items": store.list_glossary(status, keyword, domain)}


@app.post("/api/glossary")
async def api_glossary_create(request: Request):
    body = await request.json()
    if not (body.get("term") or "").strip():
        raise HTTPException(422, "term 不能为空")
    try:
        term_id = store.create_glossary(body, _user(request))
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    except Exception as exc:                           # uk(term,domain) 冲突等
        if "Duplicate" in str(exc):
            raise HTTPException(409, "同域下术语已存在(uk_term_domain)")
        raise
    return {"term_id": term_id}


@app.put("/api/glossary/{term_id}")
async def api_glossary_update(term_id: int, request: Request):
    body = await request.json()
    try:
        updated = store.update_glossary(term_id, body, _user(request))
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    if not updated:
        raise HTTPException(404, "术语不存在或无可更新字段")
    return {"term_id": term_id, "updated": True}


# ---------- 血缘补录(M3) ----------

@app.get("/api/backfill/failed")
def api_backfill_failed(limit: int = 100):
    items = store.list_failed_sql(limit)
    return {"total": len(items), "items": items}


@app.post("/api/lineage/manual")
async def api_lineage_manual(request: Request):
    body = await request.json()
    dst = (body.get("dst_full_name") or "").strip()
    sources = body.get("sources") or []
    if not dst or "." not in dst:
        raise HTTPException(422, "dst_full_name 必须为 db.table 全名")
    if not sources or any("." not in (s.get("full_name") or "") for s in sources):
        raise HTTPException(422, "sources 至少一条,且 full_name 必须为 db.table 全名")
    result = store.manual_lineage(dst, (body.get("dst_column") or "").strip(),
                                  sources, _user(request), body.get("sql_id"))
    return result


# ---------- 可信取数占位(M4:语义层/查询网关未上线) ----------

@app.post("/api/metric/query")
def api_metric_query():
    """SQL 收据卡片的后端占位:M4 接入 query-gateway 后返回 数据+SQL+口径+指标版本(7 章)。"""
    return {"error": "not_implemented", "milestone": "M4",
            "hint": "语义层与查询网关未上线,前端收据卡片样式已就绪"}


if __name__ == "__main__":
    uvicorn.run(app, host=config.PORTAL_HOST, port=config.PORTAL_PORT)
