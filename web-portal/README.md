# web-portal(简版 Web 门户,M3+M5 纯开发部分)

血缘对话平台门户(设计方案 9/10 章):SSE 流式对话(含 LLM 降级直通模式)、
覆盖率大盘、术语管理、血缘补录、反馈闭环。

## 架构与复用(设计方案 8.2)

- **不复制查询逻辑**:门户后端直接 `import server.repo` 复用 lineage-mcp-server
  的检索/血缘/影响分析函数与连接池(`server.repo.connect()`);覆盖率大盘复用
  `pipeline.coverage` 的口径 SQL。
- **sys.path 方式引入**:`portal/__init__.py` 按仓库相对路径把
  `lineage-mcp-server/`、`lineage-pipeline/` 注入 sys.path;**部署时同镜像拷贝**
  两个包(见 Dockerfile,构建上下文必须是仓库根目录),镜像内目录结构与仓库一致。
- MCP client SDK 接入(经 streamable-http 调 MCP 服务)为后续演进方向,当前
  同镜像直调等价于 8.2 的"REST 镜像同一 handler"。

## 目录

```
portal/
  app.py        FastAPI 应用(页面 + REST + SSE)
  llm.py        可插拔 LLM 客户端:OpenAI 兼容 + function-calling 工具循环(9.1)
  cards.py      降级模式结构化卡片(9.3)
  store.py      门户写路径:审计/反馈/术语/补录 + 大盘聚合
  config.py     门户环境变量(MySQL 配置沿用 server.config)
  templates/    Jinja2 页面(对话/大盘/术语/补录)
  static/       样式(全部本地,银行内网禁止外部 CDN/字体/JS)
tests/          单元 + 集成(真实 MySQL,DV_IT_MYSQL=1 门控)
```

## 运行

```bash
pip install -e ".[dev]"            # 需已可 import server/pipeline(同仓库即可)
MYSQL_PASSWORD=xxx python -m portal.app          # http://127.0.0.1:8090
# 镜像:仓库根目录下 docker build -f web-portal/Dockerfile -t web-portal .
```

## 环境变量

| 变量 | 说明 | 默认 |
|---|---|---|
| `LLM_BASE_URL` | OpenAI 兼容接口地址(含 /v1);**不配置即降级模式(9.3)** | 空 |
| `LLM_API_KEY` | LLM 鉴权 key | 空 |
| `LLM_MODEL` | 模型名 | qwen-max |
| `LLM_TIMEOUT` | 单次 LLM 请求超时(秒) | 60 |
| `PORTAL_HOST` / `PORTAL_PORT` | 监听地址/端口 | 0.0.0.0 / 8090 |
| `MYSQL_HOST/PORT/DB/USER/PASSWORD`、`DB_POOL_SIZE` | 沿用 server.config,同 MCP 服务 | — |

## 对话模式

- **LLM 模式**:function-calling 工具循环(工具即 repo.py 的
  search_term/get_lineage/impact_analysis/get_table_info,最多 5 轮),最终回答
  SSE 流式输出;系统提示词按 9.1(先 search_term、ambiguous 反问、拒答四情形、
  表名数字以工具为准)+ 9.2 注入防护。
- **降级模式(9.3)**:`LLM_BASE_URL` 未配置或调用异常时,输入直连
  search_term/get_lineage,返回结构化卡片(检索族/血缘分跳/提示),前端渲染成卡片;
  模型恢复(配置回填重启)即切回。
- 每次对话写 `query_audit(channel='portal')`,`tool_trace` 记录工具调用序列;
  反馈按钮落 `feedback` 表。

## 前端说明

- 血缘结果当前用**嵌套列表 + 简单内联 SVG 分层图**渲染;**G6 大图卡片为后续增强**
  (10.2:表级聚合默认/点击下钻/300 节点上限/超限转清单),当前不引入 G6。
- **SQL 收据卡片**(数据表格 + SQL 代码块 + 口径说明 + "自由查询"警示样式)样式与
  渲染器已就绪,后端 `/api/metric/query` 为 M4 占位,query-gateway 上线后启用。
- 用户身份:请求头 `X-User`,缺省 `anonymous`;顶栏输入框为 SSO 上线前的临时身份
  (SSO 网关接入是行内任务,代码留 TODO)。

## 测试

```bash
cd web-portal
python -m pytest tests -q                 # 仅无 DB 单元测试
DV_IT_MYSQL=1 python -m pytest tests -q   # 集成:建独立库 datavein_portal_test
```

集成测试完整走:建库 + sql/ DDL + warehouse_fixture 种子 + full_rebuild 真实血缘,
再经 TestClient 覆盖降级 chat 卡片/大盘/术语 CRUD/补录幂等/反馈。

## 已知限制

- 人工补录边(src_type=manual)即时可被 get_lineage 查到,但 `table_closure`
  待夜间全量重建后生效(影响分析/族归并在此之前看不到新边)。
- 多轮上下文由前端回传近 8 轮文本消息,未做会话级消歧选择缓存(9.4 后续)。
- 审计查询/指标模型浏览管理页(10.1)与 G6 卡片、SQL 收据真实数据属 M4/M5 后续。
