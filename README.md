# DataVein 数据血缘对话平台

面向数仓/数分团队的血缘溯源、影响分析与可信取数对话平台(自建版)。

完整设计见 [血缘对话平台系统设计方案-自建版.md](./血缘对话平台系统设计方案-自建版.md)(V2.1 评审修订版)。

## 仓库结构

```
datavein/
├── sql/                    # 附录A:全量建库 DDL(按序号执行)
├── lineage-pipeline/       # 血缘解析流水线(M1):元数据同步/SQL收集/sqlglot解析/闭包重建/覆盖率
├── lineage-mcp-server/     # FastMCP 服务(M2):search_term / get_lineage / impact_analysis 等 7 工具
├── query-gateway/          # 查询执行网关(M4):语义层编译 + 权限注入 + 只读执行(占位骨架)
├── claude-code/            # Claude Code 三件套(8.3):.mcp.json / CLAUDE.md / data-lineage Skill
└── docker-compose.yml      # mysql + redis + chroma + mcp-server 编排
```

## 快速开始(开发环境)

```bash
cp .env.example .env                 # 按需修改口令与端点
docker compose up -d mysql redis     # 起基础存储
make init-db                         # 建库建表(sql/ 按序执行)

cd lineage-pipeline && pip install -e ".[dev]"
python -m pipeline.cli sync-metadata   # 元数据同步(需配置 Metastore 连接)
python -m pipeline.cli full-rebuild    # 全量血缘重建(影子表+原子切换)
pytest                                 # 解析核心单测(离线可跑,不依赖 DB)

cd ../lineage-mcp-server && pip install -e .
python -m server.app                   # streamable-http 起服务,Claude Code 即可接入
```

## 里程碑对应

| 目录 | 里程碑 | 状态 |
|---|---|---|
| sql/ + lineage-pipeline/ | M1 血缘底座 | 骨架 + 解析/闭包核心原型 |
| lineage-mcp-server/ + claude-code/ | M2 对话 PoC | 骨架 + 血缘工具原型(取数两工具为 M4 占位) |
| query-gateway/ | M4 可信取数 | 占位 |
| web-portal/ | M5 正式门户 | 未建 |

## 工程约定

- sqlglot 版本在各组件 pyproject 中**锁死**(设计方案 7.2),升级须跑通金标准用例。
- 全量重建一律写 `*_shadow` 影子表,校验通过后 `RENAME TABLE` 原子切换(4.3)。
- 血缘边按 `sql_id` 先删后插且单事务,禁止跨任务覆盖(5.3)。
