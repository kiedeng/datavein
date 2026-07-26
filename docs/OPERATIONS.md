# 生产部署与运维手册

对应设计方案 12 章。目标读者:平台主研(1 人主研兼运维)与值班接手人。

## 1. 部署架构

```
Claude Code / 门户后端
        │ https (Bearer token)
     nginx:443 ── TLS 终结 + token 校验 + 负载均衡
        ├── mcp-server-1:8080 ┐ 无状态双副本
        └── mcp-server-2:8080 ┘
              │
   MySQL(元数据/血缘/闭包/审计) Redis(热点缓存) Chroma(向量,M3 起)
              ▲
   lineage-pipeline(批处理,cron 调起,单点可接受,失败重跑)
```

凭据边界(11 章红线):mcp-server 只持有本平台 MySQL 只读账号;数仓凭据只存在于
query-gateway(M4);nginx 的 MCP_AUTH_TOKEN 只是接入认证,不是数据权限。

## 2. 首次部署

```bash
# 1. 准备目录与证书(行内 CA 签发,放 /data/datavein/tls/server.{crt,key})
mkdir -p /data/datavein/{mysql,redis,chroma,backup,tls}

# 2. 配置(全部秘钥走 .env,不入 Git)
cp .env.example .env && vim .env    # 必填:MYSQL_*、METASTORE_DB_*、SCHEDULER_*、MCP_AUTH_TOKEN

# 3. 起服务与建库
docker compose -f deploy/docker-compose.prod.yml --env-file .env up -d mysql redis
make init-db
docker compose -f deploy/docker-compose.prod.yml --env-file .env up -d

# 4. 首次数据装载(顺序不可乱)
python -m pipeline.cli sync-metadata     # 元数据 + schema 快照
python -m pipeline.cli collect-sql       # 存量 SQL 导入(M0 准出:入仓≥90%)
python -m pipeline.cli full-rebuild      # 全量血缘 + 闭包
python -m pipeline.cli coverage          # 核对覆盖率,对照 M1 准出指标
```

## 3. 日常调度(crontab)

```cron
0  1 * * *  cd /data/datavein && python -m pipeline.cli sync-metadata
30 1 * * *  /data/datavein/deploy/backup.sh
0  2 * * *  cd /data/datavein && python -m pipeline.cli collect-sql
0  3 * * *  cd /data/datavein && python -m pipeline.cli full-rebuild
0  8 * * *  cd /data/datavein && python -m pipeline.cli coverage > /data/datavein/coverage-$(date +\%F).json
*/15 * * * *  cd /data/datavein && python -m pipeline.cli healthcheck || <告警webhook命令>
```

增量解析不走 cron:调度平台任务发布钩子调用
`python -m pipeline.cli incremental --sql-id <id>`(或包一层内部 HTTP 触发器)。

## 4. 监控与告警

`healthcheck` 命令覆盖:全量重建时效(>26h 告警)、failed 队列规模、闭包表空、
上次重建 aborted。此外值班需关注:

| 指标 | 数据源 | 阈值建议 |
|---|---|---|
| 重建耗时趋势 | pipeline_run.stats_json | 超 2h 窗口预警 |
| crosscheck_mismatch 数 | sql_repository.parse_reason | 周清零(纳入周会) |
| 接口 P95 | nginx access log | 血缘 <500ms,影响分析 <2s |
| Redis 命中率 | redis INFO | 持续 <50% 检查缓存键设计 |
| 磁盘 | 系统 | mysql 卷 >80% 扩容 |

## 5. 故障处置手册

**全量重建 aborted(影子表校验未过)**
现象:healthcheck 报 last_full_rebuild_aborted;线上血缘停留在昨日版本(仍可服务)。
处置:查 pipeline_run.stats_json 的 delta 数值 → 若因大批任务真实下线/上线导致,
确认后调大 REBUILD_EDGE_DELTA_MAX 重跑;若边数异常归零,查 sql_repository 是否被清空
(collect-sql 数据源故障),修复后重跑。**没有确认原因前不要放宽阈值。**

**闭包增量修补退化(日志 "closure patch degraded")**
现象:改动核心枢纽表触发 2.2 退化保护。
处置:预期行为,立即手动触发 full-rebuild;期间影响分析结果带旧时点,如有人在做
变更评审需口头知会。

**mcp-server 单副本失活**
nginx 自动摘除(max_fails=2),无需立刻处理;两副本全失活 → 检查 MySQL 连接数与
慢查询,`docker compose restart mcp-server-1 mcp-server-2`。

**模型端点故障(Agent 不可用)**
血缘查询能力不受影响(9.3 降级):门户直连 REST 结构化模式;通知用户临时使用门户
搜索,模型恢复自动切回。**不要因模型故障重启本平台任何组件。**

**MySQL 故障恢复**
1. 恢复最近备份:`gunzip < backup.sql.gz | mysql datavein`
2. 审计/术语/人工血缘(不可再生数据)以备份为准
3. 血缘/闭包(可再生):恢复后直接 full-rebuild 重建到当日版本
RTO 目标 <4h;每季度做一次恢复演练,演练记录归档。

**误切换回滚**
影子表切换保留一代:`RENAME TABLE lineage_edge TO lineage_edge_bad,
lineage_edge_prev TO lineage_edge`(闭包表同理),回滚后排查再重建。

## 6. 安全基线

- 全部秘钥在 .env(600 权限)或行内密管,Git 仓库零秘钥;`git log -p` 定期抽查
- mcp 容器不映射宿主端口,仅 nginx 443 对外;MCP_AUTH_TOKEN 按人发放、季度轮换
- 审计日志(query_audit)保留 ≥1 年,备份纳入每日全备
- sqlglot/依赖升级:先过 CI 金标准用例,再灰度一台副本观察一日

## 7. 容量与扩容触发点

| 信号 | 动作 |
|---|---|
| lineage_edge 超 2 千万行 | 启用按 edge_level 分区(12 章预案) |
| 边数超 5 千万或需多跳图算法 | 评估 Neo4j 迁移(模型同构,一次导数) |
| 对话并发 >20 | mcp 副本扩到 3+,MySQL 读走从库 |
