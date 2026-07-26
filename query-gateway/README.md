# query-gateway(M4 占位)

查询执行网关(设计方案 7/11 章):语义层 plan→SQL 编译 + 权限注入 + 只读执行。

执行链路:plan/SQL → 编译与静态校验 → 行权限注入 → 只读账号池执行
(LIMIT 1000 / 超时 30s / 并发 10)→ 按 sens_level 脱敏 → 审计 → 返回。

红线(硬约束,不依赖 LLM 自觉):
- 本服务是唯一持有数仓只读凭据的组件,Agent/MCP 进程无凭据
- 白名单只放行 SELECT;敏感表黑名单硬编码;DDL/DML 直接拒绝并审计告警
- 同 plan 恒同 SQL:join/filter 固定排序、sqlglot 版本锁死、golden 用例入 CI(7.2)

M4 落地清单:
- [ ] metrics/ 指标 YAML 仓库(Git 评审生效)+ Schema 校验
- [ ] compiler.py:plan 校验 → 拼 join → default_filters → row_policy → LIMIT
- [ ] executor.py:只读账号池 + 限流 + 脱敏
- [ ] audit.py:query_audit 落库(含 row_policy_applied 与指标版本)
- [ ] golden/:每指标 plan→SQL 全文比对用例
