-- 附录A-05 审计与反馈(设计方案 4.5)
CREATE TABLE IF NOT EXISTS query_audit (
  audit_id           BIGINT PRIMARY KEY AUTO_INCREMENT,
  session_id         VARCHAR(64),
  user_id            VARCHAR(64),
  channel            VARCHAR(16),          -- claude_code/portal/im
  question           TEXT,
  tool_trace         JSON,                 -- 工具调用序列:名称/参数摘要/耗时/结果refs
  final_sql          MEDIUMTEXT,           -- 实际执行 SQL(取数类)
  sql_channel        VARCHAR(16),          -- semantic/adhoc
  metric_version     VARCHAR(64),          -- plan 引用的指标版本(7.3)
  row_policy_applied TEXT,                 -- 实际注入的行权限条件,越权追查依据
  result_rows        INT,
  cost_ms            INT,
  status             VARCHAR(16),          -- ok/refused/error/masked
  refuse_reason      VARCHAR(64),
  is_customer_query  TINYINT DEFAULT 0,    -- 按客户明细查询单独标记(11章合规)
  created_at         DATETIME,
  KEY idx_user_time (user_id, created_at),
  KEY idx_time (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS feedback (
  fb_id         BIGINT PRIMARY KEY AUTO_INCREMENT,
  audit_id      BIGINT,
  user_id       VARCHAR(64),
  rating        TINYINT,                   -- 1 好评 / 0 差评
  comment       TEXT,
  triage_status VARCHAR(16) DEFAULT 'pending',  -- pending/fixed/wontfix
  fix_type      VARCHAR(32),               -- glossary/lineage_patch/metric/prompt/fewshot
  created_at    DATETIME,
  KEY idx_audit (audit_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
