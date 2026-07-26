-- 附录A-02 SQL 仓库(设计方案 4.2)
CREATE TABLE IF NOT EXISTS sql_repository (
  sql_id        BIGINT PRIMARY KEY AUTO_INCREMENT,
  task_id       VARCHAR(128),
  task_name     VARCHAR(255),              -- 调度任务标识
  node_seq      INT,                       -- 同任务多SQL节点排序
  target_table  VARCHAR(320),
  sql_text      MEDIUMTEXT,
  sql_hash      CHAR(64),
  dialect       VARCHAR(32),
  owner         VARCHAR(64),
  domain        VARCHAR(64),
  parse_status  VARCHAR(16),               -- pending/success/degraded/failed/manual
  parse_reason  VARCHAR(32),               -- failed/degraded 原因码,'dialect_unsupported' 单列运营(5.8)
  parse_msg     TEXT,
  parse_cost_ms INT,
  is_active     TINYINT DEFAULT 1,         -- 任务下线置0,其产出边随僵尸治理清理
  updated_at    DATETIME,
  UNIQUE KEY uk (task_id, node_seq),
  KEY idx_status (parse_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
