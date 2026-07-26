-- 附录A-06 运维:流水线运行记录(监控告警数据源,设计方案 12 章)
CREATE TABLE IF NOT EXISTS pipeline_run (
  run_id      BIGINT PRIMARY KEY AUTO_INCREMENT,
  run_type    VARCHAR(16),           -- full/incremental/metadata/collect
  status      VARCHAR(16),           -- running/success/failed/aborted
  stats_json  JSON,                  -- 解析统计/边数/闭包行数/校验结果
  error_msg   TEXT,
  started_at  DATETIME,
  finished_at DATETIME,
  KEY idx_type_time (run_type, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
