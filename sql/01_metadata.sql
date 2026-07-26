-- 附录A-01 元数据(设计方案 4.1)
CREATE TABLE IF NOT EXISTS table_metadata (
  full_name    VARCHAR(320) PRIMARY KEY,   -- db.table,全局唯一键
  db_name      VARCHAR(128),
  table_name   VARCHAR(255),
  table_type   VARCHAR(16),                -- table/view/external
  layer        VARCHAR(16),                -- ods/dwd/dws/ads/dim/tmp
  domain       VARCHAR(64),
  comment      TEXT,
  owner        VARCHAR(64),
  update_freq  VARCHAR(32),
  is_online    TINYINT DEFAULT 1,          -- 下线标记,僵尸治理用(5.7)
  synced_at    DATETIME,
  KEY idx_layer_domain (layer, domain),
  FULLTEXT KEY ft_comment (table_name, comment) WITH PARSER ngram
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS column_metadata (
  full_name    VARCHAR(320),
  column_name  VARCHAR(255),
  data_type    VARCHAR(64),
  comment      TEXT,
  is_sensitive TINYINT DEFAULT 0,
  sens_level   VARCHAR(8),                 -- L1-L4 数据安全分级,脱敏策略依据
  PRIMARY KEY (full_name, column_name),
  FULLTEXT KEY ft_comment (column_name, comment) WITH PARSER ngram
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 解析用结构快照,按日版本化(4.5/5.2)
CREATE TABLE IF NOT EXISTS schema_snapshot (
  snap_id      BIGINT PRIMARY KEY AUTO_INCREMENT,
  full_name    VARCHAR(320),
  snap_date    DATE,
  columns_json JSON,                       -- [{name,type}] 有序数组
  UNIQUE KEY uk (full_name, snap_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
