-- 附录A-03 血缘图谱与闭包(设计方案 4.3)
CREATE TABLE IF NOT EXISTS lineage_node (
  node_id     BIGINT PRIMARY KEY AUTO_INCREMENT,
  node_type   VARCHAR(16),                 -- table/column
  full_name   VARCHAR(320),
  column_name VARCHAR(255) DEFAULT '',
  UNIQUE KEY uk (full_name, column_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lineage_edge (
  edge_id        BIGINT PRIMARY KEY AUTO_INCREMENT,
  src_node_id    BIGINT NOT NULL,
  dst_node_id    BIGINT NOT NULL,
  edge_level     VARCHAR(16),              -- table/column
  sql_id         BIGINT,                   -- 溯源;ODS映射边此列空,src_type区分
  src_type       VARCHAR(16) DEFAULT 'sql',   -- sql/ingest_map/manual
  confidence     VARCHAR(8) DEFAULT 'high',   -- high/medium/low(4.6)
  transform_expr TEXT,
  filter_cond    TEXT,
  join_cond      TEXT,
  is_derived     TINYINT DEFAULT 0,        -- 临时表穿透合成边(5.3)
  is_self_loop   TINYINT DEFAULT 0,        -- 自依赖标记(5.3)
  updated_at     DATETIME,
  KEY idx_dst (dst_node_id, edge_level),
  KEY idx_src (src_node_id, edge_level),
  KEY idx_sql (sql_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 表级传递闭包,离线重算;全量重建写 shadow 后 RENAME 原子切换(4.3)
CREATE TABLE IF NOT EXISTS table_closure (
  ancestor   VARCHAR(320),
  descendant VARCHAR(320),
  min_hops   INT,
  path_cnt   INT,
  PRIMARY KEY (ancestor, descendant),
  KEY idx_desc (descendant)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
