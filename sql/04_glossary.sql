-- 附录A-04 术语字典与 UDF 注册(设计方案 4.4)
CREATE TABLE IF NOT EXISTS biz_glossary (
  term_id     BIGINT PRIMARY KEY AUTO_INCREMENT,
  term        VARCHAR(128) NOT NULL,
  aliases     JSON,                        -- 别名/口语叫法数组
  caliber     TEXT,                        -- 权威口径描述(出处、统计规则、例外)
  domain      VARCHAR(64),
  owner       VARCHAR(64),
  ref_table   VARCHAR(320),
  ref_column  VARCHAR(255),                -- 锚定物理字段(可空)
  metric_name VARCHAR(128),                -- 关联语义层指标(可空)
  certified   TINYINT DEFAULT 0,           -- owner 钦定权威条目,消歧置顶(6.3)
  sensitivity VARCHAR(8),
  status      VARCHAR(16) DEFAULT 'active',   -- active/pending_review/deprecated
  updated_by  VARCHAR(64),
  updated_at  DATETIME,
  UNIQUE KEY uk_term_domain (term, domain),
  FULLTEXT KEY ft_term (term, caliber) WITH PARSER ngram
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS udf_registry (
  udf_name      VARCHAR(128) PRIMARY KEY,
  semantic_desc TEXT,                      -- 供 Agent 解释表达式(5.3)
  owner         VARCHAR(64),
  updated_at    DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
