"""门户配置。MySQL 连接沿用 server.config(MYSQL_HOST 等同名环境变量),不重复定义。"""

import os

# LLM OpenAI 兼容接口(设计方案 9 章)。LLM_BASE_URL 未配置 → 降级模式(9.3)
LLM_BASE_URL: str = os.environ.get("LLM_BASE_URL", "").rstrip("/")
LLM_API_KEY: str = os.environ.get("LLM_API_KEY", "")
LLM_MODEL: str = os.environ.get("LLM_MODEL", "qwen-max")
LLM_TIMEOUT: int = int(os.environ.get("LLM_TIMEOUT", "60"))       # 单次请求超时(秒)
LLM_MAX_TOOL_ROUNDS: int = 5                                       # function-calling 工具循环上限

PORTAL_HOST: str = os.environ.get("PORTAL_HOST", "0.0.0.0")
PORTAL_PORT: int = int(os.environ.get("PORTAL_PORT", "8090"))
