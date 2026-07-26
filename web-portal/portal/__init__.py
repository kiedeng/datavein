"""web-portal 门户包。

部署形态(设计方案 8.2):门户后端与 MCP 服务同镜像拷贝,直接复用
lineage-mcp-server/server/repo.py 的查询函数与连接池,不复制逻辑;
覆盖率大盘复用 lineage-pipeline/pipeline/coverage.py。
本模块在导入时把两个兄弟包目录加入 sys.path(本地开发与镜像内目录结构一致)。
"""

import sys
from pathlib import Path

# 仓库根目录(镜像内为 /app,见 Dockerfile;本地为 git 仓库根)
_REPO_ROOT = Path(__file__).resolve().parents[2]

for _sub in ("lineage-mcp-server", "lineage-pipeline"):
    _p = _REPO_ROOT / _sub
    if _p.is_dir() and str(_p) not in sys.path:
        sys.path.insert(0, str(_p))
