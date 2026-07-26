# 门户镜像:与 lineage-mcp-server / lineage-pipeline 同镜像拷贝复用(设计方案 8.2)
# 构建上下文必须是仓库根目录:docker build -f web-portal/Dockerfile -t web-portal .
FROM python:3.12-slim
WORKDIR /app
# 保持与仓库一致的相对目录结构,portal 包按 parents[2] 定位兄弟包(见 portal/__init__.py)
COPY lineage-mcp-server/server ./lineage-mcp-server/server
COPY lineage-pipeline/pipeline ./lineage-pipeline/pipeline
COPY web-portal/pyproject.toml ./web-portal/
COPY web-portal/portal ./web-portal/portal
RUN pip install --no-cache-dir ./web-portal \
    && pip install --no-cache-dir "sqlglot==26.16.4" "sqllineage>=1.5" "redis>=5.0" "requests>=2.31" "rapidfuzz>=3.0"
WORKDIR /app/web-portal
EXPOSE 8090
CMD ["python", "-m", "portal.app"]
