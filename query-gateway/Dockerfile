FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY gateway ./gateway
COPY metrics ./metrics
RUN pip install --no-cache-dir .
EXPOSE 8090
CMD ["python", "-m", "gateway.app"]
