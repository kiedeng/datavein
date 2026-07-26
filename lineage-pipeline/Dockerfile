FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY pipeline ./pipeline
RUN pip install --no-cache-dir .
ENTRYPOINT ["python", "-m", "pipeline.cli"]
CMD ["full-rebuild"]
