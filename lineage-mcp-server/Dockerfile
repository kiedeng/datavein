FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY server ./server
RUN pip install --no-cache-dir .
EXPOSE 8080
CMD ["python", "-m", "server.app"]
