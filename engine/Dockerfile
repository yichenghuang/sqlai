FROM python:3.12-slim

RUN --mount=type=bind,source=wheels,target=/app/wheels \
    apt-get update && apt-get install -y  libmariadb3 && \
    pip install --no-cache-dir --no-index \
    --find-links=/app/wheels -r /app/wheels/requirements.txt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY src/ ./src/
COPY mcp_server.py .

ENV PYTHONPATH=/app/src

CMD ["fastmcp", "run", "mcp_server.py", "--transport", "http"]