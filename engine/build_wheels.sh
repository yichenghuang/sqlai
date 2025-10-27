#!/bin/bash

set -e

mkdir -p wheels

uv export --no-emit-project --no-editable --no-hashes \
    --format requirements.txt --output-file ./wheels/requirements.txt

echo "Building wheels in python:3.12-slim container..."
docker run -it --rm -v $(pwd)/wheels:/wheels --dns=8.8.8.8 --dns=1.1.1.1 python:3.12-slim bash -c "
    apt-get update && apt-get install -y --no-install-recommends \
        gcc build-essential pkg-config default-libmysqlclient-dev python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip install wheel && \
    pip wheel --default-timeout=100 --retries=10 -r ./wheels/requirements.txt -w /wheels
"

echo "Wheels built successfully. Contents of wheels/:"
ls -lh wheels/