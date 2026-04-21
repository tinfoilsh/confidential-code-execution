FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash coreutils curl jq \
    && rm -rf /var/lib/apt/lists/*
COPY main.py /opt/executor/main.py
RUN mkdir -p /workspace
WORKDIR /workspace
CMD ["python", "/opt/executor/main.py"]
