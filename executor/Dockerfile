FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash coreutils curl jq \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY main.py .
CMD ["python", "main.py"]
