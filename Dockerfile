FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir tinfoil
COPY main.py container_manager.py mcp.py tools.py ./
CMD ["python", "main.py"]
