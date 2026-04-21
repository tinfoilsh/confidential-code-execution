```bash
docker exec -it code-executor bash
```

```bash
curl -X POST http://localhost:8000/exec -H "Content-Type: application/json" -d '{"command": "echo hello world > /workspace/hello.txt"}'
```
