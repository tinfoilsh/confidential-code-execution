Test that the code executor docker is working & handles executing code:

```bash
docker exec -it code-executor bash
```

```bash
curl -X POST http://localhost:8000/exec \
  -H "Content-Type: application/json" \
  -d '{
    "echo hello > /workspace/hello.txt\ncat /workspace/hello.txt"
  }'
```
