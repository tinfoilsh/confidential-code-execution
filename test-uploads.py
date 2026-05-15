#!/usr/bin/env python3
"""End-to-end test of /user-uploads sync.

Drops a file in buckets, then calls /mcp with a manifest pointing at it
and asserts the file appears in /user-uploads inside the enclave.

Requires running locally:
  - confidential-code-execution at $MCP_URL (default http://localhost:7070)
    started with  -tags dev  so auth + attestation are stubbed.
  - $TINFOIL_API_KEY exported (used as bearer for buckets and MCP).
  - $BUCKETS_BASE if not the prod default (https://buckets.tinfoil.sh).

Usage:
  python3 test-uploads.py
"""

import base64
import hashlib
import json
import os
import secrets
import sys
import urllib.error
import urllib.request

MCP_URL = os.environ.get("MCP_URL", "http://localhost:7070")
BUCKETS_BASE = os.environ.get("BUCKETS_BASE", "https://buckets.tinfoil.sh")
BEARER = os.environ.get("TINFOIL_API_KEY")

if not BEARER:
    sys.exit("TINFOIL_API_KEY is required")


def hex64() -> str:
    return secrets.token_hex(32)


def b64url32() -> tuple[str, str]:
    """Return (url-safe-no-padding, standard-with-padding) of 32 random bytes."""
    raw = secrets.token_bytes(32)
    return (
        base64.urlsafe_b64encode(raw).rstrip(b"=").decode(),
        base64.b64encode(raw).decode(),
    )


def post_json(url: str, headers: dict, body: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def put_bucket_item(file_id: str, content: bytes, key_std_b64: str) -> None:
    body = {
        "value": base64.b64encode(content).decode(),
        "encryption_keys": [key_std_b64],
    }
    req = urllib.request.Request(
        f"{BUCKETS_BASE}/items/{file_id}",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {BEARER}",
            "Content-Type": "application/json",
        },
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"bucket PUT failed: {resp.status} {resp.read()!r}")


def main() -> None:
    access_token = hex64()
    container_auth_token = hex64()
    enc_key_url, enc_key_std = b64url32()

    file_id = hex64()
    filename = "hello.txt"
    content = f"hello from /user-uploads (run id {secrets.token_hex(4)})\n".encode()
    sha256 = hashlib.sha256(content).hexdigest()

    print(f"PUT bucket item file_id={file_id} sha256={sha256}")
    put_bucket_item(file_id, content, enc_key_std)

    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "bash",
            "arguments": {
                "command": (
                    "set -e; "
                    "echo '--- ls /user-uploads ---'; ls -la /user-uploads; "
                    f"echo '--- cat /user-uploads/{filename} ---'; cat /user-uploads/{filename}; "
                    f"echo '--- write attempt (should fail) ---'; (touch /user-uploads/should-fail 2>&1 || echo 'write blocked OK'); "
                    f"echo '--- whoami ---'; id"
                ),
            },
            "_meta": {
                "tinfoil_code_exec": {
                    "accessToken": access_token,
                    "containerAuthToken": container_auth_token,
                    "encryptionKey": enc_key_url,
                    "uploads": [
                        {"file_id": file_id, "filename": filename, "sha256": sha256},
                    ],
                },
            },
        },
    }

    print(f"POST {MCP_URL}/mcp ...")
    try:
        resp = post_json(
            f"{MCP_URL}/mcp",
            {"Authorization": f"Bearer {BEARER}"},
            body,
        )
    except urllib.error.HTTPError as e:
        sys.exit(f"MCP call failed: {e.code} {e.read()!r}")

    print(json.dumps(resp, indent=2))

    if "error" in resp:
        sys.exit(f"MCP returned error: {resp['error']}")

    text = resp["result"]["content"][0]["text"]
    expected = content.decode().rstrip()
    if expected not in text:
        sys.exit(f"FAIL: expected file content {expected!r} not in tool output")
    if "write blocked OK" not in text:
        sys.exit(
            "FAIL: bash was able to write to /user-uploads (ro enforcement broken)"
        )
    if "uid=1001" not in text:
        sys.exit("FAIL: bash is not running as uid 1001")
    print("PASS")


if __name__ == "__main__":
    main()
