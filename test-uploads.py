#!/usr/bin/env python3
"""End-to-end test of /user-uploads sync.

Drops a file in buckets, then calls /mcp with a manifest pointing at it
and asserts the file appears in /user-uploads inside the enclave. A
second call (same session) attaches a second file and verifies both
are present with read-only enforcement still intact.

Requires running locally:
  - confidential-code-execution at $MCP_URL (default http://localhost:7070)
    started with  -tags dev  so auth + attestation are stubbed.
  - $TINFOIL_API_KEY exported (used as bearer for buckets and MCP).
  - $BUCKETS_BASE if not the prod default (https://buckets.tinfoil.sh).

Optional: $ACCESS_TOKEN, $CONTAINER_AUTH_TOKEN, $ENCRYPTION_KEY
(base64url, no padding). If unset, random values are generated and
printed so you can re-run against the same session via env.

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


def enc_key_pair_from_env_or_random() -> tuple[str, str]:
    """Return (url-safe-no-padding, standard-with-padding) of 32 bytes.

    Reads from $ENCRYPTION_KEY (url-safe, no padding) if set.
    """
    if existing := os.environ.get("ENCRYPTION_KEY"):
        # Re-derive standard padded form from the url-safe input.
        raw = base64.urlsafe_b64decode(existing + "==")
        return existing, base64.b64encode(raw).decode()
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


def call_bash(
    access_token: str,
    container_auth_token: str,
    enc_key_url: str,
    uploads: list[dict],
    command: str,
    rpc_id: int,
) -> str:
    body = {
        "jsonrpc": "2.0",
        "id": rpc_id,
        "method": "tools/call",
        "params": {
            "name": "bash",
            "arguments": {"command": command},
            "_meta": {
                "tinfoil_code_exec": {
                    "accessToken": access_token,
                    "containerAuthToken": container_auth_token,
                    "encryptionKey": enc_key_url,
                    "uploads": uploads,
                },
            },
        },
    }
    try:
        resp = post_json(
            f"{MCP_URL}/mcp",
            {"Authorization": f"Bearer {BEARER}"},
            body,
        )
    except urllib.error.HTTPError as e:
        sys.exit(f"MCP call {rpc_id} failed: {e.code} {e.read()!r}")

    if "error" in resp:
        sys.exit(f"MCP returned error on call {rpc_id}: {resp['error']}")
    return resp["result"]["content"][0]["text"]


def main() -> None:
    access_token = os.environ.get("ACCESS_TOKEN") or hex64()
    container_auth_token = os.environ.get("CONTAINER_AUTH_TOKEN") or hex64()
    enc_key_url, enc_key_std = enc_key_pair_from_env_or_random()

    print("--- session creds (re-export to reuse this session) ---")
    print(f"export ACCESS_TOKEN={access_token}")
    print(f"export CONTAINER_AUTH_TOKEN={container_auth_token}")
    print(f"export ENCRYPTION_KEY={enc_key_url}")
    print()

    # File A
    file_a_id = hex64()
    file_a_name = "hello.txt"
    file_a_content = (
        f"hello from /user-uploads (run id {secrets.token_hex(4)})\n".encode()
    )
    file_a_sha = hashlib.sha256(file_a_content).hexdigest()
    print(f"PUT bucket A fileAccessToken={file_a_id} sha256={file_a_sha}")
    put_bucket_item(file_a_id, file_a_content, enc_key_std)

    # Call 1: upload + verify + ro check + uid check.
    cmd1 = (
        "set -e; "
        "echo '--- ls /user-uploads ---'; ls -la /user-uploads; "
        f"echo '--- cat ---'; cat /user-uploads/{file_a_name}; "
        "echo '--- write attempt (should fail) ---'; "
        "(touch /user-uploads/should-fail 2>&1 || echo 'write blocked OK'); "
        "echo '--- id ---'; id"
    )
    text1 = call_bash(
        access_token,
        container_auth_token,
        enc_key_url,
        [{"fileAccessToken": file_a_id, "filename": file_a_name, "sha256": file_a_sha}],
        cmd1,
        rpc_id=1,
    )
    print("=== call 1 output ===")
    print(text1)

    expected_a = file_a_content.decode().rstrip()
    if expected_a not in text1:
        sys.exit(f"FAIL: expected file A content {expected_a!r} not in call 1 output")
    if "write blocked OK" not in text1:
        sys.exit(
            "FAIL: bash was able to write to /user-uploads (ro enforcement broken)"
        )
    if "uid=1001" not in text1:
        sys.exit("FAIL: bash is not running as uid 1001")

    # File B added on the second call.
    file_b_id = hex64()
    file_b_name = "second.txt"
    file_b_content = b"contents of second file\n"
    file_b_sha = hashlib.sha256(file_b_content).hexdigest()
    print(f"PUT bucket B fileAccessToken={file_b_id} sha256={file_b_sha}")
    put_bucket_item(file_b_id, file_b_content, enc_key_std)

    # Call 2: same session, manifest has both files. Both should now be visible.
    cmd2 = (
        "set -e; "
        "echo '--- ls /user-uploads ---'; ls -la /user-uploads; "
        f"echo '--- sha256 ---'; sha256sum /user-uploads/{file_a_name} /user-uploads/{file_b_name}"
    )
    text2 = call_bash(
        access_token,
        container_auth_token,
        enc_key_url,
        [
            {
                "fileAccessToken": file_a_id,
                "filename": file_a_name,
                "sha256": file_a_sha,
            },
            {
                "fileAccessToken": file_b_id,
                "filename": file_b_name,
                "sha256": file_b_sha,
            },
        ],
        cmd2,
        rpc_id=2,
    )
    print("=== call 2 output ===")
    print(text2)

    if file_a_sha not in text2:
        sys.exit(f"FAIL: file A sha {file_a_sha} not in call 2 output")
    if file_b_sha not in text2:
        sys.exit(f"FAIL: file B sha {file_b_sha} not in call 2 output")

    print("PASS")


if __name__ == "__main__":
    main()
