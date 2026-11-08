#!/usr/bin/env python3
"""Stand-in for the BC-250 llama-server router, used to test the gateway.

It only listens while the file $FAKE_BASE/up exists, so "the machine is off"
really means "connections are refused" -- which is what the gateway must see.
"""
import asyncio
import json
import os
import pathlib
import re
import sys

BASE = pathlib.Path(os.environ["FAKE_BASE"])
UP = BASE / "up"
LOG = BASE / "events.log"
PORT = int(os.environ.get("FAKE_PORT", "9101"))

MODELS = {
    "object": "list",
    "data": [
        {"id": "gpt-oss-20b-MXFP4", "object": "model", "owned_by": "llamacpp"},
        {"id": "Qwen3-8B-Q4_K_M", "object": "model", "owned_by": "llamacpp"},
    ],
}


def log(event: str) -> None:
    with LOG.open("a") as fh:
        fh.write(event + "\n")


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        # A single read() is not a request: headers and body can arrive in separate
        # segments, and a body-less read makes a streaming call look non-streaming.
        raw = b""
        while b"\r\n\r\n" not in raw:
            chunk = await reader.read(65536)
            if not chunk:
                return
            raw += chunk
        head_raw, _, body_raw = raw.partition(b"\r\n\r\n")
        length = re.search(rb"content-length:\s*(\d+)", head_raw, re.IGNORECASE)
        if length:
            need = int(length.group(1))
            while len(body_raw) < need:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                body_raw += chunk
        head = head_raw.decode("latin-1")
        headers = {}
        for raw_line in head.split("\r\n")[1:]:
            if ":" in raw_line:
                k, v = raw_line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        line = head.split("\r\n")[0]
        parts = line.split(" ")
        method, path = (parts + ["", ""])[0], (parts + ["", ""])[1]
        body_text = body_raw.decode("latin-1")

        if path.startswith("/health"):
            payload = b'{"status":"ok"}'
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         + f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
            await writer.drain()
            return

        if path.startswith("/v1/models"):
            log("backend:/v1/models")
            payload = json.dumps(MODELS).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         + f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
            await writer.drain()
            return

        if path.startswith("/v1/chat/completions"):
            log("backend:chat")
            try:
                req = json.loads(body_text)
            except Exception:
                req = {}
            if req.get("stream"):
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                             b"Cache-Control: no-cache\r\nTransfer-Encoding: chunked\r\n\r\n")
                await writer.drain()
                tokens = int(headers.get("x-test-tokens", "4"))
                delay = float(headers.get("x-test-delay", "0.25"))
                for i in range(tokens):
                    chunk = (f'data: {json.dumps({"object": "chat.completion.chunk", "choices": [{"delta": {"content": f"tok{i} "}}]})}\n\n').encode()
                    writer.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                    await writer.drain()
                    await asyncio.sleep(delay)
                writer.write(b"0\r\n\r\n")
                await writer.drain()
                return
            slow = float(headers.get("x-test-delay", "0") or 0)
            if slow:
                await asyncio.sleep(slow)
            payload = json.dumps({
                "object": "chat.completion",
                "model": req.get("model", "gpt-oss-20b-MXFP4"),
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "pong"},
                             "finish_reason": "stop"}],
            }).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                         + f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
            await writer.drain()
            return

        payload = b'{"error":"not found"}'
        writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n"
                     + f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
        await writer.drain()
    except Exception as exc:  # noqa: BLE001
        log(f"backend:error {exc!r}")
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main() -> None:
    server: asyncio.AbstractServer | None = None
    log("fake:start")
    while True:
        wanted = UP.exists()
        if wanted and server is None:
            server = await asyncio.start_server(handle, "127.0.0.1", PORT)
            log("fake:listening")
        elif not wanted and server is not None:
            server.close()
            await server.wait_closed()
            server = None
            log("fake:stopped")
        await asyncio.sleep(0.25)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
