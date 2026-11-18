#!/usr/bin/env python3
"""Realtime bridge: text and binary frames must cross the gateway untouched.

Run from the repo root with the pinned requirements installed:
    python3 tests/test_realtime_bridge.py
"""
import asyncio
import pathlib
import sys
import threading

import websockets
from fastapi.testclient import TestClient

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import gateway as gwmod  # noqa: E402

PORT = 8791
_ready = threading.Event()
_loop = asyncio.new_event_loop()


async def echo(ws):
    async for message in ws:
        await ws.send(message)


async def serve():
    server = await websockets.serve(echo, "127.0.0.1", PORT)
    _ready.set()
    await asyncio.Future()


def run_upstream():
    asyncio.set_event_loop(_loop)
    _loop.run_until_complete(serve())


def main():
    threading.Thread(target=run_upstream, daemon=True).start()
    assert _ready.wait(5), "upstream echo server did not start"

    cfg = gwmod.Config()
    cfg.backend_url = "http://127.0.0.1:1"  # dead on purpose: state is forced ready
    cfg.speech_url = f"ws://127.0.0.1:{PORT}"
    cfg.models_cache = "/tmp/bc250-gateway-test-models.json"
    gw = gwmod.Gateway(cfg)
    gw.state = "ready"
    app = gwmod.build_app(gw)

    with TestClient(app) as client:
        with client.websocket_connect("/v1/realtime") as ws:
            ws.send_text("session.update")
            assert ws.receive_text() == "session.update"
            ws.send_bytes(b"\x00\x01audio")
            assert ws.receive_bytes() == b"\x00\x01audio"

    assert gw.active_requests == 0, "socket leak: active_requests != 0"
    print("PASS: realtime bridge forwards text and binary frames, no socket leak")


if __name__ == "__main__":
    main()
