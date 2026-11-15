#!/usr/bin/env python3
"""Régression : un transport cassé ne doit pas endormir l'état interne.

Cas réel du 11/11/2026 22:26 : un long flux a lâché (`http client error:
Failed to read connection` côté llama-server). Le proxy faisait
`gw.state = "asleep"` sans rien vérifier. À partir de là, `monitor_loop` ne
sondait plus la santé (elle ne le fait qu'en état « ready »), donc plus de
timer d'inactivité, plus de poweroff, plus de log : la carte a tourné toute la
nuit, puis une deuxième.

Ce test verrouille les deux gardes :
  1. `note_backend_error` ne bascule en « asleep » que si la santé échoue ;
  2. la réconciliation ramène un état « asleep » à « ready » quand la machine
     répond de nouveau.

Usage : PYTHON=/chemin/venv/bin/python python3 tests/test_state_drift.py
"""
import asyncio
import os
import pathlib
import subprocess
import sys
import time

import httpx

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
RUN = HERE / "run-drift"
API_PORT = int(os.environ.get("DRIFT_API_PORT", "9121"))

sys.path.insert(0, str(ROOT))
import gateway as gw_mod  # noqa: E402

ok = fail = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  PASS  {name}")
    else:
        fail += 1
        print(f"  FAIL  {name}  {detail}")


def write_config() -> pathlib.Path:
    RUN.mkdir(parents=True, exist_ok=True)
    cfg = RUN / "config.toml"
    cfg.write_text(
        f"""
[gateway]
listen = "127.0.0.1"
port = 8299
api_key = ""

[backend]
base_url = "http://127.0.0.1:{API_PORT}"
health_timeout = 1.0
models_cache = "{RUN}/models.json"
monitor_interval = 0.2
poll_interval = 0.2
wake_timeout = 5.0

[wake]
command = ["{HERE}/fake_actions.sh", "wake"]

[shutdown]
enabled = true
idle_timeout = 60.0
min_uptime = 1.0
poweroff_command = ["{HERE}/fake_actions.sh", "poweroff"]
plug_off_command = ["{HERE}/fake_actions.sh", "plug-off"]

[keepawake]
flag_file = "{RUN}/keep-awake"
"""
    )
    return cfg


async def main() -> None:
    RUN.mkdir(parents=True, exist_ok=True)
    for stale in ("up", "events.log", "keep-awake"):
        (RUN / stale).unlink(missing_ok=True)

    env = {**os.environ, "FAKE_BASE": str(RUN), "FAKE_PORT": str(API_PORT)}
    backend = subprocess.Popen([sys.executable, str(HERE / "fake_backend.py")], env=env)

    cfg = gw_mod.Config.load(str(write_config()))
    g = gw_mod.Gateway(cfg)
    try:
        # « up » = le faux llama-server accepte les connexions.
        (RUN / "up").touch()
        await asyncio.sleep(1.0)

        # 1. La machine répond : on reste « ready » et l'horloge repart de zéro.
        g.state = "ready"
        g.last_real_request = time.monotonic() - 3600
        await g.note_backend_error(httpx.ReadError("peer closed connection"), "stream")
        check("transport cassé + machine vivante -> état ready", g.state == "ready", g.state)
        check(
            "l'horloge d'inactivité est remise à zéro",
            time.monotonic() - g.last_real_request < 2,
            f"{time.monotonic() - g.last_real_request:.1f}s",
        )

        # 2. La machine est vraiment partie : là, oui, « asleep ».
        (RUN / "up").unlink(missing_ok=True)
        await asyncio.sleep(0.5)
        g.state = "ready"
        await g.note_backend_error(httpx.ConnectError("connection refused"), "proxy")
        check("transport cassé + machine muette -> état asleep", g.state == "asleep", g.state)

        # 3. Réconciliation : état faussé + machine qui répond = retour à « ready ».
        (RUN / "up").touch()
        await asyncio.sleep(0.5)
        g.state = "asleep"
        g.asleep_ticks = 3          # la prochaine passe de monitor_loop sonde la santé
        task = asyncio.create_task(g.monitor_loop())
        for _ in range(60):
            await asyncio.sleep(0.1)
            if g.state == "ready":
                break
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        check("réconciliation : état « asleep » corrigé en « ready »", g.state == "ready", g.state)
    finally:
        await g._client.aclose()
        backend.terminate()
        try:
            backend.wait(timeout=5)
        except subprocess.TimeoutExpired:
            backend.kill()

    print(f"\n{ok} passed, {fail} failed")
    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    asyncio.run(main())
