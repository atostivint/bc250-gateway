#!/usr/bin/env python3
"""Test ciblé : un wake immédiat après plug-off doit attendre le délai minimal.

Le test e2e ne peut pas reproduire le cas : le cycle de récupération dort déjà
cycle_delay entre off et on. Ici on appelle directement les deux méthodes,
comme quand une requête arrive 100 ms après une extinction normale.
"""
import asyncio
import os
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
os.environ["FAKE_BASE"] = str(pathlib.Path(__file__).resolve().parent / "run_min_off")
os.environ["FAKE_BOOT_DELAY"] = "0"   # le faux wake ne doit pas bloquer 3 s
pathlib.Path(os.environ["FAKE_BASE"]).mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(ROOT))
from gateway import Config, Gateway, PowerError  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parent.parent
FAKE = ROOT / "tests" / "fake_actions.sh"


def make_gateway(delay: float) -> Gateway:
    cfg = Config()
    cfg.cycle_delay = delay
    cfg.wake_command = [str(FAKE), "wake"]
    cfg.plug_off_command = [str(FAKE), "plug-off"]
    cfg.wake_command_timeout = 10.0
    return Gateway(cfg)


async def scenario(delay: float) -> float:
    gw = make_gateway(delay)
    await gw._power_off_plug()
    t0 = time.monotonic()
    await gw._power_on()
    return time.monotonic() - t0


async def main() -> int:
    elapsed = await scenario(2.0)
    ok = elapsed >= 1.8
    print(f"délai mesuré après plug-off : {elapsed:.2f}s (attendu >= 1.80s)")
    if not ok:
        print("FAIL : la carte peut être realimentée trop tôt")
        return 1
    print("PASS : réveil différé jusqu'au délai minimal hors-tension")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
