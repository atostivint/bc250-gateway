#!/usr/bin/env python3
"""bc250-gateway — OpenAI-compatible cold-start gateway for a powered-off LLM node.

Sits in front of the BC-250's llama-server router on an always-on host:

  * request arrives -> wake the machine (smart plug on), poll until healthy,
    then stream the request through untouched;
  * stream=true requests get SSE keep-alive comments while the machine boots,
    so client read timeouts are not tripped by the ~60 s cold start;
  * /v1/models is answered from a disk cache while the node sleeps, so polling
    a model list never wakes it;
  * after a configurable idle period with no real traffic, power the machine
    down gracefully, then cut the plug (order matters: never cut a live OS).

The idle detector lives here, not on the node: the gateway sees every real
request. Fail-safe direction is deliberate -- if this process dies, the node
simply stays awake (money, not data).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

LOG = logging.getLogger("bc250-gateway")

HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "host",
}


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
@dataclass
class Config:
    listen: str = "127.0.0.1"
    port: int = 8100
    api_key: str = ""

    backend_url: str = "http://127.0.0.1:8080"
    health_path: str = "/health"
    health_timeout: float = 3.0
    wake_timeout: float = 180.0
    poll_interval: float = 2.0
    models_cache: str = "/var/lib/bc250-gateway/models.json"

    wake_command: list[str] = field(default_factory=list)
    wake_command_timeout: float = 60.0
    # Nombre d'échecs de réveil CONSÉCUTIFS avant d'oser couper la prise.
    # À 1, un backend injoignable (adresse changée, port fermé, DNS) suffit à
    # power-cycler une machine peut-être saine : c'est ce qui a failli arriver
    # le 07/11 quand la carte est passée de .29 à .240.
    cycle_after_failures: int = 2

    shutdown_enabled: bool = True
    idle_timeout: float = 1800.0
    min_uptime: float = 300.0
    poweroff_command: list[str] = field(default_factory=list)
    plug_off_command: list[str] = field(default_factory=list)
    # Exit non-zero = someone is using the node (logged-in session, game,
    # keep-awake flag): defer the shutdown by a whole idle window.
    pre_sleep_command: list[str] = field(default_factory=list)
    pre_sleep_timeout: float = 20.0
    down_timeout: float = 120.0
    cycle_delay: float = 8.0

    keepawake_file: str = "/var/lib/bc250-gateway/keep-awake"
    monitor_interval: float = 15.0

    @classmethod
    def load(cls, path: str) -> "Config":
        raw = tomllib.loads(Path(path).read_text())
        c = cls()
        g = raw.get("gateway", {})
        c.listen = g.get("listen", c.listen)
        c.port = int(g.get("port", c.port))
        c.api_key = g.get("api_key", c.api_key)

        b = raw.get("backend", {})
        c.backend_url = b.get("base_url", c.backend_url).rstrip("/")
        c.health_path = b.get("health_path", c.health_path)
        c.health_timeout = float(b.get("health_timeout", c.health_timeout))
        c.wake_timeout = float(b.get("wake_timeout", c.wake_timeout))
        c.poll_interval = float(b.get("poll_interval", c.poll_interval))
        c.models_cache = b.get("models_cache", c.models_cache)

        w = raw.get("wake", {})
        c.wake_command = list(w.get("command", []))
        c.wake_command_timeout = float(w.get("timeout", c.wake_command_timeout))
        c.cycle_after_failures = max(1, int(w.get("cycle_after_failures", c.cycle_after_failures)))

        s = raw.get("shutdown", {})
        c.shutdown_enabled = bool(s.get("enabled", c.shutdown_enabled))
        c.idle_timeout = float(s.get("idle_timeout", c.idle_timeout))
        c.min_uptime = float(s.get("min_uptime", c.min_uptime))
        c.poweroff_command = list(s.get("poweroff_command", []))
        c.plug_off_command = list(s.get("plug_off_command", []))
        c.pre_sleep_command = list(s.get("pre_sleep_command", []))
        c.pre_sleep_timeout = float(s.get("pre_sleep_timeout", c.pre_sleep_timeout))
        c.down_timeout = float(s.get("down_timeout", c.down_timeout))
        c.cycle_delay = float(s.get("cycle_delay", c.cycle_delay))

        k = raw.get("keepawake", {})
        c.keepawake_file = k.get("flag_file", c.keepawake_file)
        c.monitor_interval = float(b.get("monitor_interval", c.monitor_interval))
        return c


# --------------------------------------------------------------------------- #
# backend lifecycle
# --------------------------------------------------------------------------- #
class PowerError(RuntimeError):
    pass


class Gateway:
    """Sleep/wake state machine + request accounting."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.state = "asleep"          # asleep | waking | ready | sleeping
        self.ready_since = 0.0
        self.last_real_request = 0.0
        # Requêtes en cours de traitement. Une génération qui dure plus que
        # idle_timeout ne doit pas être coupée en plein milieu : tant que ce
        # compteur est > 0, node endormi = flux tué.
        self.active_requests = 0
        self.last_wake_at = 0.0
        self.wakes = 0
        self.sleeps = 0
        self.consecutive_wake_failures = 0
        self.keep_awake_mem = False
        self._lock = asyncio.Lock()
        self._models_cache: dict[str, Any] | None = None
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=None, write=60.0, pool=5.0),
            # No idle keep-alive to the backend: a pooled socket would look like
            # live traffic to anything counting ESTABLISHED connections on 8080.
            limits=httpx.Limits(max_keepalive_connections=0, max_connections=64),
            headers={"connection": "close"},
            trust_env=False,
        )
        self._load_models_cache()

    # -- small helpers ------------------------------------------------------ #
    def _load_models_cache(self) -> None:
        try:
            self._models_cache = json.loads(Path(self.cfg.models_cache).read_text())
        except Exception:
            self._models_cache = None

    def _store_models_cache(self, payload: dict[str, Any]) -> None:
        self._models_cache = payload
        try:
            p = Path(self.cfg.models_cache)
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload))
            tmp.replace(p)
        except Exception as exc:  # cache is best-effort, never fatal
            LOG.warning("models cache write failed: %s", exc)

    async def healthy(self) -> bool:
        try:
            r = await self._client.get(
                self.cfg.backend_url + self.cfg.health_path,
                timeout=self.cfg.health_timeout,
            )
            return r.status_code == 200
        except Exception:
            return False

    def keep_awake(self) -> bool:
        if self.keep_awake_mem:
            return True
        try:
            return Path(self.cfg.keepawake_file).read_text().strip() in {"1", "on", "true"}
        except Exception:
            return False

    async def _run(self, argv: list[str], timeout: float) -> int:
        if not argv:
            raise PowerError("no command configured")
        LOG.info("run: %s", " ".join(argv))
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")},
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout)
        except asyncio.TimeoutError:
            proc.kill()
            raise PowerError(f"command timed out after {timeout}s: {argv[0]}")
        if proc.returncode != 0:
            raise PowerError(
                f"command failed ({proc.returncode}): {argv[0]}: "
                f"{err.decode(errors='replace').strip()[:300]}"
            )
        if out:
            LOG.debug("out: %s", out.decode(errors="replace").strip()[:300])
        return proc.returncode

    async def _run_ok(self, argv: list[str], timeout: float) -> bool:
        """Run a guard command: True on exit 0, False on any failure or exit != 0."""
        try:
            await self._run(argv, timeout)
            return True
        except PowerError as exc:
            LOG.info("guard command says no: %s", exc)
            return False

    # -- wake --------------------------------------------------------------- #
    async def _poll_until_healthy(self, deadline: float) -> bool:
        while time.monotonic() < deadline:
            if await self.healthy():
                return True
            await asyncio.sleep(self.cfg.poll_interval)
        return False

    async def ensure_ready(self) -> bool:
        """Wake the backend if needed. Single flight: concurrent callers share one wake."""
        if self.state == "ready":
            return True
        async with self._lock:
            if self.state == "ready":
                return True
            self.state = "waking"
            try:
                ok = await self._wake_sequence()
            except PowerError as exc:
                LOG.error("wake failed: %s", exc)
                ok = False
            if ok:
                self.state = "ready"
                self.ready_since = time.monotonic()
                self.wakes += 1
                LOG.info("backend ready (cold start %.1fs)", time.monotonic() - self.last_wake_at)
            else:
                self.state = "asleep"
            return ok

    async def _wake_sequence(self) -> bool:
        self.last_wake_at = time.monotonic()
        # Already up (powered by hand, or plug-off failed last time)?
        if await self.healthy():
            self.consecutive_wake_failures = 0
            return True
        # Un réveil a déjà échoué : la carte est peut-être alimentée sans démarrer,
        # et "on" sur une prise déjà allumée ne fait rien. Plutôt que de repayer un
        # wake_timeout complet pour rien, on cycle tout de suite. Le premier échec,
        # lui, ne cycle pas (adresse déplacée = machine saine).
        if self.consecutive_wake_failures >= 1:
            LOG.warning("échec de réveil précédent (%d) : cycle de prise immédiat",
                        self.consecutive_wake_failures)
            await self._power_off_plug()
            await asyncio.sleep(self.cfg.cycle_delay)
            if not await self._power_on():
                return False
            ok = await self._poll_until_healthy(time.monotonic() + self.cfg.wake_timeout)
            self.consecutive_wake_failures = 0 if ok else self.consecutive_wake_failures + 1
            return ok

        if not await self._power_on():
            return False
        if await self._poll_until_healthy(time.monotonic() + self.cfg.wake_timeout):
            self.consecutive_wake_failures = 0
            return True

        # La prise est passée à ON mais le backend ne répond pas. Deux causes
        # opposées : la machine est morte (cycle utile) ou elle est vivante sur
        # une autre adresse / un autre port (cycle = coupure de courant sur une
        # machine saine). On ne tranche pas au premier échec.
        self.consecutive_wake_failures += 1
        if self.consecutive_wake_failures < self.cfg.cycle_after_failures:
            LOG.error(
                "pas de /health après %.0fs (échec %d/%d) : la prise reste en l'état, "
                "PAS de cycle. Vérifier l'adresse du backend (%s) et l'état de la prise. "
                "Un second échec consécutif déclenchera le cycle.",
                self.cfg.wake_timeout, self.consecutive_wake_failures,
                self.cfg.cycle_after_failures, self.cfg.backend_url)
            return False

        LOG.warning("pas de /health après %.0fs sur %d réveils consécutifs : cycle de prise",
                    self.cfg.wake_timeout, self.consecutive_wake_failures)
        await self._power_off_plug()
        await asyncio.sleep(self.cfg.cycle_delay)
        if not await self._power_on():
            return False
        ok = await self._poll_until_healthy(time.monotonic() + self.cfg.wake_timeout)
        if ok:
            self.consecutive_wake_failures = 0
        return ok

    async def _power_on(self) -> bool:
        if not self.cfg.wake_command:
            raise PowerError("wake.command is not configured")
        await self._run(self.cfg.wake_command, self.cfg.wake_command_timeout)
        return True

    async def _power_off_plug(self) -> None:
        if self.cfg.plug_off_command:
            try:
                await self._run(self.cfg.plug_off_command, self.cfg.wake_command_timeout)
            except PowerError as exc:
                LOG.error("plug-off failed: %s", exc)
                raise

    # -- sleep -------------------------------------------------------------- #
    async def sleep_backend(self) -> bool:
        async with self._lock:
            if self.state != "ready":
                return False
            self.state = "sleeping"
            if self.cfg.pre_sleep_command and not await self._run_ok(
                    self.cfg.pre_sleep_command, self.cfg.pre_sleep_timeout):
                # Occupied: restart the idle clock instead of looping every tick.
                self.last_real_request = time.monotonic()
                self.ready_since = time.monotonic()
                self.state = "ready"
                LOG.info("node is in use, shutdown deferred")
                return False
            LOG.info("idle %.0fs -> shutting the node down", time.monotonic() - self.last_real_request)
            try:
                if self.cfg.poweroff_command:
                    await self._run(self.cfg.poweroff_command, 60)
                # Wait for the OS to actually be down before cutting the 12 V line.
                deadline = time.monotonic() + self.cfg.down_timeout
                while time.monotonic() < deadline:
                    if not await self.healthy():
                        break
                    await asyncio.sleep(2)
                else:
                    LOG.error("node still healthy after %.0fs; NOT cutting the plug",
                              self.cfg.down_timeout)
                    self.state = "ready"
                    return False
                await self._power_off_plug()
                self.sleeps += 1
                LOG.info("node down, plug off")
                self.state = "asleep"
                return True
            except PowerError as exc:
                LOG.error("sleep failed: %s", exc)
                self.state = "ready" if await self.healthy() else "asleep"
                return False

    # -- background --------------------------------------------------------- #
    async def monitor_loop(self) -> None:
        """Notice the node going away (self-shutdown, crash, unplugged) and the idle clock."""
        while True:
            await asyncio.sleep(min(self.cfg.monitor_interval, 5)
                                 if self.state == "waking" else self.cfg.monitor_interval)
            try:
                if self.state == "ready":
                    if not await self.healthy():
                        # one retry: a single blip must not flip the state
                        await asyncio.sleep(2)
                        if not await self.healthy():
                            LOG.warning("backend unreachable -> asleep")
                            self.state = "asleep"
                        continue
                    self.consecutive_wake_failures = 0
                    if self._idle_expired():
                        await self.sleep_backend()
                elif self.state in {"asleep", "sleeping"}:
                    self.state = "asleep"
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("monitor error")

    def _idle_expired(self) -> bool:
        if not self.cfg.shutdown_enabled or self.keep_awake():
            return False
        if self.active_requests > 0:
            # Quelqu'un génère : ce n'est pas de l'inactivité, même si la
            # requête a commencé il y a plus longtemps que idle_timeout.
            return False
        now = time.monotonic()
        if now - self.ready_since < self.cfg.min_uptime:
            return False
        last = max(self.last_real_request, self.ready_since)
        return (now - last) >= self.cfg.idle_timeout


# --------------------------------------------------------------------------- #
# app
# --------------------------------------------------------------------------- #
def build_app(gw: Gateway) -> FastAPI:
    cfg = gw.cfg
    app = FastAPI(title="bc250-gateway", docs_url=None, redoc_url=None, openapi_url=None)

    def authorized(request: Request) -> bool:
        if not cfg.api_key:
            return True
        auth = request.headers.get("authorization", "")
        return auth == f"Bearer {cfg.api_key}"

    def status_payload() -> dict[str, Any]:
        now = time.monotonic()
        return {
            "state": gw.state,
            "wakes": gw.wakes,
            "sleeps": gw.sleeps,
            "wake_failures": gw.consecutive_wake_failures,
            "keep_awake": gw.keep_awake(),
            "active_requests": gw.active_requests,
            "idle_for_s": round(now - max(gw.last_real_request, gw.ready_since), 1)
            if gw.state == "ready"
            else None,
            "idle_timeout_s": cfg.idle_timeout,
            "backend": cfg.backend_url,
        }

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse({"status": "ok", "backend": gw.state})

    @app.get("/admin/status")
    async def admin_status(request: Request) -> JSONResponse:
        if not authorized(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return JSONResponse(status_payload())

    @app.post("/admin/wake")
    async def admin_wake(request: Request) -> JSONResponse:
        if not authorized(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        ok = await gw.ensure_ready()
        return JSONResponse({**status_payload(), "ok": ok}, status_code=200 if ok else 503)

    @app.post("/admin/keep-awake")
    async def admin_keep_awake(request: Request) -> JSONResponse:
        if not authorized(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        body = await request.json()
        enabled = bool(body.get("enabled", True))
        gw.keep_awake_mem = enabled
        try:
            Path(cfg.keepawake_file).parent.mkdir(parents=True, exist_ok=True)
            Path(cfg.keepawake_file).write_text("1\n" if enabled else "0\n")
            persisted = True
        except Exception as exc:
            LOG.warning("keep-awake flag not persisted: %s", exc)
            persisted = False
        return JSONResponse({"keep_awake": enabled, "persisted": persisted})

    @app.post("/admin/sleep")
    async def admin_sleep(request: Request) -> JSONResponse:
        if not authorized(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        ok = await gw.sleep_backend()
        return JSONResponse({**status_payload(), "ok": ok}, status_code=200 if ok else 409)

    # -- OpenAI surface ----------------------------------------------------- #
    @app.get("/v1/models")
    async def models(request: Request) -> Response:
        if not authorized(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        if gw.state == "ready":
            try:
                r = await gw._client.get(cfg.backend_url + "/v1/models", timeout=10.0)
                if r.status_code == 200:
                    try:
                        gw._store_models_cache(r.json())
                    except Exception:
                        pass
                    return Response(r.content, status_code=200,
                                    media_type=r.headers.get("content-type", "application/json"))
            except Exception:
                pass
        if gw._models_cache is not None:
            return JSONResponse(gw._models_cache, headers={"x-bc250-state": gw.state})
        # cold cache: one wake to bootstrap, then cache forever
        if await gw.ensure_ready():
            try:
                r = await gw._client.get(cfg.backend_url + "/v1/models", timeout=10.0)
                gw._store_models_cache(r.json())
                return Response(r.content, status_code=200,
                                media_type=r.headers.get("content-type", "application/json"))
            except Exception as exc:
                return JSONResponse({"error": f"backend error: {exc}"}, status_code=502)
        return JSONResponse({"error": "backend unavailable"}, status_code=503)

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
    async def proxy(path: str, request: Request) -> Response:
        if not authorized(request):
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        body = await request.body()
        want_stream = False
        if body:
            try:
                payload = json.loads(body)
                want_stream = bool(payload.get("stream"))
            except Exception:
                want_stream = False

        # Real traffic resets the idle clock. /v1/models deliberately does not.
        # It is refreshed again when the response ends, and on every streamed
        # chunk: a request that starts at T and generates for an hour must not
        # look idle since T. While it runs, active_requests > 0 blocks the sleep.
        gw.last_real_request = time.monotonic()
        gw.active_requests += 1
        handed_to_stream = False
        try:
            fwd_headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_HEADERS}
            url = f"{cfg.backend_url}/{path}"
            if request.url.query:
                url += "?" + request.url.query

            if gw.state == "ready":
                ok = True
            elif want_stream:
                # Can't know yet: hold the SSE response open with comments while it boots.
                handed_to_stream = True
                return StreamingResponse(
                    _tracked(gw, _wake_then_stream(gw, request.method, url, fwd_headers, body)),
                    media_type="text/event-stream",
                    headers={"cache-control": "no-cache", "x-bc250-state": "waking"},
                )
            else:
                ok = await gw.ensure_ready()

            if not ok:
                return JSONResponse(
                    {"error": {"message": "BC-250 failed to wake",
                               "type": "backend_unavailable",
                               "wake_failures": gw.consecutive_wake_failures,
                               "hint": "plug is on but the backend never answered \u2014 if this is the "
                                       "first failure the plug is left alone; check the backend "
                                       "address before retrying"}},
                    status_code=503,
                )

            if want_stream:
                handed_to_stream = True
                return StreamingResponse(
                    _tracked(gw, _stream_upstream(gw, request.method, url, fwd_headers, body)),
                    media_type="text/event-stream",
                    headers={"cache-control": "no-cache"},
                )

            try:
                r = await gw._client.request(request.method, url, headers=fwd_headers, content=body,
                                             timeout=httpx.Timeout(600.0, connect=10.0))
            except httpx.RequestError as exc:
                gw.state = "asleep"
                return JSONResponse({"error": {"message": f"backend error: {exc}"}}, status_code=502)
            out_headers = {k: v for k, v in r.headers.items() if k.lower() not in HOP_HEADERS}
            return Response(r.content, status_code=r.status_code, headers=out_headers)
        finally:
            gw.last_real_request = time.monotonic()
            if not handed_to_stream:
                gw.active_requests = max(0, gw.active_requests - 1)

    return app


def _tracked(gw: Gateway, agen):
    """Suivent une réponse streamée : horloge rafraîchie à chaque morceau, et
    requête décrémentée seulement quand le flux se termine réellement."""
    async def run():
        try:
            async for chunk in agen:
                gw.last_real_request = time.monotonic()
                yield chunk
        finally:
            gw.active_requests = max(0, gw.active_requests - 1)
            gw.last_real_request = time.monotonic()
    return run()


async def _stream_upstream(gw: Gateway, method: str, url: str, headers: dict[str, str],
                           body: bytes):
    try:
        async with gw._client.stream(method, url, headers=headers, content=body,
                                     timeout=httpx.Timeout(connect=10.0, read=None,
                                                           write=60.0, pool=5.0)) as r:
            if r.status_code >= 400:
                detail = (await r.aread()).decode(errors="replace")
                yield f"event: error\ndata: {json.dumps({'status': r.status_code, 'detail': detail[:2000]})}\n\n".encode()
                return
            async for chunk in r.aiter_raw():
                yield chunk
    except httpx.RequestError as exc:
        gw.state = "asleep"
        yield f"event: error\ndata: {json.dumps({'detail': f'backend error: {exc}'})}\n\n".encode()


async def _wake_then_stream(gw: Gateway, method: str, url: str, headers: dict[str, str],
                            body: bytes):
    """Emit SSE comments while the machine boots, then the real stream.

    The comment lines are ignored by every SSE client (OpenAI SDK included) but
    keep the socket warm: a cold start is ~50 s of firmware + userspace.
    """
    yield b": bc250-waking\n\n"
    wake_task = asyncio.create_task(gw.ensure_ready())
    started = time.monotonic()
    state_hint = 5
    while not wake_task.done():
        done, _ = await asyncio.wait({wake_task}, timeout=state_hint)
        if done:
            break
        yield f": bc250-waking {int(time.monotonic() - started)}s\n\n".encode()
        state_hint = min(state_hint + 5, 20)
    ok = wake_task.result()
    if not ok:
        yield f"event: error\ndata: {json.dumps({'detail': 'BC-250 failed to wake'})}\n\n".encode()
        return
    yield f": bc250-ready {int(time.monotonic() - started)}s\n\n".encode()
    async for chunk in _stream_upstream(gw, method, url, headers, body):
        yield chunk


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> int:
    cfg_path = os.environ.get("BC250_GATEWAY_CONFIG", "/etc/bc250-gateway/config.toml")
    logging.basicConfig(
        level=os.environ.get("BC250_GATEWAY_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    cfg = Config.load(cfg_path)
    gw = Gateway(cfg)
    app = build_app(gw)

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.monitor = asyncio.create_task(gw.monitor_loop())
        if await gw.healthy():
            gw.state = "ready"
            gw.ready_since = time.monotonic()
            LOG.info("backend already up at start")

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        task = getattr(app.state, "monitor", None)
        if task:
            task.cancel()

    LOG.info("listening on %s:%s -> %s", cfg.listen, cfg.port, cfg.backend_url)
    uvicorn.run(app, host=cfg.listen, port=cfg.port, log_level="warning", access_log=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())