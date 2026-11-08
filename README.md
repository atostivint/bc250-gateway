# bc250-gateway

An OpenAI-compatible HTTP gateway that **powers a machine on when a request
arrives, and off again when it goes idle.**

Some inference boxes (ex-mining APUs, gaming rigs, a spare desktop with one GPU)
are cheap to run and expensive to *leave running*: 70 W of idle draw is the whole
bill. Those machines usually cannot be woken with Wake-on-LAN either — no standby
rail, dead until the 12 V arrives. The only power button is a switched socket.

This gateway sits on an always-on host, in front of the model server:

```
client ──▶ bc250-gateway ──▶ llama-server / vLLM / Ollama (OpenAI-compatible)
               │
               ├─ asleep: run the wake command (socket on) → poll /health
               ├─ stream=true: SSE comments while the machine boots
               ├─ /v1/models: answered from a disk cache, never wakes anything
               └─ idle N minutes: graceful shutdown → wait → cut the power
```

## Why not just `llama-swap` or `drove`?

Those manage **processes**. They load and unload models on one host to keep VRAM
free, and the machine stays at full idle draw — which is exactly the cost this is
meant to remove. Here the unit of control is the **power supply**.

Turnkey alternatives did not fit: `ac101m/sleepy-llm` is the closest (MIT, needs a
JVM, and its shutdown is fire-and-forget — no "wait until the OS is really down,
then cut the plug"). Writing it took one file.

## Features

- **Wake on request**, single-flight: N concurrent requests trigger one wake, not N.
- **SSE keep-alive during boot.** A cold start is ~50 s of firmware and userspace.
  `stream: true` clients receive `: bc250-waking 12s` comments so no read timeout
  fires; the real stream follows untouched.
- **`/v1/models` from a disk cache**, so a dashboard polling the model list never
  costs 70 W.
- **Two-stage sleep**: graceful shutdown command → wait until the host stops
  answering `/health` → *then* the power-off command. Cutting the 12 V on a live
  OS is a cold yank on its SSD.
- **No power cycle on the first failed wake** (`cycle_after_failures`, default 2).
  A dead machine and a machine that moved to another address look identical from
  here; only the second consecutive failure is allowed to cut and restore power.
  The first one returns a `503` with a hint.
- **Occupancy guard**: a command that may veto the shutdown (someone logged in at
  the desktop, a game running, a keep-awake flag).
- **The idle clock starts at the *end* of a request**, and is refreshed on every
  streamed chunk. A generation that runs longer than `idle_timeout` is not idle
  traffic: `active_requests` blocks the shutdown while one is in flight, so a
  40-minute completion is never cut in the middle.
- **No idle keep-alive to the upstream**: a pooled socket would look like traffic
  to anything counting established connections.
- Fail-safe direction is deliberate: **if this process dies, the machine stays
  on.** You lose money, not data.

## Quick start

```bash
python3 -m venv /opt/bc250-gateway/venv
/opt/bc250-gateway/venv/bin/pip install fastapi httpx uvicorn

cp config.example.toml /etc/bc250-gateway/config.toml   # then edit
BC250_GATEWAY_CONFIG=/etc/bc250-gateway/config.toml \
  /opt/bc250-gateway/venv/bin/python gateway.py
```

Point an OpenAI client at `http://localhost:8100/v1`. Any API key works unless you
set `gateway.api_key`.

With Docker:

```bash
docker build -t bc250-gateway:0.1.2 .
docker run --rm -p 8100:8100 -v /etc/bc250-gateway:/etc/bc250-gateway:ro \
  -v bc250-state:/var/lib/bc250-gateway bc250-gateway:0.1.2
```

## Configuration

Everything lives in one TOML file (`BC250_GATEWAY_CONFIG`). The three commands are
**argv arrays, run without a shell** — write your own scripts and point at them:

| Key | Meaning |
|---|---|
| `[backend] base_url` | the model server to protect, e.g. `http://10.0.0.10:8080` |
| `[backend] wake_timeout` | how long to poll `/health` after power-on (49-60 s is normal) |
| `[wake] command` | argv that powers the machine **on** (smart plug, IPMI, relay…) |
| `[shutdown] poweroff_command` | argv that shuts the OS down cleanly (e.g. `ssh user@host sudo systemctl poweroff`) |
| `[shutdown] plug_off_command` | argv that cuts the power, **after** the OS is down |
| `[shutdown] pre_sleep_command` | argv that returns non-zero if the machine is **in use** |
| `[shutdown] idle_timeout` | seconds without a real request before sleeping (default 1800) |
| `[wake] cycle_after_failures` | consecutive failed wakes before cutting power (default 2) |
| `[gateway] api_key` | if set, every path requires `Authorization: Bearer …` |

`bc250-plug` is a small example power command for Home Assistant (webhook or REST
token), kept free of credentials: read them from the environment.

**Do not run a second idle detector on the machine itself.** Two actors fighting
over sleep end with a plug cut under a running OS. This gateway is the single
owner because it sees the real traffic.

## Endpoints

| Path | Purpose |
|---|---|
| `POST /v1/chat/completions`, `/v1/completions`, … | proxied, streaming preserved |
| `GET /v1/models` | served live when awake, from cache when asleep |
| `GET /health` | gateway liveness (not the backend's) |
| `GET /admin/status` | state, wakes, sleeps, `wake_failures`, idle time |
| `POST /admin/wake` / `POST /admin/sleep` | manual control |
| `POST /admin/keep-awake` `{"enabled": true}` | block the idle shutdown |

## Deploying on Kubernetes

`deploy/kubernetes.yaml` is a starting point: a Deployment with one replica
(**never two** — each replica would decide on its own to sleep the machine), a
PVC for the model cache, and a ConfigMap. Create the credentials (SSH key, plug
token) as a Secret, or via External Secrets.

A private image needs a `docker-registry` pull secret in the namespace.

## Limits worth knowing before you deploy

- **Wake latency is the feature's price.** Measured on a 24 CU APU with a 12 GB
  model: 54-59 s to `/health`, 75-81 s to the first token (model load + prefill).
  Use `stream: true`, or set the client's read timeout above that.
- **Wake depends on whatever controls the socket.** A cloud integration means no
  internet, no wake. Prefer local control when you have the choice.
- **Anything that must always be available cannot live here** — including
  embeddings called on every turn: a sleeping box yields an empty answer, not an
  error.
- The gateway holds no credential of its own beyond what you put in the Secret.
  Prefer narrow capabilities (a webhook that toggles one socket) over broad ones
  (an API token that controls the whole home automation).

## License

MIT — see `LICENSE`.
