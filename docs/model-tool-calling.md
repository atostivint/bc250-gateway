# Tool calling on the models behind this gateway

Which of the local models can actually drive an agent loop, measured through this
gateway — not inferred from model cards. Useful before pointing a coding agent's
worker role at `bc250/gpt-oss-20b-MXFP4` and friends.

Measured 2026-11-08 against llama.cpp `b12020` (`--jinja`, one `llama-server` per
model, Q8_0 KV cache). Re-run it after upgrading llama.cpp or swapping a GGUF:
chat templates live inside the GGUF and change with it.

## Two tests, because they answer different questions

1. **Capability** — one request carrying `tools[]` and `tool_choice: "auto"`.
   Either the reply has structured `tool_calls`, or it has prose imitating them.
2. **End to end** — a three-tool loop (`read_file`, `str_replace`, `finish`), two
   bounded tasks on a throwaway git repo: append three accented French lines to a
   markdown file, and fix an off-by-operator bug in a function. Success means the
   file really changed **and** a check run outside the model passed (`git diff` plus
   an assertion). A model claiming success proves nothing: a 12B once wrote a
   `cargo test … ok` line and no test file on disk.

```bash
curl -s https://<gateway>/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "gpt-oss-20b-MXFP4", "temperature": 0, "max_tokens": 300,
  "messages": [{"role": "user", "content": "List the files in the current directory."}],
  "tools": [{"type": "function", "function": {"name": "list_dir",
    "description": "List files in a directory",
    "parameters": {"type": "object", "properties": {"path": {"type": "string"}},
                   "required": ["path"]}}}],
  "tool_choice": "auto"}'
# structured: choices[0].message.tool_calls is non-null, finish_reason == "tool_calls"
# imitation:  tool_calls is null and the text holds {"name": ...} or <function name=...>
```

The tool set has to match the question. Offering only `read_file` and asking to
list a directory makes a correct model answer "I can't list files" — which reads
as "no tool support" and is a false negative.

## Results

| Model | Structured `tool_calls` | Edit task A | Edit task B | Warm latency |
|---|---|---|---|---|
| `gpt-oss-20b-MXFP4` | yes | pass | pass | ~4 s |
| `Qwen3-8B-Q4_K_M` | yes, **thinking off** | pass | pass | ~4 s |
| `Qwen3-14B-Q4_K_M` | yes, **thinking off** | pass | pass | ~8 s |
| `NVIDIA-Nemotron-3.5-Bolt-E4B` | yes | pass | pass | ~11 s |
| `Mellum2-12B-A2.5B-Instruct` | yes | pass (6 turns) | pass | ~5 s |
| `LFM2-8B-A1B` | yes in the probe | **fail** | **fail** | — |
| `Qwen2.5-Coder-7B-Instruct` | **no** | fail | fail | — |
| `gemma-3-12b-it` | no (`python` `tool_code` blocks) | not run | not run | — |
| `SenseNova-Loop-9B-Preview` | — | — | — | fails to load (`500`) |

Exercised for tool calling but not for edits, still untested: `TwIL-8B`,
`Nanbeige4.2-3B`, `Spark-X2.6-4B`, `Ternary-Bonsai-2-9B`, `OpenAI-20B-NEOPlus`,
`LFM3-1.7B`.

## The four traps, in the order they cost time

**1. Qwen3 needs its thinking switched off.** With reasoning on, `max_tokens` is
spent on the reasoning trace: `finish_reason: "length"`, zero `tool_calls`, and an
empty `content` that looks like a broken template. Send
`"chat_template_kwargs": {"enable_thinking": false}` (or `/no_think`) and the same
model returns a perfectly formed call. A client that cannot pass extra body fields
will conclude the model has no tool support, wrongly.

**2. A failed tool call must be fed back.** `Mellum2` copied `Relire le diff` as
`Reluire le diff`; the `str_replace` was rejected, and the model read the file
again and fixed it — but only because the harness returned the error text in a
`role: "tool"` message. Swallow the error and the model loops, then summarizes
work it never did. Cheapest reliability fix available: return the verbatim error,
once.

**3. `ctx-size` is decided by the server, not guessed by the client.** `/v1/models`
reports the flags each model was launched with (131072 for `gpt-oss-20b` and
`Mellum2`, 32768 for most others). "The model dropped my context" usually means a
client-side cap, and blaming the brief for it sends you tuning the wrong thing.

**4. A cold start and a broken model look alike.** Two `502`s in a batch run were
memory pressure from the previous model still resident (16 GB shared between CPU
and GPU); `Mellum2` answered normally when retried alone. `SenseNova-Loop-9B`, by
contrast, fails every time with `model failed to load`. Retry once before drawing
a conclusion, and read the error body — it distinguishes the two.

## When the model has no structured tool calling

It can still edit files if the harness drops the tool protocol and uses text. The
orchestrator injects the file content, the model returns exactly one
SEARCH/REPLACE block, the orchestrator applies it with exact matching and retries
once with the error. `Qwen2.5-Coder-7B`, useless as a tool-calling agent, then
passes both edit tasks in 1-3.4 s:

```
<<<<<<< SEARCH
    return a - b
=======
    return a + b
>>>>>>> REPLACE
```

Two conditions: the model must never be asked to *report* file state (it invents
it — asked to read a file, the same 7B printed a plausible file it had never
seen), and the block format suits the model. Strong tool-calling models given this
prompt often answer with the corrected code and no markers at all, so pick the
protocol per model rather than globally.

## What this implies for a worker role

- Default worker: `gpt-oss-20b-MXFP4`. It was clean on both tasks at every attempt.
- `Qwen3-14B` / `Qwen3-8B` are fine alternatives *if* the client sends
  `enable_thinking: false`.
- Never let the model build or test. It returns a diff; the orchestrator runs the
  build and the tests. An unproven "all green" is worse than a failure.
- Briefs stay short and one action per turn, but that is about attention, not
  about a 32k wall that does not exist here.
