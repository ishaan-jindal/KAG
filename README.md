# KAG — Issue-to-Repro Harness

> Stop chasing ghosts. Paste an issue, get back proof.

Every maintainer burns hours just proving a bug is real. Logs pasted, steps missing, "works on my machine." KAG kills that: give it a GitHub issue, get back a minimal `repro.ts` that fails deterministically — or it tells you it can't.

## How it works

```
GitHub issue text
  │  (TS harness + UI on DigitalOcean)
  ▼
Open-weight LLM via DO Serverless Inference (Qwen 3.5 / gpt-oss-20b)
  │  generates candidate repro.ts
  ▼
Sandbox: `node --permission` × 3 runs
  │  parses exit code + stack, normalizes trace → signature
  ▼
Deterministic repro or honest "cannot-repro" + JSONL replay log
```

Loop max 3 attempts × 3 runs. No flakes: a repro only counts if it fails the same way 3×.

Optional demo flex: same prompt against self-hosted `qwen2.5-coder:7b` via Ollama on an RTX 4000 Ada GPU droplet ($0.76/hr) with one env-var swap, then destroy.

## Why open-weight AI is core

The model *is* the engine — generation, failure clustering, and the judge. No OpenAI API. Swap any DO-hosted open-weight model (Llama 4 Maverick, Ministral 3 14B, Gemma 4, DeepSeek V4) with one env var.

## Components

| File | Scope | What it does |
|---|---|---|
| `src/tools.ts` ~130 LOC | CORE | Sandbox exec, `normalizeStack`, `signature` |
| `src/verify.ts` ~70 LOC | CORE | Pure determinism gate |
| `src/harness.ts` ~180 LOC | CORE | Generate + retry loop + JSONL + CLI + `--replay` + `judge(prose)` |
| `src/server.ts` ~120 LOC | DEMO | `node:http` + SSE UI, no framework |
| `src/eval/` ~50 LOC + data | CORE | 5 fixtures + `suite.json` + `run.ts` scorer |
| `SKILL.md` | CORE | Agent Skill wrapper, closed verdict vocabulary |
| `LICENSE` | CORE | Apache-2.0 |

~550 LOC ceiling (core harness ~430 + UI ~120). Eval fixtures are data, not LOC. No Python. No LangChain. No vector DB.

Server endpoints: `GET /` (form + stream + replay) · `POST /run` → `{ id }` · `GET /events/:id` (SSE over JSONL tail) · `GET /repro/:id` · `GET /replay/:id`. SSE because runs are server→client only — one schema, two sinks (file + UI).

## Determinism gate

Signature = `TIMEOUT` | `SIG:<sig>` | `EXIT:<code>|<errClass>|<frames>` | `NO_FAIL`. Message text excluded; class + frames only.

| 3 runs | Verdict |
|---|---|
| 3× identical non-`NO_FAIL` sig | `deterministic` (`mode: crash` or `hang`) |
| 3× `NO_FAIL` | `no-failure` |
| mixed sigs | `flaky` |
| 3× load-time errors | `invalid-candidate` |

Sandbox: `node --permission` FS-only (no `--allow-net` on Node 22) locked to attempt dir, no child-process/worker/addons allowances, 10s SIGKILL, `max-old-space-size=128`, minimal env. Exit code alone untrusted — fingerprint stderr for `ERR_ACCESS_DENIED`. Network is prompt-hardened, not permission-isolated — determinism, not containment.

`cannot-repro` after 3 honest negatives, with per-attempt verdict kinds in the final log line. `judge(prose)` is one extra model call turning verdicts into a human summary — display-only, the structured verdict is the contract.

## Eval suite + scoring

5 fixtures: `crash-deterministic`, `flaky-race`, `volatile-trace`, `hang`, `not-a-bug`. Each declares `verdict` + `mode` (hard pass/fail), `sigContains` class check (right bug, not just any bug), plus telemetry bounds `maxAttempts` / `reproMaxLoc` and `replay: true`.

Score 0–100, diagnostic only: 60 verdict accuracy · 15 minimality · 15 attempt efficiency · 10 replay integrity. Plus footer: model, tokens in/out, est. cost, wall-clock. `npm run eval -- --model <m>` appends to `eval/leaderboard.jsonl` — the model bake-off table proving the one-env-var swap.

## Architecture ($30 DO budget)

- **$5 App Platform container** — UI + harness + ephemeral `runs/*.jsonl`, `repros/*` on one box
- **DO Serverless Inference** — pay-per-token, ~$0.01/demo (`openai-gpt-oss-120b`: $0.10 in / $0.70 out per 1M; `qwen3.5-397b-a17b`: $0.55 / $3.50)
- **Functions free tier** — trigger-only second door for `SKILL.md` callers (no sandbox inside; scale-to-zero, keeps demo box undisturbed)
- **GPU only on stage** — RTX 4000 Ada ~$2–3 for a 3–4 hr demo, destroy same day

Worst case ≈ $13. Typical demo < $5. No DO Spaces — ephemeral logs keep the always-on budget at $5.

## SKILL.md contract

Closed verdict set only: `deterministic(crash|hang)` · `cannot-repro {no-failure|flaky|invalid-candidate}`. Exactly two artifacts per run (`repros/<id>.repro.ts`, `runs/<id>.jsonl`). Honest-negative rule in writing: never present a candidate that failed the 3× gate as a repro. Max 3 attempts × 3 runs, ~$0.01/attempt at `openai-gpt-oss-120b` rates.

## Demo script (~6 min)

1. Paste deterministic crash → SSE streams generate → 3 runs → sig
2. Open 20-line `repro.ts` — the minimal promise
3. Hit **replay** — identical sig
4. Paste flaky race → `cannot-repro (flaky)` — the trust beat, never cut
5. `export DO_INFERENCE_URL=http://<droplet>:11434/v1` → rerun on GPU Ollama
6. `doctl compute droplet delete` + cost receipt

## Repo layout

```
SKILL.md            # Agent Skill (Open Standard) — wraps KAG
src/harness.ts      # issue → generate → sandbox → verify → retry
src/tools.ts        # sandbox exec, trace parse
src/verify.ts       # determinism gate (3× same fail)
src/server.ts       # HTTP + SSE UI, stdlib only
src/eval/           # 5 fixtures + suite.json + run.ts scorer
Dockerfile, app.yaml # App Platform container configuration
demo/               # 6-beat checklist & runbook for judges
LICENSE             # Apache License 2.0
TEAM.md             # Multi-lane responsibilities & frozen contracts
```

## Quickstart

```bash
# 1. Install dependencies
npm i

# 2. Configure model inference (DO Serverless or Ollama)
export DO_INFERENCE_KEY=...
export DO_MODEL=openai-gpt-oss-120b # or qwen3.5-397b-a17b

# 3. Run harness against an issue or fixture
npm run kag -- --issue src/eval/fixtures/crash-deterministic.md
# → Generates ./repros/<id>.repro.ts and ./runs/<id>.jsonl

# 4. Verify replay integrity
npm run kag -- --replay <id>

# 5. Run evaluation suite
npm run eval

# 6. Start local Web UI & SSE server (port 8080)
npm start
```

Headless API: `POST /run { "issue": "..." }`. GPU swap: `export DO_INFERENCE_URL=http://<droplet>:11434/v1`.

## Status

Active multi-lane implementation in progress per [TEAM.md](TEAM.md):
- **Lane 1 (Sandbox + Gate):** `src/tools.ts`, `src/verify.ts`
- **Lane 2 (Loop + Eval):** `src/harness.ts`, `src/eval/`
- **Lane 3 (UI + Deploy):** `src/server.ts`, `Dockerfile`, `app.yaml`
- **Lane 4 (Skill + Demo + Entry):** `SKILL.md`, `LICENSE`, `demo/CHECKLIST.md`, README

See [`demo/CHECKLIST.md`](demo/CHECKLIST.md) for the ~6-minute judge demonstration script.

## License

[Apache License 2.0](LICENSE) — Copyright (c) 2026 KAG contributors.

