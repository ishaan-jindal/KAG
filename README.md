# KAG — Issue-to-Repro Harness

> Stop chasing ghosts. Paste an issue, get back proof.

Every maintainer burns hours just proving a bug is real. Logs pasted, steps missing, "works on my machine." KAG kills that: give it a GitHub issue, get back a minimal `repro.ts` that fails deterministically — or it tells you it can't.

## How it works

```
GitHub issue text
  │  (TS harness on DigitalOcean)
  ▼
Open-weight LLM via DO Serverless Inference (Qwen 3.5 / gpt-oss-20b)
  │  generates candidate repro.ts
  ▼
Sandbox: `node --permission` × 3 runs
  │  parses exit code + stack, clusters trace
  ▼
Deterministic repro or honest "cannot-repro" + JSONL replay log
```

Loop max 3. No flakes: a repro only counts if it fails the same way 3×.

Optional demo flex: same prompt against self-hosted `qwen2.5-coder:7b` via Ollama on an RTX 4000 Ada GPU droplet ($0.76/hr), then destroy.

## Why open-weight AI is core

The model *is* the engine — generation, failure clustering, and the judge. No OpenAI API. Swap any DO-hosted open-weight model (Llama 4 Maverick, Ministral 3 14B, Gemma 4, DeepSeek V4) with one env var.

## Architecture ($30 DO budget)

- **$5 App Platform container** — TS harness + UI (harness = `KAG`)
- **DO Serverless Inference** — pay-per-token, ~$0.24/demo (`gpt-oss-120b`: $0.10 in / $0.70 out per 1M)
- **Functions free tier** — skill execution (90k GiB-s/mo free)
- **GPU only on stage** — RTX 4000 Ada ~$2–3 for a 3–4 hr demo, destroy same day

Worst case ≈ $13. Typical demo < $5.

## Repo layout (planned)

```
SKILL.md            # Agent Skill (Open Standard) — wraps KAG
src/harness.ts      # issue → generate → sandbox → verify → retry
src/tools.ts        # sandbox exec, trace parse
src/verify.ts       # determinism gate (3× same fail)
src/eval/           # 5 pass/fail fixtures
LICENSE             # MIT
```

~450 LOC TypeScript. No Python. No LangChain. No vector DB.

## Quickstart (planned)

```bash
npm i
export DO_INFERENCE_KEY=...
export DO_MODEL=qwen-3.5-397b
npm run kag -- --issue https://github.com/org/repo/issues/42
# → ./repros/42.repro.ts + ./runs/42.jsonl
```

## Status

Scaffold phase. README-first. Harness lands next.

## License

MIT — will be added with scaffold. Public release planned for challenge entry.
