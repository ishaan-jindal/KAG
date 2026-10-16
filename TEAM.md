# TEAM — KAG crew of 4

Ownership is by file. No two people edit the same file without a PR review.
Contracts below are frozen — if you need to change one, announce it first.

## Lane 1 — Sandbox + Gate

**Owns:** `src/tools.ts`, `src/verify.ts`

**Responsibilities:**
- FS-only `node --permission` exec locked to attempt dir (no `--allow-net` on Node 22, no child-process/worker/addons grants)
- Enforce 10s SIGKILL (never SIGTERM-only), `max-old-space-size=128`, minimal env, throwaway cwd
- Implement `normalizeStack` (strip `Node.js vX`, `0x…` pointers, abs paths → `basename:line:col`, timestamps) and `signature` (`TIMEOUT` | `SIG:<sig>` | `EXIT:<code>|<errClass>|<frames>` | `NO_FAIL`)
- Implement pure `gate`: 3× identical non-`NO_FAIL` → `deterministic` (`crash`/`hang`); 3× `NO_FAIL` → `no-failure`; mixed → `flaky`; 3× load errors → `invalid-candidate`
- Fingerprint stderr for `ERR_ACCESS_DENIED` — exit code alone is untrusted

**Provides to others:** `signature(run) → string`, `gate([sig, sig, sig]) → verdict`

**Done when:** inline thrower ×3 yields equal sigs with paths stripped; gate truth table passes; no LLM needed.

## Lane 2 — Loop + Eval

**Owns:** `src/harness.ts`, `src/eval/`

**Responsibilities:**
- DO Serverless Inference call: `POST https://inference.do-ai.run/v1/chat/completions`, Bearer `DO_INFERENCE_KEY`, body `model` (default `openai-gpt-oss-120b`), `messages`, `max_completion_tokens` (not `max_tokens`), `temperature: 0.2`, 120s abort
- 3 attempts × 3 runs retry loop with `describe(verdict)` feedback; `judge(prose)` display-only summary after `final`
- JSONL log (`generate` with full source / `run` / `verdict` / `final`) to `runs/<id>.jsonl`; artifacts to `repros/<id>.repro.ts`; CLI `--issue <url|file>` + `--replay` (diff stored sig vs rerun)
- Eval: 5 fixtures (`crash-deterministic`, `flaky-race`, `volatile-trace`, `hang`, `not-a-bug`) + `suite.json` (`verdict`, `mode`, `sigContains`, `maxAttempts`, `reproMaxLoc`, `replay`) + `run.ts` scorer (60/15/15/10) + `leaderboard.jsonl` telemetry
- Generate erasable-only TS (no `enum`/namespaces — type stripping has no build step)

**Consumes:** lane 1's `signature` + `gate`. **Provides:** `/run` payload shape lane 3 shells out to.

**Done when:** one real end-to-end run (~$0.01) + `npm run eval` green on hard contract + replay integrity holds.

## Lane 3 — UI + Deploy

**Owns:** `src/server.ts`, `Dockerfile`, `app.yaml`

**Responsibilities:**
- Stdlib-only `node:http` + SSE UI: `GET /` (form + `<pre>` stream + replay button) · `POST /run { issue, issueId? } → { id }` · `GET /events/:id` (tail JSONL as SSE) · `GET /repro/:id` · `GET /replay/:id`
- Same `LogEvent` schema as JSONL — one schema, two sinks; polling fallback noted if SSE gets fiddly
- Container: `node:24-slim`, `npm ci --omit=dev`, `CMD node src/server.ts`; `runs/` + `repros/` ephemeral (no Spaces — keeps always-on at $5)
- Deploy to $5 App Platform (1 shared vCPU / 512 MiB — the cap that justifies lane 1's memory limit); never touch harness logic, shell out to lane 2

**Done when:** browser shows generate → 3 runs → sig streaming live; deploy reachable by judges.

## Lane 4 — Skill + Demo + Entry

**Owns:** `SKILL.md`, `LICENSE`, `demo/`, README polish

**Responsibilities:**
- `SKILL.md` with fixed vocab: when-to-use / when-NOT (no security reports, no full-repo checkouts, no perf without a failing case), usage, inputs (`GITHUB_TOKEN` for issue API or raw file), output contract (exactly 2 artifacts, closed verdict set), honest-negative rule verbatim, cost + caps, one-line sandbox caveat
- MIT `LICENSE`; README quickstart / cost table / scorecard updates
- `demo/` 6-beat checklist: deterministic crash → minimal repro → replay → **flaky `cannot-repro` (never cut)** → GPU env-var swap → droplet destroy + billing receipt; record video/GIF for async judging
- Challenge entry: public repo, clean-clone run with only `DO_INFERENCE_KEY`/`DO_MODEL` set, open-weight only, sample `runs/*.jsonl` committed

**Done when:** entry checklist fully ticked and receipts show typical demo < $5.

## Shared contracts (frozen)

- Verdicts: `deterministic(crash|hang)` · `cannot-repro {no-failure|flaky|invalid-candidate}` — no other wording anywhere
- Env: `DO_INFERENCE_KEY` (model access key), `DO_MODEL` (default `openai-gpt-oss-120b`), `DO_INFERENCE_URL` override for GPU Ollama
- Costs: ~$0.01/attempt at gpt-oss-120b rates; Qwen `qwen3.5-397b-a17b` is 5–35× pricier per token

## Workflow

- Branches: `lane-1-sandbox`, `lane-2-harness`, `lane-3-ui`, `lane-4-skill`. PRs only, one reviewer, CI is `npm run eval` when it exists.
- Merge order: 1 → 2 → 3 → 4. Lanes 2–4 start immediately against the contracts above using stubs.
- Daily sync: 5 min on interface changes only. Anything else stays in the PR.
