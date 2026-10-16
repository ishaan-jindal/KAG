---
name: kag
description: Paste a GitHub issue, get back a minimal deterministic repro.ts or an honest cannot-repro.
---

# KAG Skill

## When to use

Use when you have a bug report (issue text or URL) and need proof: a minimal
`repro.ts` that fails the same way 3 times in a row.

## When NOT to use

- Security reports — do not run them through the harness.
- Full-repo checkouts — KAG takes one issue plus a candidate file, not a repo.
- Perf without a failing case — no crash/hang means no repro to gate.

## Usage

```bash
npm run kag -- --issue https://github.com/org/repo/issues/42
# → ./repros/42.repro.ts + ./runs/42.jsonl
# --replay re-runs stored sources and diffs sigs
```

Headless: `POST /run { issue }`. GPU swap:
`export DO_INFERENCE_URL=http://<droplet>:11434/v1`.

## Inputs

- Issue text directly, or an issue URL/file path via `--issue <url|file>`.
- `GITHUB_TOKEN` for the issue API, or pass a raw file to skip the API.

## Output contract

Exactly 2 artifacts per run: `repros/<id>.repro.ts` and `runs/<id>.jsonl`.
Closed verdict set only: `deterministic(crash|hang)` ·
`cannot-repro {no-failure|flaky|invalid-candidate}` — no other wording anywhere.

## Honest-negative rule

Never present a candidate that failed the 3× gate as a repro.

## Cost & caps

Max 3 attempts × 3 runs, ~$0.01/attempt at `openai-gpt-oss-120b` rates
($0.10 in / $0.70 out per 1M tokens).

## Sandbox caveat

Sandbox is FS-only `node --permission` for determinism, not containment:
network is prompt-hardened, not permission-isolated.
