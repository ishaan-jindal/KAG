# Demo Checklist & Runbook (~6 min)

> **Core Philosophy:** Minimal code, verifiable proof, and honest negatives. Never cut the flaky beat.

---

## Timing Breakdown & Checklist

- [ ] **Beat 1: Deterministic Crash** (0:00 – 1:30, 1.5 min) — Paste issue text → live SSE stream (generate + 3 runs + normalized sig)
- [ ] **Beat 2: Minimal Repro Inspection** (1:30 – 2:30, 1.0 min) — Inspect standalone `< 20 LOC` generated `repros/<id>.repro.ts`
- [ ] **Beat 3: Replay Verification** (2:30 – 3:00, 0.5 min) — Click **Replay** → diff against `runs/<id>.jsonl` → identical signature
- [ ] **Beat 4: Flaky Race (Trust Beat)** (3:00 – 4:30, 1.5 min) — Paste race issue → divergent runs → `cannot-repro {flaky}` (Never Cut)
- [ ] **Beat 5: GPU Droplet Swap Flex** (4:30 – 5:30, 1.0 min) — `export DO_INFERENCE_URL=...` → run on GPU Ollama with zero code changes
- [ ] **Beat 6: Teardown & Cost Receipt** (5:30 – 6:00, 0.5 min) — `doctl compute droplet delete` → show total session spend < $5

---

## 1. Deterministic Crash: SSE Stream (Beat 1)

### Input to Paste
Copy and paste the contents of [`src/eval/fixtures/crash-deterministic.md`](file:///c:/Users/aarya/KAG/src/eval/fixtures/crash-deterministic.md) into the web UI or pass via CLI:

```markdown
Calling load({}) throws TypeError: Cannot read properties of undefined.
Repro: import { load } from "./config"; load({});
Happens every run on Node 22, no env vars needed.
Expected: throw a named ConfigError instead.
Stack points at config.ts:12 reading .name.
```

### Action
- **Web UI:** Navigate to `http://localhost:8080`, paste snippet, click **Run**.
- **CLI Alternative:**
  ```bash
  npm run kag -- --issue src/eval/fixtures/crash-deterministic.md
  ```

### What Judges See
1. **Live Generation:** SSE streams the prompt completion from open-weight model (`openai-gpt-oss-120b`).
2. **Triplicate Execution:** Sandbox runs candidate 3× under `node --permission` (FS-only, throwaway cwd, 10s SIGKILL).
3. **Trace Normalization:** Path pointers and volatile memory addresses are stripped to yield stable signature:
   ```text
   EXIT:1|TypeError|load:config.ts:12
   ```
4. **Gate Evaluation:** 3× identical signatures trigger the frozen contract verdict:
   ```text
   verdict: deterministic(crash)
   ```

---

## 2. Minimal Repro Inspection (Beat 2)

### Action
Open the generated artifact in `repros/<id>.repro.ts`:
```bash
cat repros/*.repro.ts
```

### What Judges See
- **Under 20 Lines:** Self-contained, minimal reproduction script.
- **Erasable TypeScript:** No complex imports, no external npm packages, executable directly under Node 22+ with zero build step.
- **Deterministic Trigger:** Isolates the root cause directly without repo noise.

---

## 3. Replay: Zero-Drift Verification (Beat 3)

### Action
- **Web UI:** Click the **Replay** button below the output stream.
- **CLI Alternative:**
  ```bash
  npm run kag -- --replay <id>
  ```

### What Judges See
- The harness re-executes the stored candidate reproduction from `repros/<id>.repro.ts`.
- Computes new run signature and compares against the recorded signature in `runs/<id>.jsonl`.
- **Match Confirmed:** Identical exit codes and normalized frames. Proves the bug is 100% stable across time.

---

## 4. The Trust Beat: Flaky Race Detection (Beat 4)

> ⚠️ **CRITICAL: NEVER CUT THIS BEAT.** This demonstrates that KAG refuses to hallucinate passing repros.

### Input to Paste
Copy and paste the contents of [`src/eval/fixtures/flaky-race.md`](file:///c:/Users/aarya/KAG/src/eval/fixtures/flaky-race.md):

```markdown
queue.push(job) intermittently throws RangeError: buffer full.
Only fails under parallel load; single-run repro passes.
Suspected missing await between push and flush.
Fails ~1 in 3 runs with the same script.
Do not bless a single passing run as proof.
```

### What Judges See
1. The model generates a race condition candidate.
2. The gate executes the candidate 3 times:
   - Run 1: `NO_FAIL` (passed)
   - Run 2: `EXIT:1|RangeError|push:queue.ts:44`
   - Run 3: `NO_FAIL` (passed)
3. Mixed signatures trigger the **Honest-Negative Rule** and the frozen contract verdict:
   ```text
   verdict: cannot-repro {flaky}
   ```
4. **Takeaway:** KAG will never pretend a flaky or unreproducible bug is solved.

---

## 5. GPU Droplet Swap Flex (Beat 5)

Demonstrate open-weight flexibility by pointing KAG to a self-hosted GPU instance running Ollama with one environment variable change.

### Provision GPU Droplet (Stage Setup)
```bash
# Provision RTX 4000 Ada GPU droplet on DigitalOcean ($0.76/hr)
doctl compute droplet create kag-gpu \
  --region nyc1 \
  --size g-4000-ada-1gpu \
  --image gpu-stack \
  --wait
```

### Run Model on GPU
```bash
# On droplet: run open-weight code model
ollama run qwen2.5-coder:7b
```

### Zero-Code Env Swap
```bash
# Point local or App Platform harness to droplet Ollama endpoint
export DO_INFERENCE_URL="http://<DROPLET_PUBLIC_IP>:11434/v1"
export DO_MODEL="qwen2.5-coder:7b"

# Run harness
npm run kag -- --issue src/eval/fixtures/crash-deterministic.md
```

### What Judges See
- The exact same harness seamlessly executes using local GPU inference.
- Proves no proprietary LLM vendor lock-in; open-weight weights can run on DO serverless or bare-metal droplets.

---

## 6. Teardown & Cost Receipt (Beat 6)

### Teardown Command
```bash
# Destroy temporary GPU droplet immediately after demonstration
doctl compute droplet delete kag-gpu --force
```

### Billing & Budget Receipt

```text
======================================================
KAG Demonstration Budget Summary (DigitalOcean Budget)
======================================================
1. App Platform (Web UI + SSE tail):       $5.00 / month (fixed)
2. DO Serverless Inference (5 demo runs):  ~$0.05 (pay-per-token)
3. GPU Droplet Runtime (~15 min @ $0.76/h): ~$0.19 (ephemeral)
------------------------------------------------------
Total Demo Spend:                          ~$5.24 (< $6 total)
Per-Run Marginal Cost:                     ~$0.01 / attempt
======================================================
```

---

## Async Recording Recommendations

- **Resolution:** 1080p (1920x1080) at 60 FPS.
- **Layout:** Split screen with Terminal on left (large readable font, e.g., 16pt JetBrains Mono) and Web UI on right.
- **Audio:** Clear voiceover explaining the 6 beats; emphasize the honest-negative rule during Beat 4.
- **Artifacts:** Commit sample `runs/*.jsonl` so judges can inspect the raw event stream.
