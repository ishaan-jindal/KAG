# Demo checklist (~6 min)

- [ ] 1. Paste deterministic crash → SSE streams generate → 3 runs → sig
- [ ] 2. Open 20-line `repro.ts` — the minimal promise
- [ ] 3. Hit **replay** — identical sig
- [ ] 4. Paste flaky race → `cannot-repro (flaky)` — the trust beat, never cut
- [ ] 5. `export DO_INFERENCE_URL=http://<droplet>:11434/v1` → rerun on GPU Ollama
- [ ] 6. `doctl compute droplet delete` + cost receipt
