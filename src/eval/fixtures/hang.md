# Hang: `serve()` never returns

Calling `serve()` hangs forever, no output, no exit.
Kill after 10s; expect `TIMEOUT` signature, mode `hang`.
Repro is `import { serve } from "./srv"; serve();`.
Must not be reported as crash or pass.
Second run must time out identically.
