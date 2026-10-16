// Lane 1 — FS-only sandbox exec + signature. No network grant.
// Node 22 has no --allow-net: FS-only is enforced by simply never passing a
// net/child-process/worker/addons permission flag. `node --permission` denies
// everything not explicitly allowed.
import { spawn } from "node:child_process";

export type RunResult = {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  signal: string | null;
  timedOut: boolean;
};

// Strip machine-specific noise so equal failures hash equal.
export function normalizeStack(stderr: string): string {
  // TODO: strip `Node.js vX` banners, `0x…` pointers, abs paths → `basename:line:col`, timestamps.
  return stderr;
}

// TIMEOUT | SIG:<sig> | EXIT:<code>|<errClass>|<frames> | NO_FAIL
export function signature(run: RunResult): string {
  if (run.timedOut || run.signal === "SIGKILL") return "TIMEOUT";
  if (run.signal) return `SIG:${run.signal}`;
  const err = normalizeStack(run.stderr);
  // Fingerprint permission denials in stderr — exit code alone is untrusted.
  // TODO: fold ERR_ACCESS_DENIED into the EXIT signature as its errClass.
  if (err.includes("ERR_ACCESS_DENIED")) {
    // TODO: extract errClass + top frames, return EXIT form.
    return `EXIT:${run.exitCode ?? "?"}|ERR_ACCESS_DENIED|`;
  }
  // TODO: detect failure (non-zero exit / thrown error class + frames) vs success.
  if (run.exitCode === 0) return "NO_FAIL";
  // TODO: return `EXIT:<code>|<errClass>|<frames>`.
  return `EXIT:${run.exitCode ?? "?"}||`;
}

// Throwaway cwd + minimal env + 10s SIGKILL (never SIGTERM-only) + 128 MiB cap.
export function runSandbox(entryFile: string, attemptDir: string): Promise<RunResult> {
  // TODO: resolve attemptDir allowlists; minimal env (PATH/lang only); capture stdout/stderr caps.
  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [
        "--permission",
        // TODO: scope to attempt dir, e.g. `--allow-fs-read=${attemptDir}` (+ node_modules readout if needed).
        "--allow-fs-read=*",
        // TODO: scope writes to attempt dir only.
        "--allow-fs-write=*",
        "--max-old-space-size=128",
        entryFile,
      ],
      { timeout: 10_000, killSignal: "SIGKILL", cwd: attemptDir, env: {} },
    );
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (c) => {
      stdout += String(c);
    });
    child.stderr.on("data", (c) => {
      stderr += String(c);
    });
    child.on("close", (code, signal) => {
      resolve({ stdout, stderr, exitCode: code, signal, timedOut: signal === "SIGKILL" });
    });
  });
}
