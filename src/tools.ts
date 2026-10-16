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
  let s = stderr.replace(/Node\.js v[^\s]*/g, "");
  s = s.replace(/0x[0-9a-fA-F]+/g, "0x…");
  s = s.replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?/g, "<ts>");
  s = s.replace(/(file:\/\/)?(\/[^\s():\"']+)/g, (m) => {
    const p = m.replace(/^file:\/\//, "");
    if (!p.startsWith("/")) return m;
    const mm = p.match(/^(.*\/)?([^\/]+?)((?::\d+)(?::\d+)?)?$/);
    if (!mm) return m;
    return (mm[2] ?? "") + (mm[3] ?? "");
  });
  return s
    .split("\n")
    .map((l) => l.replace(/[ \t]+/g, " ").trim())
    .filter((l) => l !== "" && !l.includes("node:internal"))
    .join("\n");
}

// First `/^(\w*Error)\b(\s*\[[A-Z_]+\])?/` else first non-empty line ≤80ch.
export function parseErrClass(stderr: string): string {
  const m = stderr.match(/^(\w*Error)\b(\s*\[[A-Z_]+\])?/m);
  if (m) return ((m[1] ?? "") + (m[2] ?? "")).trim() || "UNKNOWN";
  for (const line of stderr.split("\n")) {
    const t = line.trim();
    if (t) return t.slice(0, 80);
  }
  return "UNKNOWN";
}

// First 3 `at` frames as `basename[:fn]`, joined `+`.
export function parseFrames(stderr: string): string {
  const out: string[] = [];
  for (const line of stderr.split("\n")) {
    const t = line.trim();
    if (!t.startsWith("at ")) continue;
    if (t.includes("node:internal")) continue;
    const rest = t.slice(3).trim();
    let fn = "";
    let fileLoc = rest;
    const pm = rest.match(/^(.*)\(([^()]+)\)$/);
    if (pm) {
      fn = (pm[1] ?? "").trim().replace(/^async\s+/, "");
      fileLoc = (pm[2] ?? "").trim();
    }
    const base = (fileLoc.split("/").pop() ?? "").split(":")[0]?.replace(/^file:+/, "") ?? "";
    if (!base) continue;
    out.push(fn ? `${base}:${fn}` : base);
    if (out.length >= 3) break;
  }
  return out.join("+");
}

// TIMEOUT | SIG:<sig> | EXIT:<code>|<errClass>|<frames> | NO_FAIL
export function signature(run: RunResult): string {
  if (run.timedOut) return "TIMEOUT";
  if (run.signal) return `SIG:${run.signal}`;
  const err = normalizeStack(run.stderr);
  const frames = parseFrames(err);
  // Fingerprint permission denials in stderr — exit code alone is untrusted.
  if (err.includes("ERR_ACCESS_DENIED")) return `EXIT:${run.exitCode ?? "?"}|ERR_ACCESS_DENIED|${frames}`;
  if (run.exitCode === 0) {
    if (err === "" || !/(\w*Error|ERR_[A-Z_]+)/.test(err)) return "NO_FAIL";
    return `EXIT:0|${parseErrClass(err)}|${frames}`;
  }
  return `EXIT:${run.exitCode ?? "?"}|${parseErrClass(err)}|${frames}`;
}

const CAP = 256 * 1024;

// Throwaway cwd + minimal env + 10s SIGKILL (never SIGTERM-only) + 128 MiB cap.
export function runSandbox(entryFile: string, attemptDir: string): Promise<RunResult> {
  return new Promise((resolve) => {
    let settled = false;
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    // Own timer → SIGKILL (never SIGTERM-only); cleared on settle.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const done = (r: RunResult) => {
      if (settled) return;
      settled = true;
      if (timer !== undefined) clearTimeout(timer);
      resolve(r);
    };
    const cap = (cur: string, chunk: unknown): string =>
      cur.length >= CAP ? cur : cur + String(chunk).slice(0, CAP - cur.length);
    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(
        process.execPath,
        [
          "--permission",
          `--allow-fs-read=${attemptDir}`,
          `--allow-fs-read=${entryFile}`,
          `--allow-fs-write=${attemptDir}`,
          "--max-old-space-size=128",
          entryFile,
        ],
        { cwd: attemptDir, env: { PATH: process.env.PATH ?? "" } },
      );
    } catch (e) {
      done({ stdout, stderr: String(e), exitCode: null, signal: null, timedOut: false });
      return;
    }
    timer = setTimeout(() => {
      timedOut = true;
      try {
        child.kill("SIGKILL");
      } catch {
        // Already exited; close handler settles.
      }
    }, 10_000);
    if (child.stdout) child.stdout.on("data", (c) => {
      stdout = cap(stdout, c);
    });
    if (child.stderr) child.stderr.on("data", (c) => {
      stderr = cap(stderr, c);
    });
    child.on("error", (e) => {
      done({ stdout, stderr: stderr + String(e), exitCode: null, signal: null, timedOut: false });
    });
    child.on("close", (code, signal) => {
      done({ stdout, stderr, exitCode: code, signal: signal ?? null, timedOut });
    });
  });
}
