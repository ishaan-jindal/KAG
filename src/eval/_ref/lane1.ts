// Lane 2 — LOCAL REFERENCE IMPL of lane 1's contract. THROWAWAY.
//
// Purpose: let `npm run eval` run green before lane 1 merges, without editing
// src/tools.ts (TEAM.md ownership rule). When lane 1 lands, delete this file and
// point src/eval/run.ts at the real `../../tools.ts`. Nothing in src/ outside
// src/eval/ imports this.
//
// Contract mirrored from src/tools.ts: RunResult, normalizeStack, signature,
// runSandbox.

import { spawn } from "node:child_process";
import { realpathSync } from "node:fs";

export type RunResult = {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  signal: string | null;
  timedOut: boolean;
};

// Machine-specific noise that must not affect equality of failures.
export function normalizeStack(stderr: string): string {
  return (
    stderr
      // banner + version
      .replace(/^\s*Node\.js v[\d.]+.*$/gm, "")
      // raw pointers
      .replace(/0x[0-9a-fA-F]+/g, "<ptr>")
      // file:// URL scheme, otherwise frames read `at load (file:///config.ts:1:1)`
      .replace(/file:\/\/\/?/g, "")
      // /a/b/c/parse.ts:7:15 -> parse.ts:7:15
      .replace(/(?:[A-Za-z0-9._@+-]+[\\/])+([A-Za-z0-9._-]+\.(?:ts|js|mjs|cjs))(:\d+:\d+)/g, "$1$2")
      // ISO timestamps embedded in error messages
      .replace(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?/g, "<ts>")
      // bare absolute dirs
      .replace(/(?:[A-Za-z0-9._@+-]+[\\/]){2,}/g, "")
      .replace(/\n{3,}/g, "\n\n")
      .trim()
  );
}

function errorClass(normalized: string): string {
  // Node prints load errors as `code: 'ERR_MODULE_NOT_FOUND'` inside the error
  // dump, not as the class name. verify.ts's isLoadError greps the *signature*
  // for that code, so signature() must carry it through or every module
  // resolution failure grades as a generic crash instead of invalid-candidate.
  const code = normalized.match(/code:\s*'(ERR_[A-Z_]+)'/)?.[1];
  if (code) return code;
  return normalized.match(/\b(\w*(?:Error|Exception))\b/)?.[1] ?? "";
}

function frames(normalized: string): string {
  return normalized
    .split("\n")
    .filter((l) => /^\s*at\s/.test(l))
    .slice(0, 3)
    .map((l) => l.trim())
    .join(">");
}

// TIMEOUT | SIG:<sig> | EXIT:<code>|<errClass>|<frames> | NO_FAIL
// Message text is deliberately excluded: only class + frames decide identity.
export function signature(run: RunResult): string {
  if (run.timedOut) return "TIMEOUT";
  if (run.signal) return `SIG:${run.signal}`;

  const err = normalizeStack(run.stderr);
  const framesPart = frames(err);

  if (err.includes("ERR_ACCESS_DENIED")) return `EXIT:${run.exitCode ?? "?"}|ERR_ACCESS_DENIED|${framesPart}`;

  const isLoadError =
    /ERR_MODULE_NOT_FOUND|SyntaxError|Cannot find module|ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX/.test(err);
  const thrown = /^\s*at\s/m.test(err) || Boolean(errorClass(err));
  const failed = run.exitCode !== 0 || thrown;

  if (!failed) return "NO_FAIL";
  return `EXIT:${run.exitCode ?? "?"}|${errorClass(err) || "?"}|${framesPart}`;
}

const OUT_CAP = 64 * 1024;

export function runSandbox(entryFile: string, attemptDir: string): Promise<RunResult> {
  // Node's loader realpath()s the entry before the permission check, so the
  // allowlist must contain the RESOLVED path. On macOS /var/folders -> /private
  // and /tmp -> /private/tmp, so the un-resolved path yields ERR_ACCESS_DENIED
  // in fs.realpathSync before any user code runs.
  const dir = realpathSync(attemptDir);
  const entry = realpathSync(entryFile);

  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [
        "--permission",
        `--allow-fs-read=${dir}`,
        `--allow-fs-write=${dir}`,
        "--max-old-space-size=128",
        entry,
      ],
      {
        timeout: 10_000,
        killSignal: "SIGKILL",
        cwd: dir,
        env: { PATH: process.env.PATH ?? "", LANG: "C" },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let stdout = "";
    let stderr = "";
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, 10_000);

    child.stdout.on("data", (c: Buffer) => {
      if (stdout.length < OUT_CAP) stdout += String(c);
    });
    child.stderr.on("data", (c: Buffer) => {
      if (stderr.length < OUT_CAP) stderr += String(c);
    });
    child.on("error", (err) => {
      clearTimeout(timer);
      resolve({ stdout, stderr: String(err), exitCode: null, signal: null, timedOut: false });
    });
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      resolve({ stdout, stderr, exitCode: code, signal, timedOut: timedOut || signal === "SIGKILL" });
    });
  });
}