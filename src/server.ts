// Lane 3 — UI + Deploy.
//
// Stdlib-only `node:http` server: run orchestration (spawns the lane 2 harness
// detached), a live SSE tail of `runs/<id>.jsonl`, a polling fallback, artifact
// endpoints, and one self-contained HTML page.
//
// One schema, two sinks: the JSONL events written by the harness are relayed
// VERBATIM to SSE clients and parsed 1:1 by /status. This server never
// fabricates or reshapes log events — a missing log is an honest 404, never a
// fake verdict.
import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, rmSync } from "node:fs";
import {
  mkdir,
  open,
  readFile,
  readdir,
  stat,
  unlink,
  writeFile,
  type FileHandle,
} from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { join } from "node:path";

// ---------------------------------------------------------------------------
// Config

const PORT = Number(process.env.PORT ?? 8080);
const RUNS_DIR = "runs";
const REPROS_DIR = "repros";
const FIXTURES_DIR = join("src", "eval", "fixtures");
const HARNESS_SCRIPT = process.env.KAG_HARNESS_SCRIPT ?? "src/harness.ts";

/** Every id that touches the filesystem must match this. */
const ID_RE = /^[a-z0-9-]{1,32}$/;
const FIXTURE_RE = /^[a-z-]+$/;

const MAX_BODY_BYTES = 100 * 1024;
const MAX_CONCURRENT_RUNS = 2;
const PRUNE_MAX_AGE_MS = 48 * 60 * 60 * 1000;

const SSE_TICK_MS = 250;
const SSE_PING_MS = 15_000;
// Worst case per run: 30s URL fetch + 3 attempts × (120s inference + 3× 10s
// sandbox) + 120s judge ≈ 11 min. The stream must outlive the run it tails.
const SSE_DEADLINE_MS = 15 * 60 * 1000;
// A URL fetch (30s) plus inference (120s) can precede the first log line by
// minutes. While waiting, the stream is open with heartbeats (see handleEvents).
const FIRST_EVENT_WAIT_S = 150;
const FIRST_EVENT_WAIT_MS = FIRST_EVENT_WAIT_S * 1000;
const SSE_MAX_CHUNK = 8 * 1024 * 1024;
const SSE_TAIL_BYTES = 64 * 1024;

const REPLAY_TIMEOUT_MS = 60_000;
const REPLAY_OUTPUT_CAP = 1024 * 1024;

// ---------------------------------------------------------------------------
// Small helpers

function json(res: ServerResponse, status: number, body: unknown): void {
  if (res.destroyed) return;
  if (res.headersSent) {
    try {
      res.end();
    } catch {
      /* socket already gone */
    }
    return;
  }
  const text = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(text),
  });
  res.end(text);
}

function sendText(res: ServerResponse, status: number, body: string): void {
  res.writeHead(status, {
    "content-type": "text/plain; charset=utf-8",
    "content-length": Buffer.byteLength(body),
  });
  res.end(body);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Async handlers go through this so a rejection becomes a clean 500. */
function settle(work: Promise<void>, res: ServerResponse): void {
  work.catch((err) => {
    console.error("request handler error:", err);
    if (!res.headersSent) json(res, 500, { error: "internal error" });
    else {
      try {
        res.end();
      } catch {
        /* socket already gone */
      }
    }
  });
}

type BodyRead = { ok: true; body: string } | { ok: false; status: number; error: string };

function readBody(req: IncomingMessage, maxBytes: number): Promise<BodyRead> {
  return new Promise((resolve) => {
    let size = 0;
    let tooBig = false;
    let settled = false;
    const chunks: Buffer[] = [];
    const finish = (result: BodyRead): void => {
      if (!settled) {
        settled = true;
        resolve(result);
      }
    };
    req.on("data", (chunk: Buffer) => {
      if (tooBig) return;
      size += chunk.length;
      if (size > maxBytes) {
        tooBig = true;
        chunks.length = 0; // stop buffering, keep draining
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (tooBig) finish({ ok: false, status: 413, error: `request body exceeds ${maxBytes} bytes` });
      else finish({ ok: true, body: Buffer.concat(chunks).toString("utf8") });
    });
    req.on("error", () => finish({ ok: false, status: 400, error: "request stream error" }));
    req.on("aborted", () => finish({ ok: false, status: 400, error: "request aborted" }));
  });
}

/** Parse one JSONL line; null when it is not a JSON object. */
function parseEventLine(line: string): Record<string, unknown> | null {
  const trimmed = line.trim();
  if (!trimmed) return null;
  try {
    const parsed: unknown = JSON.parse(trimmed);
    if (parsed !== null && typeof parsed === "object") return parsed as Record<string, unknown>;
    return { type: "raw", line: trimmed };
  } catch {
    return { type: "raw", line: trimmed };
  }
}

function isFinalLine(line: string): boolean {
  const ev = parseEventLine(line);
  return ev !== null && ev.type === "final";
}

async function readTail(file: string, maxBytes: number): Promise<string> {
  const fh = await open(file, "r");
  try {
    const st = await fh.stat();
    const len = Math.min(st.size, maxBytes);
    const buf = Buffer.alloc(len);
    if (len > 0) await fh.read(buf, 0, len, st.size - len);
    return buf.toString("utf8");
  } finally {
    await fh.close().catch(() => undefined);
  }
}

// ---------------------------------------------------------------------------
// Active run tracking + shutdown

type ActiveRun = { child: ChildProcess; pid: number; startedAt: number; firstLog?: NodeJS.Timeout };
const activeRuns = new Map<string, ActiveRun>();
let shuttingDown = false;

/**
 * Spawn failures that surface after POST /run already answered 200
 * (spawn errors are async — 'error', not a throw). Kept briefly so
 * /events and /status can answer honestly instead of 404ing.
 */
const deadRuns = new Map<string, { error: string; at: number }>();
const DEAD_RUN_TTL_MS = 10 * 60 * 1000;

function deadRunError(id: string): string | null {
  const d = deadRuns.get(id);
  if (!d) return null;
  if (Date.now() - d.at > DEAD_RUN_TTL_MS) {
    deadRuns.delete(id);
    return null;
  }
  return d.error;
}

/** Pids of in-flight /replay children (own process groups). Swept on shutdown. */
const replayPids = new Set<number>();

function killTree(pid: number, signal: NodeJS.Signals): void {
  if (pid <= 0) return;
  try {
    // detached child is its own process-group leader; this also reaps its sandbox children
    process.kill(-pid, signal);
  } catch {
    try {
      process.kill(pid, signal);
    } catch {
      /* already gone */
    }
  }
}

function shutdown(signal: NodeJS.Signals): void {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`kag ui: received ${signal}, stopping ${activeRuns.size} active run(s)`);
  for (const run of activeRuns.values()) killTree(run.pid, "SIGTERM");
  for (const pid of replayPids) killTree(pid, "SIGTERM");
  const force = setTimeout(() => {
    for (const run of activeRuns.values()) killTree(run.pid, "SIGKILL");
    for (const pid of replayPids) killTree(pid, "SIGKILL");
    process.exit(0);
  }, 2000);
  force.unref();
  if (activeRuns.size === 0) process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// ---------------------------------------------------------------------------
// POST /run { issue, issueId? } → { id }

type RunBody = { issue?: unknown; issueId?: unknown };

async function handlePostRun(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const read = await readBody(req, MAX_BODY_BYTES);
  if (!read.ok) {
    json(res, read.status, { error: read.error });
    return;
  }
  let raw: unknown;
  try {
    raw = JSON.parse(read.body);
  } catch {
    json(res, 400, { error: "request body must be JSON" });
    return;
  }
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    json(res, 400, { error: "request body must be a JSON object" });
    return;
  }
  const body = raw as RunBody;
  if (typeof body.issue !== "string" || body.issue.trim() === "") {
    json(res, 400, { error: "issue (non-empty string) required" });
    return;
  }

  let id: string;
  if (body.issueId === undefined) {
    id = randomUUID().slice(0, 12);
  } else if (typeof body.issueId === "string" && ID_RE.test(body.issueId)) {
    id = body.issueId;
  } else {
    json(res, 400, { error: "issueId must match ^[a-z0-9-]{1,32}$" });
    return;
  }
  if (activeRuns.has(id)) {
    json(res, 409, { error: `run ${id} is already active` });
    return;
  }
  if (activeRuns.size >= MAX_CONCURRENT_RUNS) {
    json(res, 429, { error: `too many active runs (max ${MAX_CONCURRENT_RUNS}); retry shortly` });
    return;
  }

  // Stage the input: URLs pass through, pasted text lands in runs/<id>.issue.txt.
  const trimmed = body.issue.trim();
  let issueArg = trimmed;
  if (!/^https?:\/\//i.test(trimmed)) {
    await mkdir(RUNS_DIR, { recursive: true });
    issueArg = join(RUNS_DIR, `${id}.issue.txt`);
    await writeFile(issueArg, body.issue, "utf8");
  }

  // Re-check synchronously just before spawn. There is no await between this
  // re-check, spawn(), and activeRuns.set() below, so on Node's event loop the
  // reservation is atomic: the 2-run cap and the 409 duplicate guard both hold
  // under concurrent POSTs. (The earlier pre-staging check is only a fast path.)
  if (activeRuns.has(id)) {
    json(res, 409, { error: `run ${id} is already active` });
    return;
  }
  if (activeRuns.size >= MAX_CONCURRENT_RUNS) {
    json(res, 429, { error: `too many active runs (max ${MAX_CONCURRENT_RUNS}); retry shortly` });
    return;
  }

  if (!existsSync(HARNESS_SCRIPT)) {
    json(res, 500, { error: `harness script not found: ${HARNESS_SCRIPT}` });
    return;
  }
  // Fresh slate for this id: a reused explicit issueId must not append to (or
  // replay stale events from) a previous run's log. Sync — preserves the
  // atomic reservation window above.
  rmSync(join(RUNS_DIR, `${id}.jsonl`), { force: true });
  deadRuns.delete(id);

  let child: ChildProcess;
  try {
    child = spawn(process.execPath, [HARNESS_SCRIPT, "--issue", issueArg, "--id", id], {
      detached: true,
      stdio: ["ignore", "inherit", "inherit"],
    });
  } catch (err) {
    console.error(`[run ${id}] failed to spawn harness:`, err);
    json(res, 500, { error: "failed to spawn harness" });
    return;
  }

  activeRuns.set(id, { child, pid: child.pid ?? -1, startedAt: Date.now() });
  child.once("error", (err) => {
    const rec = activeRuns.get(id);
    if (rec?.firstLog) clearInterval(rec.firstLog);
    activeRuns.delete(id);
    // Spawn failed after we already answered 200: leave a tombstone so
    // /events and /status report the real cause instead of a bare 404.
    deadRuns.set(id, { error: err.message, at: Date.now() });
    console.error(`[run ${id}] harness spawn error: ${err.message}`);
  });
  child.on("exit", (code, signal) => {
    const rec = activeRuns.get(id);
    if (rec?.firstLog) clearInterval(rec.firstLog);
    activeRuns.delete(id);
    console.log(`[run ${id}] harness exited (code=${code}, signal=${signal})`);
    if (shuttingDown && activeRuns.size === 0) process.exit(0);
  });
  child.unref();

  const via = /^https?:\/\//i.test(trimmed) ? "url" : "pasted " + body.issue.length + " chars";
  console.log(`[run ${id}] started via ${via} (pid=${child.pid ?? "?"})`);
  // The harness writes its first log line only after the issue fetch plus the
  // first inference round-trip — minutes on CPU. Surface that moment so the
  // terminal shows progress instead of silence.
  const probe = setInterval(() => {
    if (shuttingDown || !activeRuns.has(id)) {
      clearInterval(probe);
      return;
    }
    if (existsSync(join(RUNS_DIR, `${id}.jsonl`))) {
      clearInterval(probe);
      const rec = activeRuns.get(id);
      if (rec) rec.firstLog = undefined;
      console.log(`[run ${id}] first log line (inference done, sandbox running)`);
    }
  }, 1000);
  probe.unref();
  const started = activeRuns.get(id);
  if (started) started.firstLog = probe;

  json(res, 200, { id });
}

// ---------------------------------------------------------------------------
// GET /events/:id — SSE tail of runs/<id>.jsonl (verbatim lines)

async function handleEvents(req: IncomingMessage, res: ServerResponse, id: string): Promise<void> {
  if (!ID_RE.test(id)) {
    json(res, 400, { error: "invalid run id" });
    return;
  }
  const file = join(RUNS_DIR, `${id}.jsonl`);

  let fh: FileHandle | null = null;
  try {
    fh = await open(file, "r");
  } catch {
    fh = null;
  }
  // The tail below opens the stream as soon as headers go out; remember if the
  // pre-stream wait already opened them so writeHead runs exactly once.
  let headersOpen = false;
  if (fh === null) {
    const dead = deadRunError(id);
    if (dead) {
      json(res, 500, { error: `harness failed to start for id ${id}: ${dead}` });
      return;
    }
    if (!activeRuns.has(id)) {
      // Honest 404 — never a fabricated verdict.
      json(res, 404, { error: `no run log for id ${id}` });
      return;
    }
    // Active run whose harness is still thinking: it writes its first line only
    // after fetching the issue and finishing inference, minutes in the worst
    // case. Open the stream now and heartbeat while waiting, so neither the
    // client nor an intermediary kills an idle connection.
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    });
    res.write(": connected\n\n");
    headersOpen = true;
    const waitPing = setInterval(() => {
      try {
        res.write(": ping\n\n");
      } catch {
        /* socket gone; the wait loop exits on req.destroyed */
      }
    }, SSE_PING_MS);
    try {
      const waitUntil = Date.now() + FIRST_EVENT_WAIT_MS;
      let waitDead: string | null = null;
      while (fh === null && Date.now() < waitUntil && !req.destroyed) {
        waitDead = deadRunError(id);
        if (waitDead) break;
        await sleep(SSE_TICK_MS);
        try {
          fh = await open(file, "r");
        } catch {
          fh = null;
        }
      }
      if (fh === null && !req.destroyed) {
        // The stream is already open, so a 404 is impossible: answer with a
        // terminal transport-level event instead (outside the verdict vocabulary).
        const msg = deadRunError(id) ?? waitDead ?? `harness produced no log for id ${id} within ${FIRST_EVENT_WAIT_S}s`;
        try {
          res.write(`data: ${JSON.stringify({ type: "error", error: msg })}\n\n`);
        } catch {
          /* socket already gone */
        }
        try {
          res.end();
        } catch {
          /* socket already gone */
        }
      }
    } finally {
      clearInterval(waitPing);
    }
    if (fh === null || req.destroyed) return;
  }

  const lastEventId = req.headers["last-event-id"];
  let resumePending: number | null = null;
  if (typeof lastEventId === "string" && /^\d{1,15}$/.test(lastEventId)) {
    resumePending = Number(lastEventId);
  }

  if (!headersOpen) {
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    });
    res.write(": connected\n\n");
  }

  let closed = false;
  let terminal = false; // a `final` event went through; close after it
  let readOffset = 0;
  let pendingOffset = 0; // file byte offset of the first byte in `carry`
  let carry = Buffer.alloc(0);
  let tickTimer: NodeJS.Timeout | null = null;
  let pingTimer: NodeJS.Timeout | null = null;
  const deadline = Date.now() + SSE_DEADLINE_MS;

  const cleanup = (): void => {
    if (closed) return;
    closed = true;
    if (tickTimer) {
      clearTimeout(tickTimer);
      tickTimer = null;
    }
    if (pingTimer) {
      clearInterval(pingTimer);
      pingTimer = null;
    }
    if (fh) {
      fh.close().catch(() => undefined);
      fh = null;
    }
  };

  const endStream = (): void => {
    if (!closed) {
      cleanup();
      try {
        res.end();
      } catch {
        /* socket already gone */
      }
    }
  };

  const relay = (line: string, offset: number): void => {
    if (!line.trim()) return;
    try {
      res.write(`id: ${offset}\ndata: ${line}\n\n`);
    } catch {
      endStream();
      return;
    }
    if (isFinalLine(line)) terminal = true;
  };

  const tick = async (): Promise<void> => {
    if (closed) return;
    if (Date.now() > deadline) {
      try {
        res.write(
          `data: ${JSON.stringify({
            type: "error",
            error: `SSE deadline exceeded after 15 minutes; reopen /events/${id} or poll /status/${id}`,
          })}\n\n`,
        );
      } catch {
        /* socket already gone */
      }
      endStream();
      return;
    }
    try {
      if (!fh) {
        endStream();
        return;
      }
      const st = await fh.stat();

      if (resumePending !== null) {
        let start = resumePending;
        if (!Number.isFinite(start) || start < 0 || start > st.size) start = 0;
        resumePending = null;
        readOffset = start;
        pendingOffset = start;
        carry = Buffer.alloc(0);
        if (start >= st.size && st.size > 0) {
          // Resuming at EOF: if the log already ends in `final`, nothing more is coming.
          const tail = await readTail(file, SSE_TAIL_BYTES);
          const lines = tail.split("\n");
          for (let i = lines.length - 1; i >= 0; i--) {
            if (lines[i].trim()) {
              if (isFinalLine(lines[i])) endStream();
              break;
            }
          }
          if (closed) return;
        }
      }

      if (st.size < pendingOffset) {
        // Log was truncated or replaced — start over from byte zero.
        readOffset = 0;
        pendingOffset = 0;
        carry = Buffer.alloc(0);
      }

      if (st.size > readOffset) {
        const len = Math.min(st.size - readOffset, SSE_MAX_CHUNK);
        const buf = Buffer.alloc(len);
        const { bytesRead } = await fh.read(buf, 0, len, readOffset);
        readOffset += bytesRead;
        const chunk = buf.subarray(0, bytesRead);
        const data = carry.length > 0 ? Buffer.concat([carry, chunk]) : chunk;
        let start = 0;
        let nl = -1;
        while ((nl = data.indexOf(0x0a, start)) !== -1) {
          const line = data.subarray(start, nl).toString("utf8");
          const after = pendingOffset + nl + 1;
          start = nl + 1;
          relay(line, after);
          if (closed) return;
          if (terminal) {
            endStream();
            return;
          }
        }
        carry = Buffer.from(data.subarray(start));
        pendingOffset += start;
      }
    } catch (err) {
      console.error(`[events ${id}] tail error:`, err);
    }
    if (!closed && !terminal) {
      tickTimer = setTimeout(() => void tick(), SSE_TICK_MS);
    }
  };

  pingTimer = setInterval(() => {
    if (closed) return;
    try {
      res.write(": ping\n\n");
    } catch {
      cleanup();
    }
  }, SSE_PING_MS);

  req.on("close", cleanup);
  res.on("close", cleanup);

  void tick();
}

// ---------------------------------------------------------------------------
// GET /status/:id — polling fallback { events, done }

async function handleStatus(res: ServerResponse, id: string): Promise<void> {
  if (!ID_RE.test(id)) {
    json(res, 400, { error: "invalid run id" });
    return;
  }
  const dead = deadRunError(id);
  if (dead) {
    json(res, 500, { error: `harness failed to start for id ${id}: ${dead}` });
    return;
  }
  let text: string;
  try {
    text = await readFile(join(RUNS_DIR, `${id}.jsonl`), "utf8");
  } catch {
    json(res, 404, { error: `no run log for id ${id}` });
    return;
  }
  const events: unknown[] = [];
  let done = false;
  for (const rawLine of text.split("\n")) {
    if (!rawLine.trim()) continue;
    const ev = parseEventLine(rawLine);
    if (ev === null) continue;
    events.push(ev);
    if (ev.type === "final") done = true;
  }
  json(res, 200, { events, done });
}

// ---------------------------------------------------------------------------
// Artifacts

async function handleRepro(res: ServerResponse, id: string): Promise<void> {
  if (!ID_RE.test(id)) {
    json(res, 400, { error: "invalid run id" });
    return;
  }
  try {
    sendText(res, 200, await readFile(join(REPROS_DIR, `${id}.repro.ts`), "utf8"));
  } catch {
    json(res, 404, { error: `no repro for id ${id}` });
  }
}

async function handleLog(res: ServerResponse, id: string): Promise<void> {
  if (!ID_RE.test(id)) {
    json(res, 400, { error: "invalid run id" });
    return;
  }
  try {
    sendText(res, 200, await readFile(join(RUNS_DIR, `${id}.jsonl`), "utf8"));
  } catch {
    json(res, 404, { error: `no log for id ${id}` });
  }
}

type ReplayResult = { output: string; exitCode: number | null; stderr?: string; error?: string };

function runReplay(id: string): { done: Promise<ReplayResult>; cancel: () => void } {
  let child: ChildProcess | undefined;
  let cancel: () => void = () => undefined;
  const done = new Promise<ReplayResult>((resolve) => {
    child = spawn(process.execPath, [HARNESS_SCRIPT, "--replay", id], {
      // Own process group so a timeout kill reaps in-flight sandbox grandchildren too.
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    if (child.pid !== undefined) replayPids.add(child.pid);
    let out = "";
    let err = "";
    child.stdout?.on("data", (chunk: Buffer) => {
      if (out.length < REPLAY_OUTPUT_CAP) out += chunk.toString("utf8");
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      if (err.length < REPLAY_OUTPUT_CAP) err += chunk.toString("utf8");
    });
    let settled = false;
    const finish = (result: ReplayResult): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (child?.pid !== undefined) replayPids.delete(child.pid);
      resolve(result);
    };
    const timer = setTimeout(() => {
      if (child?.pid !== undefined) killTree(child.pid, "SIGKILL");
      finish({ output: out, exitCode: null, stderr: err || undefined, error: "replay timed out after 60s" });
    }, REPLAY_TIMEOUT_MS);
    cancel = () => {
      if (child?.pid !== undefined) killTree(child.pid, "SIGKILL");
      finish({ output: out, exitCode: null, stderr: err || undefined, error: "replay aborted (client disconnected)" });
    };
    child.on("error", (e) => finish({ output: out, exitCode: -1, stderr: err || undefined, error: e.message }));
    child.on("close", (code) => finish({ output: out, exitCode: code, stderr: err || undefined }));
  });
  return { done, cancel };
}

async function handleReplay(req: IncomingMessage, res: ServerResponse, id: string): Promise<void> {
  if (!ID_RE.test(id)) {
    json(res, 400, { error: "invalid run id" });
    return;
  }
  const { done, cancel } = runReplay(id);
  // A client that goes away must not leave the replay child running its 60s.
  res.on("close", () => {
    if (!res.writableEnded) cancel();
  });
  json(res, 200, await done);
}

async function handleFixture(res: ServerResponse, name: string): Promise<void> {
  if (!FIXTURE_RE.test(name)) {
    json(res, 404, { error: `unknown fixture ${name}` });
    return;
  }
  let allowed: string[];
  try {
    const entries = await readdir(FIXTURES_DIR);
    allowed = entries.filter((f) => f.endsWith(".md")).map((f) => f.slice(0, -3));
  } catch {
    json(res, 404, { error: "fixtures unavailable" });
    return;
  }
  if (!allowed.includes(name)) {
    json(res, 404, { error: `unknown fixture ${name}` });
    return;
  }
  try {
    sendText(res, 200, await readFile(join(FIXTURES_DIR, `${name}.md`), "utf8"));
  } catch {
    json(res, 404, { error: `unknown fixture ${name}` });
  }
}

// ---------------------------------------------------------------------------
// GET / — one self-contained page (no external assets)

function handleRoot(res: ServerResponse): void {
  const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>KAG — issue → minimal repro</title>
<style>
  :root {
    --ink: #381631;
    --punch: #e21c61;
    --warm: #e26159;
    --sun: #fea85f;
    --paper: #d8dcb4;
    --foam: #5eb6ad;
    --teal: #1b958d;
    --deep: #105390;
    --display: "Arial Black", "Franklin Gothic Heavy", Impact, Haettenschweiler, "Arial Narrow Bold", system-ui, sans-serif;
    --body: system-ui, -apple-system, "Segoe UI", Verdana, Geneva, Tahoma, sans-serif;
    --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
    color-scheme: light;
  }
  * { box-sizing: border-box; }
  html { -webkit-text-size-adjust: 100%; }
  body {
    margin: 0;
    background-color: var(--paper);
    background-image: repeating-radial-gradient(circle at 10px 10px, rgba(56,22,49,.16) 0, rgba(56,22,49,.16) 1.6px, transparent 1.6px, transparent 12px);
    color: var(--ink);
    font: 16px/1.55 var(--body);
    overflow-x: clip;
  }
  main { max-width: 920px; margin: 0 auto; padding: 28px 18px 72px; min-width: 0; }
  :focus-visible { outline: 3px solid #e21c61; outline-offset: 2px; border-radius: 2px; }

  /* ---- hero ---- */
  .hero {
    background: var(--ink);
    color: var(--paper);
    border: 3px solid var(--ink);
    box-shadow: 8px 8px 0 var(--punch);
    padding: 22px 22px 20px;
    margin-bottom: 22px;
    position: relative;
    overflow: clip;
  }
  .hero::after {
    content: "";
    position: absolute;
    inset: auto -30px -46px auto;
    width: 190px;
    height: 190px;
    background-image: repeating-radial-gradient(circle at 6px 6px, rgba(254,168,95,.55) 0, rgba(254,168,95,.55) 2.2px, transparent 2.2px, transparent 11px);
    transform: rotate(12deg);
    pointer-events: none;
  }
  .stickers { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 14px; position: relative; z-index: 1; }
  .sticker {
    font-family: var(--display);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .08em;
    border: 3px solid var(--ink);
    padding: 4px 12px;
    background: var(--punch);
    color: var(--paper);
    transform: rotate(-2deg);
    box-shadow: 3px 3px 0 rgba(216,220,180,.85);
  }
  .sticker.s2 { background: var(--foam); color: var(--ink); transform: rotate(1.6deg); }
  h1 {
    font-family: var(--display);
    font-size: clamp(30px, 5.4vw, 48px);
    line-height: 1.02;
    text-transform: uppercase;
    letter-spacing: .01em;
    margin: 0 0 10px;
    position: relative;
    z-index: 1;
  }
  h1 .hl {
    display: inline-block;
    background: var(--sun);
    color: var(--ink);
    padding: 0 10px;
    border: 3px solid var(--paper);
    transform: rotate(-1deg);
  }
  .sub { color: var(--paper); opacity: .92; margin: 0; max-width: 62ch; position: relative; z-index: 1; }
  .steps { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; position: relative; z-index: 1; }
  .steps span {
    font-family: var(--display);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .08em;
    border: 2px solid var(--paper);
    padding: 3px 10px;
    color: var(--paper);
  }

  /* ---- cards ---- */
  .card {
    background: var(--paper);
    border: 3px solid var(--ink);
    box-shadow: 6px 6px 0 var(--ink);
    padding: 18px;
    margin: 0 0 20px;
  }
  .kicker {
    display: inline-block;
    font-family: var(--display);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .08em;
    background: var(--ink);
    color: var(--paper);
    padding: 5px 12px;
    transform: rotate(-1deg);
    margin-bottom: 12px;
  }
  .kicker.sun { background: var(--sun); color: var(--ink); border: 3px solid var(--ink); transform: rotate(1deg); }
  label.fld {
    display: inline-block;
    font-family: var(--display);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .08em;
    margin-bottom: 8px;
  }
  textarea {
    width: 100%;
    max-width: 100%;
    min-height: 140px;
    background: var(--ink);
    color: var(--paper);
    border: 3px solid var(--ink);
    box-shadow: 4px 4px 0 var(--teal);
    padding: 12px;
    font-family: var(--mono);
    font-size: 14px;
    line-height: 1.5;
    resize: vertical;
    display: block;
  }
  textarea::placeholder { color: var(--foam); opacity: 1; }
  .btns { margin: 14px 0 2px; display: flex; flex-wrap: wrap; gap: 10px; }
  button {
    font-family: var(--display);
    font-size: 13px;
    text-transform: uppercase;
    letter-spacing: .06em;
    background: var(--sun);
    color: var(--ink);
    border: 3px solid var(--ink);
    box-shadow: 4px 4px 0 var(--ink);
    padding: 10px 18px;
    cursor: pointer;
  }
  button:hover:not(:disabled) { transform: translate(-1px, -1px); box-shadow: 5px 5px 0 var(--ink); }
  button:active:not(:disabled) { transform: translate(3px, 3px); box-shadow: 1px 1px 0 var(--ink); }
  button:disabled { opacity: .55; cursor: default; transform: none; }
  button.primary { background: var(--punch); color: var(--paper); }
  #copyRepro { background: var(--teal); color: var(--paper); }
  #toggleLog { background: var(--paper); }
  #formmsg { min-height: 1.4em; margin: 10px 0 0; font-weight: 700; color: var(--deep); overflow-wrap: anywhere; }
  #runstat {
    display: inline-block;
    min-height: 1.4em;
    margin: 6px 0 0;
    font-family: var(--mono);
    font-size: 13px;
    font-weight: 700;
    background: var(--sun);
    border: 3px solid var(--ink);
    box-shadow: 3px 3px 0 var(--ink);
    padding: 3px 10px;
    overflow-wrap: anywhere;
  }
  #runstat:empty { display: none; }

  /* ---- verdict banner ---- */
  #banner {
    display: none;
    padding: 14px 16px;
    margin: 20px 0;
    font-family: var(--display);
    font-size: 15px;
    text-transform: uppercase;
    letter-spacing: .03em;
    background: var(--paper);
    border: 3px solid var(--ink);
    box-shadow: 6px 6px 0 var(--ink);
    transform: rotate(-.4deg);
    overflow-wrap: anywhere;
  }
  #banner.det { background: var(--foam); color: var(--ink); }
  #banner.cant { background: var(--sun); color: var(--ink); }
  #banner.err { background: var(--punch); color: var(--paper); }

  /* ---- timeline ---- */
  #timeline { margin: 14px 0 20px; display: grid; gap: 14px; }
  #timeline:empty { display: none; }
  .ev {
    border: 3px solid var(--ink);
    border-left-width: 12px;
    background: var(--paper);
    box-shadow: 4px 4px 0 var(--ink);
    padding: 10px 12px;
    overflow-wrap: anywhere;
    min-width: 0;
  }
  .ev.generate { border-left-color: var(--deep); }
  .ev.run { border-left-color: var(--teal); }
  .ev.verdict { border-left-color: var(--punch); }
  .ev.raw { border-left-color: var(--warm); }
  .ev .tag {
    display: inline-block;
    font-family: var(--display);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .08em;
    border: 2px solid var(--ink);
    padding: 2px 10px;
    margin: 0 8px 4px 0;
    transform: rotate(-1deg);
    background: var(--paper);
    color: var(--ink);
  }
  .ev.generate .tag { background: var(--deep); color: var(--paper); }
  .ev.run .tag { background: var(--teal); color: var(--paper); }
  .ev.verdict .tag { background: var(--punch); color: var(--paper); }
  .ev.raw .tag { background: var(--sun); color: var(--ink); }
  .ev .meta { color: var(--ink); opacity: .78; font-family: var(--mono); font-size: 13px; }
  .ev .ok { color: var(--deep); font-weight: 800; }
  .ev .bad { color: var(--punch); font-weight: 800; }

  pre {
    background: var(--ink);
    color: var(--paper);
    border: 3px solid var(--ink);
    box-shadow: 4px 4px 0 var(--teal);
    padding: 10px 12px;
    overflow: auto;
    max-width: 100%;
    max-height: 340px;
    white-space: pre-wrap;
    word-break: break-word;
    margin: 8px 0 0;
    font-family: var(--mono);
    font-size: 13px;
  }
  details { margin-top: 8px; }
  details summary {
    cursor: pointer;
    display: inline-block;
    font-family: var(--display);
    font-size: 12px;
    text-transform: uppercase;
    letter-spacing: .06em;
    color: var(--paper);
    background: var(--warm);
    border: 2px solid var(--ink);
    padding: 3px 10px;
  }

  /* ---- repro panel ---- */
  .panel {
    border: 3px solid var(--ink);
    box-shadow: 6px 6px 0 var(--ink);
    background: var(--foam);
    padding: 18px;
    margin: 12px 0;
    display: none;
  }
  .panel h2 {
    display: inline-block;
    font-family: var(--display);
    font-size: 16px;
    text-transform: uppercase;
    letter-spacing: .05em;
    background: var(--ink);
    color: var(--paper);
    padding: 5px 14px;
    margin: 0 0 6px;
    transform: rotate(-1deg);
  }
  #actionmsg { min-height: 1.4em; font-weight: 700; margin: 8px 0; overflow-wrap: anywhere; }
  footer.foot {
    margin-top: 22px;
    border: 3px solid var(--ink);
    background: var(--paper);
    box-shadow: 4px 4px 0 var(--ink);
    padding: 8px 12px;
    font-family: var(--mono);
    font-size: 12px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .05em;
  }
  @media (max-width: 640px) {
    main { padding: 16px 12px 56px; }
    .hero { box-shadow: 5px 5px 0 var(--punch); padding: 18px 16px; }
    .hero::after { display: none; }
    .card, .panel { padding: 14px; box-shadow: 4px 4px 0 var(--ink); }
    .btns { flex-direction: column; align-items: stretch; }
    .btns button { width: 100%; }
    #banner { transform: none; }
  }
</style>
</head>
<body>
<main>
  <header class="hero">
    <div class="stickers" aria-hidden="true">
      <span class="sticker s1">runs it 3x</span>
      <span class="sticker s2">issue to repro</span>
    </div>
    <h1>KAG — issue → <span class="hl">minimal repro</span></h1>
    <p class="sub">Paste an issue (text or URL). The harness generates a candidate, runs it 3×, and gates a verdict.</p>
    <div class="steps" aria-hidden="true">
      <span>paste</span>
      <span>candidate</span>
      <span>verdict</span>
    </div>
  </header>
  <section class="card" aria-label="start a run">
    <span class="kicker">01 - paste the issue</span>
    <form id="f">
      <label class="fld" for="issue">Issue text or link</label>
      <textarea id="issue" placeholder="Paste issue text or https://github.com/owner/repo/issues/123"></textarea>
      <div class="btns">
        <button type="submit" id="run" class="primary">Run</button>
        <button type="button" data-fixture="crash-deterministic">fixture: crash-deterministic</button>
        <button type="button" data-fixture="flaky-race">fixture: flaky-race</button>
      </div>
    </form>
    <div id="formmsg" role="status"></div>
    <div id="runstat" role="status"></div>
  </section>
  <div id="banner" role="status"></div>
  <section aria-label="run timeline">
    <span class="kicker sun">02 - watch the run</span>
    <div id="timeline"></div>
  </section>
  <div class="panel" id="reproPanel">
    <h2>minimal repro</h2>
    <div class="btns">
      <button type="button" id="copyRepro">copy repro</button>
      <button type="button" id="cReplay">replay</button>
      <button type="button" id="toggleLog">raw log</button>
    </div>
    <div id="actionmsg" role="status"></div>
    <pre id="reproText"></pre>
    <pre id="logText" style="display:none"></pre>
  </div>
  <footer class="foot">harness generates - runs 3x - gates a verdict</footer>
</main>
<script>
(function () {
  "use strict";
  var forcePoll = new URLSearchParams(window.location.search).get("poll") === "1";
  function byId(x) { return document.getElementById(x); }
  var issue = byId("issue"), form = byId("f"), runBtn = byId("run"), formmsg = byId("formmsg");
  var banner = byId("banner"), timeline = byId("timeline"), reproPanel = byId("reproPanel");
  var reproText = byId("reproText"), logText = byId("logText"), actionmsg = byId("actionmsg");

  var id = null, es = null, pollTimer = null, busy = false;
  var allEvents = [], rendered = 0, terminal = false, pollFailures = 0;
  var runstat = byId("runstat"), runClock = null, runT0 = 0;

  function setText(node, text) { node.textContent = text == null ? "" : String(text); }
  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = String(text);
    return n;
  }
  function safeJson(value) {
    try { return JSON.stringify(value); } catch (err) { return String(value); }
  }
  function lineCount(src) {
    // generate.source always ends with a newline — don't count the trailing empty split.
    var parts = String(src).split("\\n");
    if (parts.length > 0 && parts[parts.length - 1] === "") parts.pop();
    return parts.length;
  }
  function sigOf(ev) {
    if (ev == null) return null;
    if (ev.sig != null) return ev.sig;
    if (ev.signature != null) return ev.signature;
    return null;
  }
  function runIndexOf(ev) {
    if (ev == null) return null;
    if (ev.run != null) return ev.run;
    if (ev.runIndex != null) return ev.runIndex;
    return null;
  }

  function fmtDur(ms) {
    var s = Math.floor(ms / 1000);
    var m = Math.floor(s / 60); s = s % 60;
    return (m < 10 ? "0" + m : "" + m) + ":" + (s < 10 ? "0" + s : "" + s);
  }
  function stopClock() { if (runClock) { clearInterval(runClock); runClock = null; } }
  function startClock() {
    stopClock(); runT0 = Date.now();
    setText(runstat, "run " + id + " · waiting for first event 00:00 (inference can take minutes on CPU)");
    runClock = setInterval(function () {
      if (terminal || !id) { stopClock(); return; }
      var streaming = allEvents.length > 0 || rendered > 0;
      setText(runstat, "run " + id + (streaming ? " · streaming " : " · waiting for first event ") + fmtDur(Date.now() - runT0));
    }, 1000);
  }
  function endClock() {
    stopClock();
    if (id && runT0) setText(runstat, "run " + id + " · done in " + fmtDur(Date.now() - runT0));
  }

  function renderEvent(ev) {
    var type = ev && typeof ev.type === "string" ? ev.type : "unknown";
    var card = el("div", "ev " + type.replace(/[^a-z-]/gi, ""));
    var head = el("div");
    head.appendChild(el("span", "tag", type));
    if (ev && ev.attempt != null) head.appendChild(el("span", "meta", "attempt " + (typeof ev.attempt === "number" ? ev.attempt + 1 : ev.attempt) + " · "));
    if (type === "generate") {
      if (ev && ev.model != null) head.appendChild(el("span", "meta", String(ev.model)));
    } else if (type === "run") {
      var r = runIndexOf(ev);
      if (r != null) head.appendChild(el("span", "meta", "run " + (typeof r === "number" ? r + 1 : r) + "-of-3 · "));
      var s = sigOf(ev);
      var sCls = "meta";
      if (s != null) sCls += String(s) === "NO_FAIL" ? " ok" : " bad";
      head.appendChild(el("span", sCls, "sig: " + (s == null ? "(none)" : String(s))));
    } else if (type === "verdict") {
      head.appendChild(el("span", "meta", "verdict: " + (ev && ev.verdict != null ? String(ev.verdict) : "?")));
    }
    card.appendChild(head);
    if (type === "generate" && ev && typeof ev.source === "string") {
      var d = document.createElement("details");
      d.appendChild(el("summary", "", "generated candidate (" + lineCount(ev.source) + " lines)"));
      d.appendChild(el("pre", "", ev.source));
      card.appendChild(d);
      if (ev && typeof ev.thinking === "string" && ev.thinking) {
        var t = document.createElement("details");
        t.appendChild(el("summary", "", "model thinking (" + ev.thinking.length + " chars)"));
        t.appendChild(el("pre", "", ev.thinking));
        card.appendChild(t);
      }
    } else if (type !== "run" && type !== "verdict") {
      card.appendChild(el("pre", "", safeJson(ev)));
    }
    timeline.appendChild(card);
  }

  function stopStreaming() {
    if (es) { try { es.close(); } catch (err) {} es = null; }
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  function showError(ev) {
    terminal = true;
    stopStreaming();
    endClock();
    banner.className = "err";
    banner.style.display = "block";
    setText(banner, "error: " + (ev && ev.error != null ? String(ev.error) : "stream error"));
  }

  function showFinal(ev) {
    terminal = true;
    stopStreaming();
    endClock();
    var raw = ev && ev.verdict != null ? String(ev.verdict) : "unknown";
    // Lane 2 emits the combined form "deterministic(crash)" / "cannot-repro(flaky)"
    // plus a structured mode field. Trust mode first; the parenthesized suffix is a
    // legacy fallback only. Both are trimmed — spaced variants must not lose styling.
    var verdict = raw.split("(")[0].trim();
    var mode = ev && ev.mode != null ? String(ev.mode).trim() : "";
    if (!mode) {
      var m = raw.match(/\(([^)]+)\)/);
      if (m) mode = m[1].trim();
    }
    var label = verdict + (mode ? " (" + mode + ")" : "");
    banner.className = verdict === "deterministic" ? "det" : (verdict === "cannot-repro" ? "cant" : "");
    banner.style.display = "block";
    var extra = (ev && ev.repro ? " · " + ev.repro : "")
      + (ev && ev.declaredNoRepro ? " · model declared no scriptable trigger" : "");
    setText(banner, "final: " + label + extra);
    if (ev && ev.repro) {
      reproPanel.style.display = "block";
      if (id) {
        fetch("/repro/" + encodeURIComponent(id)).then(function (resp) {
          return resp.ok ? resp.text() : null;
        }).then(function (text) {
          if (text != null && !reproText.textContent) setText(reproText, text);
        }).catch(function () {});
      }
    }
  }

  function processEvent(ev) {
    allEvents.push(ev);
    var t = ev && typeof ev.type === "string" ? ev.type : "";
    if (t === "final") { showFinal(ev); return; }
    if (t === "error") { showError(ev); return; }
    renderEvent(ev);
  }

  function startSse() {
    es = new EventSource("/events/" + encodeURIComponent(id));
    es.onmessage = function (m) {
      var ev;
      try { ev = JSON.parse(m.data); } catch (err) { ev = { type: "raw", line: m.data }; }
      processEvent(ev);
    };
    es.onerror = function () {
      if (terminal) return;
      if (es) { try { es.close(); } catch (err) {} es = null; }
      startPolling();
    };
  }

  async function pollOnce() {
    if (!id || terminal) return;
    var resp;
    try { resp = await fetch("/status/" + encodeURIComponent(id)); }
    catch (err) { return; }
    if (resp.status === 404) {
      pollFailures += 1;
      if (pollFailures >= 60) showError({ error: "no run log for " + id + " after 60s of polling" });
      return;
    }
    if (!resp.ok) return;
    var data;
    try { data = await resp.json(); } catch (err) { return; }
    pollFailures = 0;
    var events = data && Array.isArray(data.events) ? data.events : [];
    for (var i = rendered; i < events.length; i++) processEvent(events[i]);
    rendered = events.length;
    // Log is complete but carried no final event — the harness died mid-run.
    // End polling with an honest error instead of polling forever.
    if (data && data.done && !terminal) showError({ error: "run log ended with no final event (harness died?)" });
  }

  function startPolling() {
    if (terminal || pollTimer) return;
    rendered = 0; allEvents = []; timeline.textContent = ""; pollFailures = 0;
    pollTimer = setInterval(pollOnce, 1000);
    pollOnce();
  }

  form.onsubmit = async function (e) {
    e.preventDefault();
    if (busy) return;
    var text = issue.value.trim();
    if (!text) { setText(formmsg, "paste issue text or a URL first"); return; }
    busy = true;
    stopStreaming();
    terminal = false; allEvents = []; rendered = 0; pollFailures = 0;
    timeline.textContent = "";
    banner.style.display = "none"; banner.textContent = ""; banner.className = "";
    setText(runstat, "");
    reproPanel.style.display = "none"; reproText.textContent = "";
    logText.textContent = ""; logText.style.display = "none"; actionmsg.textContent = "";
    setText(formmsg, "starting run…");
    runBtn.disabled = true; runBtn.textContent = "running…";
    try {
      var resp = await fetch("/run", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ issue: text })
      });
      var data = null;
      try { data = await resp.json(); } catch (err) { data = null; }
      if (resp.status === 429) {
        setText(formmsg, "429 — " + ((data && data.error) || "too many concurrent runs, retry shortly"));
      } else if (!resp.ok) {
        setText(formmsg, "error " + resp.status + (data && data.error ? " — " + data.error : ""));
      } else if (data && data.id) {
        id = data.id;
        setText(formmsg, "run " + id + " started" + (forcePoll ? " (polling)" : ""));
        if (forcePoll) startPolling(); else startSse();
        startClock();
      } else {
        setText(formmsg, "unexpected response from /run");
      }
    } catch (err) {
      setText(formmsg, "network error: " + (err && err.message ? err.message : err));
    }
    busy = false; runBtn.disabled = false; runBtn.textContent = "Run";
  };

  var fixtureButtons = document.querySelectorAll("[data-fixture]");
  for (var fi = 0; fi < fixtureButtons.length; fi++) {
    fixtureButtons[fi].addEventListener("click", async function () {
      var name = this.getAttribute("data-fixture");
      try {
        var resp = await fetch("/fixtures/" + encodeURIComponent(name));
        if (!resp.ok) { setText(formmsg, "fixture " + name + " unavailable (" + resp.status + ")"); return; }
        issue.value = await resp.text();
        setText(formmsg, "loaded fixture: " + name);
      } catch (err) { setText(formmsg, "fixture load failed"); }
    });
  }

  byId("copyRepro").addEventListener("click", async function () {
    if (!id) return;
    try {
      var resp = await fetch("/repro/" + encodeURIComponent(id));
      if (!resp.ok) { setText(actionmsg, "no repro artifact (" + resp.status + ")"); return; }
      var text = await resp.text();
      setText(reproText, text);
      if (navigator.clipboard && navigator.clipboard.writeText) {
        try {
          await navigator.clipboard.writeText(text);
          setText(actionmsg, "copied " + text.length + " chars to clipboard");
        } catch (err) {
          setText(actionmsg, "clipboard blocked — select the text below");
        }
      } else {
        setText(actionmsg, "clipboard unavailable — select the text below");
      }
    } catch (err) { setText(actionmsg, "repro fetch failed"); }
  });

  byId("cReplay").addEventListener("click", async function () {
    if (!id) return;
    setText(actionmsg, "replaying…");
    try {
      var resp = await fetch("/replay/" + encodeURIComponent(id));
      var data = await resp.json();
      var out = data && data.output != null ? String(data.output) : "";
      var code = data && data.exitCode != null ? data.exitCode : "?";
      setText(actionmsg, "exit " + code + (out ? " — " + out : " — (no output)") +
        (data && data.error ? " [" + data.error + "]" : ""));
    } catch (err) { setText(actionmsg, "replay failed"); }
  });

  byId("toggleLog").addEventListener("click", async function () {
    if (!id) return;
    if (logText.style.display !== "none") { logText.style.display = "none"; return; }
    try {
      var resp = await fetch("/log/" + encodeURIComponent(id));
      if (!resp.ok) { setText(actionmsg, "no log artifact (" + resp.status + ")"); return; }
      setText(logText, await resp.text());
      logText.style.display = "block";
    } catch (err) { setText(actionmsg, "log fetch failed"); }
  });
})();
</script>
</body>
</html>`;
  res.writeHead(200, {
    "content-type": "text/html; charset=utf-8",
    "content-length": Buffer.byteLength(html),
  });
  res.end(html);
}

// ---------------------------------------------------------------------------
// Router

function handleRequest(req: IncomingMessage, res: ServerResponse): void {
  let url: URL;
  try {
    url = new URL(req.url ?? "/", "http://localhost");
  } catch {
    json(res, 400, { error: "bad request URL" });
    return;
  }
  const path = url.pathname;
  const parts = path.split("/").filter(Boolean);
  const method = req.method ?? "GET";

  if (method === "GET" && path === "/") {
    handleRoot(res);
    return;
  }
  if (method === "GET" && path === "/healthz") {
    json(res, 200, { ok: true });
    return;
  }
  if (method === "POST" && path === "/run") {
    settle(handlePostRun(req, res), res);
    return;
  }
  if (method === "GET" && parts.length === 2) {
    const id = parts[1];
    if (parts[0] === "events") {
      settle(handleEvents(req, res, id), res);
      return;
    }
    if (parts[0] === "status") {
      settle(handleStatus(res, id), res);
      return;
    }
    if (parts[0] === "repro") {
      settle(handleRepro(res, id), res);
      return;
    }
    if (parts[0] === "log") {
      settle(handleLog(res, id), res);
      return;
    }
    if (parts[0] === "replay") {
      settle(handleReplay(req, res, id), res);
      return;
    }
    if (parts[0] === "fixtures") {
      settle(handleFixture(res, id), res);
      return;
    }
  }
  json(res, 404, { error: "not found" });
}

const server = createServer((req, res) => {
  try {
    handleRequest(req, res);
  } catch (err) {
    console.error("request dispatch error:", err);
    if (!res.headersSent) json(res, 500, { error: "internal error" });
    else {
      try {
        res.end();
      } catch {
        /* socket already gone */
      }
    }
  }
});

// ---------------------------------------------------------------------------
// Boot: ensure dirs, prune stale runs (best-effort), listen

async function pruneRuns(): Promise<void> {
  const cutoff = Date.now() - PRUNE_MAX_AGE_MS;
  try {
    const entries = await readdir(RUNS_DIR);
    for (const name of entries) {
      try {
        const p = join(RUNS_DIR, name);
        const st = await stat(p);
        if (st.isFile() && st.mtimeMs < cutoff) await unlink(p);
      } catch {
        /* best effort — never throw at boot */
      }
    }
  } catch {
    /* runs/ does not exist yet */
  }
}

await mkdir(RUNS_DIR, { recursive: true }).catch(() => undefined);
await mkdir(REPROS_DIR, { recursive: true }).catch(() => undefined);
await pruneRuns();

// Re-prune on a timer: a long-lived process must not accumulate runs/ forever.
const pruneTimer = setInterval(() => {
  pruneRuns().catch((err) => console.error("periodic prune failed:", err));
}, 6 * 60 * 60 * 1000);
pruneTimer.unref();

server.listen(PORT, () => {
  console.log(`kag ui on :${PORT} — harness: ${HARNESS_SCRIPT}`);
});
