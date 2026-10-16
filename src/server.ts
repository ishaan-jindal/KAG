import { createServer } from "node:http";
import { readFile, readdir } from "node:fs/promises";
import { join } from "node:path";

// Same LogEvent schema as runs/<id>.jsonl — one schema, two sinks (file + SSE).
// Lane 2 owns the JSONL writer; this file only tails it.
type LogEvent =
  | { type: "generate"; id: string; attempt: number; source: string; ts: string }
  | { type: "run"; id: string; attempt: number; runIndex: number; sig: string; ts: string }
  | { type: "verdict"; id: string; attempt: number; verdict: string; ts: string }
  | { type: "final"; id: string; verdict: string; mode: string; ts: string };

type RunRequest = { issue: string; issueId?: string };

const RUNS_DIR = "runs";
const REPROS_DIR = "repros";
const PORT = Number(process.env.PORT ?? 8080);

function json(res: import("node:http").ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { "content-type": "application/json" });
  res.end(text);
}

function readBody(req: import("node:http").IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let buf = "";
    req.on("data", (chunk: unknown) => {
      buf += String(chunk);
    });
    req.on("end", () => resolve(buf));
    req.on("error", reject);
  });
}

function handleRoot(res: import("node:http").ServerResponse): void {
  res.writeHead(200, { "content-type": "text/html" });
  res.end(`<!doctype html>
<html><body>
<h1>KAG</h1>
<form id="f"><textarea id="issue" rows="8" cols="60" placeholder="Paste issue text or URL"></textarea><br>
<button type="submit">Run</button>
<button type="button" id="replay">Replay</button></form>
<pre id="stream"></pre>
<script>
const pre = document.getElementById("stream");
let id = null;
document.getElementById("f").onsubmit = async (e) => {
  e.preventDefault();
  const issue = document.getElementById("issue").value;
  const r = await fetch("/run", { method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ issue }) });
  ({ id } = await r.json());
  const es = new EventSource("/events/" + id);
  es.onmessage = (m) => { pre.textContent += m.data + "\\n"; };
};
document.getElementById("replay").onclick = async () => {
  if (!id) return;
  const r = await fetch("/replay/" + id);
  pre.textContent += await r.text() + "\\n";
};
</script>
</body></html>`);
}

// TODO(lane-3): shell out to lane 2 harness with { issue, issueId }, return its run id.
async function handlePostRun(
  req: import("node:http").IncomingMessage,
  res: import("node:http").ServerResponse,
): Promise<void> {
  const raw = await readBody(req);
  const body = JSON.parse(raw) as RunRequest;
  if (!body.issue) {
    json(res, 400, { error: "issue required" });
    return;
  }
  const id = body.issueId ?? String(Date.now());
  // TODO: spawn `node src/harness.ts --issue ...` detached; SSE tails runs/<id>.jsonl.
  void req;
  json(res, 200, { id });
}

// TODO(lane-3): tail runs/<id>.jsonl as SSE; each line is one LogEvent.
async function handleEvents(id: string, res: import("node:http").ServerResponse): Promise<void> {
  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });
  try {
    const text = await readFile(join(RUNS_DIR, `${id}.jsonl`), "utf8");
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      const ev = JSON.parse(line) as LogEvent;
      res.write(`data: ${JSON.stringify(ev)}\n\n`);
    }
  } catch {
    res.write(`data: ${JSON.stringify({ type: "final", id, verdict: "cannot-repro", mode: "invalid-candidate" })}\n\n`);
  }
  // TODO: replace one-shot read with fs.watch tail + polling fallback if SSE gets fiddly.
  res.end();
}

// TODO(lane-3): serve repros/<id>.repro.ts.
async function handleRepro(id: string, res: import("node:http").ServerResponse): Promise<void> {
  try {
    const src = await readFile(join(REPROS_DIR, `${id}.repro.ts`), "utf8");
    res.writeHead(200, { "content-type": "text/plain" });
    res.end(src);
  } catch {
    res.writeHead(404);
    res.end("no repro");
  }
}

// TODO(lane-3): shell out `node src/harness.ts --replay <id>`, diff stored sig vs rerun.
async function handleReplay(id: string, res: import("node:http").ServerResponse): Promise<void> {
  void readdir;
  json(res, 501, { error: `replay not wired yet: ${id}` });
}

const server = createServer((req, res) => {
  const url = new URL(req.url ?? "/", "http://localhost");
  const parts = url.pathname.split("/").filter(Boolean);
  if (req.method === "GET" && url.pathname === "/") {
    handleRoot(res);
    return;
  }
  if (req.method === "POST" && url.pathname === "/run") {
    handlePostRun(req, res).catch(() => json(res, 500, { error: "run failed" }));
    return;
  }
  if (req.method === "GET" && parts[0] === "events" && parts[1]) {
    handleEvents(parts[1], res).catch(() => res.end());
    return;
  }
  if (req.method === "GET" && parts[0] === "repro" && parts[1]) {
    handleRepro(parts[1], res).catch(() => res.end());
    return;
  }
  if (req.method === "GET" && parts[0] === "replay" && parts[1]) {
    handleReplay(parts[1], res).catch(() => res.end());
    return;
  }
  res.writeHead(404);
  res.end("not found");
});

server.listen(PORT, () => {
  console.log(`kag ui on :${PORT}`);
});
