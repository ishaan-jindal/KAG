// Lane 2 — generate → 3 attempts × 3 runs loop. TODOs mark where logic lands.
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";

export type LogEvent =
  | { type: "generate"; attempt: number; model: string; source: string }
  | { type: "run"; attempt: number; run: number; signature: string }
  | { type: "verdict"; attempt: number; verdict: string }
  | { type: "final"; verdict: string; repro: string };

const INFERENCE_URL = process.env.DO_INFERENCE_URL ?? "https://inference.do-ai.run/v1/chat/completions";
const MODEL = process.env.DO_MODEL ?? "openai-gpt-oss-120b";

export async function generate(issue: string, attempt: number): Promise<string> {
  const res = await fetch(INFERENCE_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.DO_INFERENCE_KEY ?? ""}`,
    },
    body: JSON.stringify({
      model: MODEL,
      messages: [{ role: "user", content: issue }],
      max_completion_tokens: 2048,
      temperature: 0.2,
    }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!res.ok) throw new Error(`inference ${res.status}`);
  const data = (await res.json()) as {
    choices?: Array<{ message?: { content?: string } }>;
  };
  // TODO: extract code block / validate erasable-only TS.
  return data.choices?.[0]?.message?.content ?? "";
}

// Display-only prose summary after `final`; never affects verdict.
export function judge(_prose: string): string {
  // TODO: summarize final verdict for display.
  return _prose;
}

function log(id: string, event: LogEvent): void {
  mkdirSync("runs", { recursive: true });
  appendFileSync(`runs/${id}.jsonl`, JSON.stringify(event) + "\n");
}

function parseArgs(argv: string[]): { issue?: string; replay?: string } {
  const out: { issue?: string; replay?: string } = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--issue") out.issue = argv[++i];
    else if (argv[i] === "--replay") out.replay = argv[++i];
  }
  return out;
}

async function main(): Promise<void> {
  const { issue, replay } = parseArgs(process.argv.slice(2));
  if (replay) {
    // TODO: load runs/<id>.jsonl, rerun repro, diff stored sig vs rerun.
    return;
  }
  if (!issue) throw new Error("usage: node src/harness.ts --issue <url|file> [--replay <id>]");
  const body = issue.startsWith("http") ? issue : readFileSync(issue, "utf8"); // TODO: fetch issue URL via API.
  const id = Date.now().toString(36);
  for (let attempt = 0; attempt < 3; attempt++) {
    const source = await generate(body, attempt);
    log(id, { type: "generate", attempt, model: MODEL, source });
    // TODO: write candidate, 3× runSandbox → signature → gate → describe(verdict) feedback.
    log(id, { type: "verdict", attempt, verdict: "TODO" });
  }
  // TODO: pick best repro, gate final, write repros/<id>.repro.ts.
  mkdirSync("repros", { recursive: true });
  writeFileSync(`repros/${id}.repro.ts`, "// TODO: minimal repro\n");
  log(id, { type: "final", verdict: "TODO", repro: `repros/${id}.repro.ts` });
}

if (process.argv[1] === new URL(import.meta.url).pathname) await main();
