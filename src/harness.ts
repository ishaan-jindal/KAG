// Lane 2 — generate → 3 attempts × 3 runs → gate → verdict → artifacts.
//
// Consumes lane 1's `runSandbox` / `signature` / `gate` by injection rather than
// by hard import, so `src/eval/` can drive the loop against a local reference
// implementation before lane 1 lands. main() wires the real ones. Nothing here
// mutates lane 1 / lane 3 / lane 4 files.

import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import type { GateVerdict } from "./verify.ts";
import type { RunResult } from "./tools.ts";

export const MAX_ATTEMPTS = 3;
export const RUNS_PER_ATTEMPT = 3;

/** Open verdict vocabulary — frozen in TEAM.md, nothing outside this union is ever emitted. */
export type Outcome = {
  verdict: "deterministic" | "cannot-repro";
  mode: "crash" | "hang" | "flaky" | "no-failure" | "invalid-candidate";
};

export type Usage = { tokensIn: number; tokensOut: number };

// --- LogEvent ---------------------------------------------------------------
// ADDITIVE CHANGE vs the scaffold shape (TEAM.md requires announce-before-change):
// every event gained `ts`; `generate` gained `usage`; `final` gained
// `attempt` / `kinds` / `usage` / `declaredNoRepro`. No field was removed or
// renamed, so lane 3's JSONL-tail reader keeps working against old logs.

export type LogEvent =
  | { type: "generate"; attempt: number; model: string; source: string; ts: string; usage: Usage; truncated: boolean }
  | { type: "run"; attempt: number; run: number; signature: string; ts: string }
  | { type: "verdict"; attempt: number; verdict: string; ts: string }
  | {
      type: "final";
      verdict: string;
      repro: string | null;
      attempt: number | null;
      kinds: string[];
      usage: Usage;
      declaredNoRepro: boolean;
      ts: string;
    };

export type Sandbox = {
  runSandbox: (entryFile: string, attemptDir: string) => Promise<RunResult>;
  signature: (run: RunResult) => string;
  gate: (sigs: [string, string, string]) => GateVerdict;
};

export type Attempt = {
  attempt: number;
  source: string;
  sigs: string[];
  outcome: Outcome;
  violations: string[];
  truncated: boolean;
};

export type LoopResult = {
  outcome: Outcome;
  attempts: Attempt[];
  repro: string | null;
  winningAttempt: number | null;
  usage: Usage;
  declaredNoRepro: boolean;
};

// --- verdict mapping --------------------------------------------------------

export function toOutcome(g: GateVerdict, sig: string): Outcome {
  if (g === "deterministic") return { verdict: "deterministic", mode: sig === "TIMEOUT" ? "hang" : "crash" };
  return { verdict: "cannot-repro", mode: g };
}

export function renderOutcome(o: Outcome): string {
  return `${o.verdict} (${o.mode})`;
}

// --- erasable-only validation ----------------------------------------------
// Node strips types with no build step, so a non-erasable construct is a hard
// runtime SyntaxError → `invalid-candidate`. This is a cheap pre-flight so the
// model gets told instead of burning an attempt.

const ERASABLE_BANS: Array<[RegExp, string]> = [
  [/^[ \t]*enum[ \t]+\w/m, "enum"],
  [/^[ \t]*(declare[ \t]+)?namespace[ \t]+\w/m, "namespace"],
  [/^[ \t]*module[ \t]+\w/m, "module"],
  [/\bimport\s+\w+\s*=\s*require\(/, "import = require"],
  // Parameter properties appear inside the constructor parameter list, not at the
  // start of a line, so anchor on the opening paren instead.
  // `readonly` is deliberately absent: it is legal and erasable in type position
  // (`readonly x: string`), so banning it would reject valid code. A missed
  // parameter property still surfaces as a SyntaxError -> invalid-candidate.
  [/\(\s*(public|private|protected)\s+\w+\s*[:,)]/, "parameter property"],
  [/^[ \t]*@\w+/m, "decorator"],
];

export function erasableViolations(src: string): string[] {
  return ERASABLE_BANS.filter(([re]) => re.test(src)).map(([, label]) => label);
}

// --- prompt -----------------------------------------------------------------

const NO_REPRO_MARKER = "// NO_REPRO";

const SYSTEM_PROMPT = [
  "You write minimal TypeScript reproduction files for a bug triage harness.",
  "",
  "Rules, all mandatory:",
  `1. Reply with exactly ONE fenced \`\`\`ts code block. No prose before or after it.`,
  "2. Erasable TypeScript only — it is executed directly by Node with type",
  "   stripping and no build step. Forbidden: `enum`, `namespace`/`module`,",
  "   parameter properties, decorators.",
  "3. The file must THROW or HANG, reproducing the reported failure exactly.",
  "   Do NOT fix the bug. Do NOT add defensive checks that hide it. Do NOT",
  "   swallow the error.",
  "4. Under 20 lines. No explanatory comments.",
  "5. Import only from modules listed under AVAILABLE MODULES. If that list is",
  "   empty the file must be self-contained — inline the logic. Node ESM requires",
  "   the extension in the specifier: import from \"./config.ts\", never \"./config\".",
  "   Issue text often omits it; add it back.",
  "6. The sandbox denies network, filesystem writes and child processes. Do not",
  "   use them. No timers that outlive the crash.",
  "7. Unless the bug itself depends on it, keep the crash path free of",
  "   `Date`, `Math.random`, `process.pid` and `process.hrtime` — a repro whose",
  "   failure varies run to run is rejected.",
  "",
  "If the reported bug genuinely cannot be triggered from a script, reply with",
  "a single line containing exactly:",
  NO_REPRO_MARKER,
  "An honest NO_REPRO is a valid and valued answer. Inventing a crash to look",
  "useful is not.",
].join("\n");

function userPrompt(issue: string, modules: string[], feedback: string | null): string {
  const parts = [issue.trim()];
  if (modules.length > 0) {
    parts.push(`AVAILABLE MODULES (import from these, they exist on disk):\n${modules.map((m) => `- ${m}`).join("\n")}`);
  }
  if (feedback) parts.push(feedback);
  return parts.join("\n\n---\n\n");
}

// --- inference --------------------------------------------------------------

const INFERENCE_URL = process.env.DO_INFERENCE_URL ?? "https://inference.do-ai.run/v1/chat/completions";
const MODEL = process.env.DO_MODEL ?? "openai-gpt-oss-120b";

// gpt-oss-120b/20b cap *total* completion at 4096 and reasoning tokens come out
// of that same budget, so 2048 truncated mid-file → invalid-candidate.
const MAX_COMPLETION_TOKENS = Number(process.env.KAG_MAX_COMPLETION_TOKENS ?? 4000);

export type GenerateResult = { source: string; usage: Usage; truncated: boolean };

export async function generate(
  issue: string,
  modules: string[],
  feedback: string | null,
): Promise<GenerateResult> {
  const res = await fetch(INFERENCE_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.DO_INFERENCE_KEY ?? ""}`,
    },
    body: JSON.stringify({
      model: MODEL,
      messages: [
        { role: "system", content: SYSTEM_PROMPT },
        { role: "user", content: userPrompt(issue, modules, feedback) },
      ],
      max_completion_tokens: MAX_COMPLETION_TOKENS,
      temperature: 0.2,
    }),
    signal: AbortSignal.timeout(120_000),
  });
  if (!res.ok) throw new Error(`inference ${res.status} ${res.statusText}`);

  const data = (await res.json()) as {
    choices?: Array<{
      message?: { content?: string };
      finish_reason?: string | null;
    }>;
    usage?: { prompt_tokens?: number; completion_tokens?: number };
  };

  const choice = data.choices?.[0];
  const raw = choice?.message?.content ?? "";
  const usage: Usage = {
    tokensIn: data.usage?.prompt_tokens ?? 0,
    tokensOut: data.usage?.completion_tokens ?? 0,
  };
  const truncated = choice?.finish_reason === "length";
  return { source: extractSource(raw), usage, truncated };
}

export function extractSource(text: string): string {
  const fenced = text.match(/```(?:ts|typescript|javascript|js)?[ \t]*\r?\n([\s\S]*?)```/);
  return (fenced ? fenced[1] : text).trim() + "\n";
}

export function declaredNoRepro(source: string): boolean {
  return source.split("\n").some((l) => l.trim() === NO_REPRO_MARKER);
}

// --- retry feedback ---------------------------------------------------------
// For `flaky` we hand back the three divergent signatures rather than the word
// "flaky" — that is the whole point of normalising, and feeding the bare enum
// back makes attempt 2 nearly identical to attempt 1.

export function describe(attempt: Attempt): string {
  const lines: string[] = [`YOUR PREVIOUS ATTEMPT FAILED THE GATE: ${renderOutcome(attempt.outcome)}`];
  lines.push("Sandbox signatures, one per run:");
  for (let i = 0; i < attempt.sigs.length; i++) lines.push(`  run ${i + 1}: ${attempt.sigs[i] || "(empty)"}`);

  const distinct = [...new Set(attempt.sigs)];
  switch (attempt.outcome.mode) {
    case "flaky":
      lines.push(
        `These ${distinct.length} signatures DIFFER across runs. A repro that fails differently` +
          " run to run is not proof and is discarded. Remove every source of variation" +
          " (timing, randomness, pid/hrtime, iteration order, shared module state)" +
          " so all three runs fail identically.",
      );
      break;
    case "no-failure":
      lines.push(
        "The file exited cleanly every run — nothing failed. If the reported bug really does not" +
          " reproduce, answer with the NO_REPRO line instead of inventing a failure.",
      );
      break;
    case "invalid-candidate":
      lines.push(
        "The file failed to load, so no test ever ran. Emit only importable, self-contained TypeScript" +
          " that parses under Node's type stripping.",
      );
      break;
    case "crash":
    case "hang":
      lines.push("The gate returned deterministic but an earlier attempt already won — do not regress it.");
      break;
  }

  if (attempt.violations.length > 0) {
    lines.push(`Non-erasable constructs detected: ${attempt.violations.join(", ")}. These are SyntaxErrors here.`);
  }
  if (attempt.truncated) {
    lines.push("Your last reply was cut off at the token limit. Emit a shorter file — under 20 lines, no comments.");
  }
  return lines.join("\n");
}

// --- the loop ---------------------------------------------------------------

export type LoopOptions = {
  issue: string;
  /** Artifact id, used for the repro path recorded in the `final` event. */
  id?: string;
  /** Files materialised beside the candidate so repros can import real code. */
  modules?: Record<string, string>;
  sandbox: Sandbox;
  onEvent?: (e: LogEvent) => void;
  /** Swap the generator out (tests / eval plumbing checks). */
  generateFn?: (issue: string, modules: string[], feedback: string | null) => Promise<GenerateResult>;
  workDir?: string;
};

export async function runLoop(opts: LoopOptions): Promise<LoopResult> {
  const modules = opts.modules ?? {};
  const moduleNames = Object.keys(modules);
  const gen = opts.generateFn ?? generate;
  const emit = opts.onEvent ?? ((): void => {});
  const now = (): string => new Date().toISOString();

  const attempts: Attempt[] = [];
  const usage: Usage = { tokensIn: 0, tokensOut: 0 };
  let repro: string | null = null;
  let winningAttempt: number | null = null;
  let declared = false;
  let outcome: Outcome = { verdict: "cannot-repro", mode: "no-failure" };

  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    const feedback = attempt === 0 ? null : describe(attempts[attempt - 1]);
    // A transport failure is not a repro failure — surface it rather than
    // silently burning the remaining attempts.
    const generated = await gen(opts.issue, moduleNames, feedback);
    usage.tokensIn += generated.usage.tokensIn;
    usage.tokensOut += generated.usage.tokensOut;
    emit({
      type: "generate",
      attempt,
      model: MODEL,
      source: generated.source,
      ts: now(),
      usage: generated.usage,
      truncated: generated.truncated,
    });

    if (declaredNoRepro(generated.source)) {
      declared = true;
      outcome = { verdict: "cannot-repro", mode: "no-failure" };
      emit({ type: "verdict", attempt, verdict: renderOutcome(outcome), ts: now() });
      break;
    }

    const dir = opts.workDir ?? join(tmpdir(), `kag-${process.pid}-${attempt}`);
    mkdirSync(dir, { recursive: true });
    for (const [name, src] of Object.entries(modules)) writeFileSync(join(dir, name), src);
    const entry = join(dir, "repro.ts");
    writeFileSync(entry, generated.source);

    const sigs: string[] = [];
    for (let run = 1; run <= RUNS_PER_ATTEMPT; run++) {
      const result = await opts.sandbox.runSandbox(entry, dir);
      const sig = opts.sandbox.signature(result);
      sigs.push(sig);
      emit({ type: "run", attempt, run, signature: sig, ts: now() });
    }

    const typed = sigs as [string, string, string];
    outcome = toOutcome(opts.sandbox.gate(typed), typed[0]);
    const record: Attempt = {
      attempt,
      source: generated.source,
      sigs,
      outcome,
      violations: erasableViolations(generated.source),
      truncated: generated.truncated,
    };
    attempts.push(record);
    emit({ type: "verdict", attempt, verdict: renderOutcome(outcome), ts: now() });

    if (outcome.verdict === "deterministic") {
      repro = generated.source;
      winningAttempt = attempt;
      break;
    }
  }

  emit({
    type: "final",
    verdict: renderOutcome(outcome),
    repro: winningAttempt === null ? null : `repros/${opts.id ?? "adhoc"}.repro.ts`,
    attempt: winningAttempt,
    kinds: attempts.map((a) => renderOutcome(a.outcome)),
    usage,
    declaredNoRepro: declared,
    ts: new Date().toISOString(),
  });

  return { outcome, attempts, repro, winningAttempt, usage, declaredNoRepro: declared };
}

// --- judge (display only) ---------------------------------------------------
// Never feeds back into a verdict. One extra model call turning the structured
// outcome into a human sentence; falls back to a local template so a missing
// key or a failed call never breaks a run.

export function fallbackSummary(result: LoopResult): string {
  if (result.outcome.verdict === "deterministic") {
    return `Confirmed reproducible: the candidate fails ${renderOutcome(result.outcome)} on all ${RUNS_PER_ATTEMPT} runs. Attempts: ${result.attempts.map((a) => renderOutcome(a.outcome)).join(" → ")}.`;
  }
  if (result.declaredNoRepro) {
    return "Not reproduced: the model reported no scriptable trigger for this report. Reported as an honest negative.";
  }
  return `Not reproduced. Attempts: ${result.attempts.map((a) => renderOutcome(a.outcome)).join(" → ")}.`;
}

export async function judge(issue: string, result: LoopResult): Promise<string> {
  if (!process.env.DO_INFERENCE_KEY) return fallbackSummary(result);
  const attemptLine = result.attempts
    .map((a) => `attempt ${a.attempt + 1}: ${renderOutcome(a.outcome)} [${a.sigs.join(" | ")}]`)
    .join("\n");
  try {
    const res = await fetch(INFERENCE_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.DO_INFERENCE_KEY}`,
      },
      body: JSON.stringify({
        model: MODEL,
        messages: [
          {
            role: "system",
            content:
              "Summarise a bug-reproduction attempt for a maintainer in at most 3 sentences. State what was tried, " +
              "the verdict, and whether the next step is a fix or a better bug report. Do not speculate.",
          },
          {
            role: "user",
            content: `ISSUE:\n${issue.slice(0, 2000)}\n\nVERDICT: ${renderOutcome(result.outcome)}\n\n${attemptLine}`,
          },
        ],
        max_completion_tokens: 300,
        temperature: 0.2,
      }),
      signal: AbortSignal.timeout(120_000),
    });
    if (!res.ok) return fallbackSummary(result);
    const data = (await res.json()) as { choices?: Array<{ message?: { content?: string } }> };
    return data.choices?.[0]?.message?.content?.trim() || fallbackSummary(result);
  } catch {
    return fallbackSummary(result);
  }
}

// --- issue input ------------------------------------------------------------

export function parseIssueRef(ref: string): { owner: string; repo: string; number: number } | null {
  const m = ref.match(/github\.com\/([^/\s]+)\/([^/\s]+)\/issues\/(\d+)/);
  if (!m) return null;
  return { owner: m[1], repo: m[2], number: Number(m[3]) };
}

/** Fetch an issue + a few comments and serialise them into the prompt. */
export async function fetchIssue(ref: string): Promise<string> {
  const parsed = parseIssueRef(ref);
  if (!parsed) throw new Error(`not a GitHub issue URL: ${ref}`);
  const { owner, repo, number } = parsed;
  const api = `https://api.github.com/repos/${owner}/${repo}/issues/${number}`;
  const headers: Record<string, string> = {
    Accept: "application/vnd.github+json",
    "User-Agent": "kag",
  };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;

  const res = await fetch(api, { headers, signal: AbortSignal.timeout(30_000) });
  if (res.status === 404) throw new Error(`issue not found or private: ${api}`);
  if (res.status === 403) throw new Error(`GitHub API rate limited or token lacks access: ${api}`);
  if (!res.ok) throw new Error(`github ${res.status}`);

  const issue = (await res.json()) as {
    title?: string;
    body?: string;
    labels?: Array<{ name?: string }>;
    state?: string;
  };

  const sections = [`# ${issue.title ?? "(untitled)"}`, "", (issue.body ?? "(no body)").slice(0, 8000)];
  const labels = (issue.labels ?? []).map((l) => l.name).filter(Boolean);
  if (labels.length > 0) sections.push(`labels: ${labels.join(", ")}`);
  if (issue.state) sections.push(`state: ${issue.state}`);

  const cRes = await fetch(`${api}/comments`, { headers, signal: AbortSignal.timeout(30_000) });
  if (cRes.ok) {
    const comments = (await cRes.json()) as Array<{ body?: string }>;
    if (comments.length > 0) {
      sections.push("", "## Comments");
      for (const c of comments.slice(0, 5)) sections.push("", (c.body ?? "").slice(0, 1500));
    }
  }
  return sections.join("\n");
}

export async function readIssue(ref: string): Promise<string> {
  return ref.startsWith("http") ? fetchIssue(ref) : readFileSync(ref, "utf8");
}

// --- replay -----------------------------------------------------------------
// Re-runs the *stored* source and diffs signatures against what was logged, so
// "replay integrity" means the gate is reproducible, not just re-readable.

export type ReplayResult = { attempt: number; stored: string[]; rerun: string[]; identical: boolean };

export async function replayLog(
  events: LogEvent[],
  sandbox: Sandbox,
  workDir: string,
  modules: Record<string, string> = {},
): Promise<ReplayResult> {
  const final = [...events].reverse().find((e) => e.type === "final");
  const attempt = final && final.type === "final" ? final.attempt : null;
  const gens = events.filter((e): e is Extract<LogEvent, { type: "generate" }> => e.type === "generate");
  const target = gens.find((g) => g.attempt === attempt) ?? gens[gens.length - 1];
  if (!target) throw new Error("no generate event to replay");

  const stored = events
    .filter((e): e is Extract<LogEvent, { type: "run" }> => e.type === "run" && e.attempt === target.attempt)
    .map((r) => r.signature);

  const dir = join(workDir, `replay-${target.attempt}`);
  mkdirSync(dir, { recursive: true });
  // The repro may import the modules it was generated against; without them a
  // replay fails to load and the integrity check is meaningless.
  for (const [name, src] of Object.entries(modules)) writeFileSync(join(dir, name), src);
  const entry = join(dir, "repro.ts");
  writeFileSync(entry, target.source);

  const rerun: string[] = [];
  for (let run = 1; run <= RUNS_PER_ATTEMPT; run++) {
    rerun.push(sandbox.signature(await sandbox.runSandbox(entry, dir)));
  }
  return {
    attempt: target.attempt,
    stored,
    rerun,
    identical: stored.join("|") === rerun.join("|"),
  };
}

// --- CLI --------------------------------------------------------------------

function parseArgs(argv: string[]): { issue?: string; replay?: string } {
  const out: { issue?: string; replay?: string } = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--issue") out.issue = argv[++i];
    else if (argv[i] === "--replay") out.replay = argv[++i];
  }
  return out;
}

function logTo(id: string, e: LogEvent): void {
  mkdirSync("runs", { recursive: true });
  appendFileSync(`runs/${id}.jsonl`, JSON.stringify(e) + "\n");
}

async function main(): Promise<void> {
  const { issue, replay } = parseArgs(process.argv.slice(2));

  const [{ runSandbox, signature }, { gate }] = await Promise.all([
    import("./tools.ts"),
    import("./verify.ts"),
  ]);
  const sandbox: Sandbox = { runSandbox, signature, gate };

  if (replay) {
    const events = (readFileSync(`runs/${replay}.jsonl`, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((l) => JSON.parse(l) as LogEvent));
    const result = await replayLog(events, sandbox, tmpdir());
    console.log(JSON.stringify(result, null, 2));
    if (!result.identical) process.exitCode = 1;
    return;
  }

  if (!issue) throw new Error("usage: node src/harness.ts --issue <url|file> [--replay <id>]");
  const issueNum = issue.startsWith("http") ? parseIssueRef(issue)?.number : null;
  const id = issueNum ? String(issueNum) : Date.now().toString(36);

  const body = await readIssue(issue);
  const result = await runLoop({ id, issue: body, sandbox, onEvent: (e) => logTo(id, e) });

  if (result.repro !== null) {
    mkdirSync("repros", { recursive: true });
    writeFileSync(`repros/${id}.repro.ts`, result.repro);
  }

  const summary = await judge(body, result);
  console.log(renderOutcome(result.outcome));
  console.log(summary);
  if (result.repro !== null) console.log(`repros/${id}.repro.ts`);
  console.log(`runs/${id}.jsonl`);
  console.log(
    `tokens in=${result.usage.tokensIn} out=${result.usage.tokensOut} model=${MODEL}`,
  );
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) await main();