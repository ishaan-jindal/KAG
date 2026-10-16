// Lane 2 — eval scorer.
//
// Drives the harness loop once per suite entry against the local reference impl
// (./_ref/lane1.ts) + lane 1's real gate (../verify.ts), then scores:
//
//   60 verdict accuracy  15 minimality  15 attempt efficiency  10 replay integrity
//
// Hard contract = verdict + mode. Everything else is a diagnostic bound.
// Diagnostic only: the number is for comparing models, not for gating a merge.

import { readFile, appendFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  runLoop,
  replayLog,
  renderOutcome,
  type LogEvent,
  type LoopResult,
} from "../harness.ts";
import { gate } from "../verify.ts";
import * as ref from "./_ref/lane1.ts";

const W_VERDICT = 60;
const W_MINIMALITY = 15;
const W_EFFICIENCY = 15;
const W_REPLAY = 10;

const HERE = new URL(".", import.meta.url);

type Expect = {
  verdict: string;
  mode: string;
  sigContains: string;
  maxAttempts: number;
  reproMaxLoc: number;
  replay: boolean;
};

type SuiteEntry = {
  id: string;
  fixture: string;
  modules?: string[];
  inconclusiveOk?: boolean;
  retries?: number;
  expect: Expect;
};

type EntryResult = {
  id: string;
  got: string;
  want: string;
  verdictOk: boolean;
  minimalityOk: boolean;
  efficiencyOk: boolean;
  replayOk: boolean;
  attempts: number;
  reproLoc: number | null;
  sigs: string[];
  retries: number;
  inconclusive: boolean;
  note: string;
  usage: { tokensIn: number; tokensOut: number };
};

/**
 * Dry mode emits the canonical minimal repro for each fixture instead of calling
 * the model. It validates gate + replay + scorer plumbing for $0; it says
 * nothing about model quality, which is what the real run measures.
 */
const DRY_SOURCES: Record<string, string> = {
  "crash-deterministic": 'import { load } from "./config.ts";\nload({});\n',
  "volatile-trace": 'import { parse } from "./parse.ts";\nparse("bad");\n',
  hang: 'import { serve } from "./srv.ts";\nserve();\n',
  "flaky-race": 'import { push } from "./race.ts";\npush("j");\n',
  "not-a-bug": 'import { sum } from "./sum.ts";\nsum([1, 2, 3]);\n',
};

const sandbox = { runSandbox: ref.runSandbox, signature: ref.signature, gate };

function argOf(name: string, fallback: string): string {
  const i = process.argv.indexOf(name);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

/** USD per 1M tokens, from DO's published serverless inference rates. */
const RATES: Record<string, { in: number; out: number }> = {
  "openai-gpt-oss-120b": { in: 0.1, out: 0.7 },
  "openai-gpt-oss-20b": { in: 0.05, out: 0.45 },
  "qwen3.5-397b-a17b": { in: 0.55, out: 3.5 },
  "llama-4-maverick": { in: 0.2, out: 0.8 },
};

function cost(model: string, tokensIn: number, tokensOut: number): number {
  const r = RATES[model] ?? RATES["openai-gpt-oss-120b"];
  return (tokensIn / 1e6) * r.in + (tokensOut / 1e6) * r.out;
}

async function loadModules(names: string[] | undefined): Promise<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const name of names ?? []) {
    out[name] = await readFile(new URL(`fixtures/${name}`, HERE), "utf8");
  }
  return out;
}

async function runEntry(entry: SuiteEntry, dry: boolean): Promise<EntryResult> {
  const issue = await readFile(new URL(entry.fixture, HERE), "utf8");
  const modules = await loadModules(entry.modules);
  const workDir = join(tmpdir(), `kag-eval-${entry.id}-${process.pid}`);

  const events: LogEvent[] = [];
  const result: LoopResult = await runLoop({
    id: entry.id,
    issue,
    modules,
    sandbox,
    workDir,
    onEvent: (e) => events.push(e),
    generateFn: dry
      ? async () => ({
          source: DRY_SOURCES[entry.id] ?? "// NO_REPRO\n",
          usage: { tokensIn: 0, tokensOut: 0 },
          truncated: false,
        })
      : undefined,
  });

  const got = renderOutcome(result.outcome);
  const want = `${entry.expect.verdict} (${entry.expect.mode})`;
  const sigs = result.attempts.flatMap((a) => a.sigs);

  const verdictOk = got === want;
  const sigOk = sigs.some((s) => s.includes(entry.expect.sigContains));
  const attemptsUsed = result.attempts.length;
  const efficiencyOk = attemptsUsed <= entry.expect.maxAttempts;
  const reproLoc = result.repro === null ? null : result.repro.split("\n").filter(Boolean).length;
  const minimalityOk = reproLoc === null || reproLoc <= entry.expect.reproMaxLoc;

  let replayOk = false;
  if (entry.expect.replay && result.repro !== null) {
    const replay = await replayLog(events, sandbox, workDir, modules);
    replayOk = replay.identical;
  }

  // A wrong-but-deterministic verdict and the expected one are both hard fails;
  // sigOk is reported separately because it catches "right mode, wrong bug".
  const note = [
    sigOk ? "" : `sigContains(${entry.expect.sigContains}) missed`,
    reproLoc === null ? "no repro emitted" : "",
  ]
    .filter(Boolean)
    .join("; ");

  return {
    id: entry.id,
    got,
    want,
    verdictOk: verdictOk && sigOk,
    minimalityOk,
    efficiencyOk,
    replayOk,
    attempts: attemptsUsed,
    reproLoc,
    sigs,
    retries: 0,
    inconclusive: false,
    note,
    usage: result.usage,
  };
}

/**
 * flaky-race keys its failure on pid parity, so 3 runs agree ~25% of the time.
 * Re-rolling the fixture on an inconclusive verdict keeps the score from
 * reporting a coin flip as a harness defect.
 */
async function runEntryWithRetries(entry: SuiteEntry, dry: boolean): Promise<EntryResult> {
  const max = entry.retries ?? 0;
  let last = await runEntry(entry, dry);
  for (let i = 0; i < max && !last.verdictOk && entry.inconclusiveOk; i++) {
    const again = await runEntry(entry, dry);
    if (again.verdictOk) {
      again.retries = i + 1;
      return again;
    }
    last = again;
  }
  if (entry.inconclusiveOk && !last.verdictOk) last.inconclusive = true;
  return last;
}

async function main(): Promise<void> {
  const model = argOf("--model", process.env.DO_MODEL ?? "openai-gpt-oss-120b");
  const dry = hasFlag("--dry");
  const only = argOf("--only", "");
  const started = Date.now();

  const raw = await readFile(new URL("suite.json", HERE), "utf8");
  const suite = (JSON.parse(raw) as SuiteEntry[]).filter((e) => !only || e.id === only);

  const results: EntryResult[] = [];
  let tokensIn = 0;
  let tokensOut = 0;

  for (const entry of suite) {
    const r = await runEntryWithRetries(entry, dry);
    results.push(r);
    tokensIn += r.usage.tokensIn;
    tokensOut += r.usage.tokensOut;
  }

const n = results.length || 1;
const acc = (f: (r: EntryResult) => boolean): number => results.filter(f).length / n;
const verdictAccuracy = acc((r) => r.verdictOk);
const minimality = acc((r) => r.minimalityOk);
const efficiency = acc((r) => r.efficiencyOk);

// Replay only applies to entries that actually emitted a repro. A correct
// cannot-repro must not be penalised for having nothing to replay, so it is
// excluded from the denominator rather than counted as a replay failure.
const emitted = results.filter((r) => r.reproLoc !== null);
const replayIntegrity = emitted.length === 0 ? 1 : emitted.filter((r) => r.replayOk).length / emitted.length;

  const score = Math.round(
    W_VERDICT * verdictAccuracy + W_MINIMALITY * minimality + W_EFFICIENCY * efficiency + W_REPLAY * replayIntegrity,
  );

  for (const r of results) {
    const mark = r.inconclusive ? "?" : r.verdictOk ? "PASS" : "FAIL";
    console.log(
      `${mark.padEnd(4)} ${r.id.padEnd(20)} got=${r.got.padEnd(28)} want=${r.want.padEnd(28)}` +
        ` attempts=${r.attempts} loc=${r.reproLoc ?? "-"} replay=${r.replayOk}${r.note ? `  [${r.note}]` : ""}`,
    );
    for (const s of r.sigs) console.log(`       ${s}`);
  }

  const row = {
    ts: new Date().toISOString(),
    model,
    dry,
    score,
    breakdown: {
      verdict: +verdictAccuracy.toFixed(3),
      minimality: +minimality.toFixed(3),
      efficiency: +efficiency.toFixed(3),
      replay: +replayIntegrity.toFixed(3),
    },
    tokensIn,
    tokensOut,
    cost: +cost(model, tokensIn, tokensOut).toFixed(6),
    wallClockMs: Date.now() - started,
  };

  console.log(JSON.stringify(row, null, 2));

  if (dry) {
    console.log("dry run — no leaderboard row written");
    return;
  }
  await appendFile(new URL("leaderboard.jsonl", HERE), JSON.stringify(row) + "\n");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});