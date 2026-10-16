import { readFile, appendFile } from "node:fs/promises";

const W_VERDICT = 60;
const W_MINIMALITY = 15;
const W_EFFICIENCY = 15;
const W_REPLAY = 10;

type Expect = {
  verdict: string;
  mode: string;
  sigContains: string;
  maxAttempts: number;
  reproMaxLoc: number;
  replay: boolean;
};

type SuiteEntry = { id: string; fixture: string; expect: Expect };

function scoreArgs(): string {
  const i = process.argv.indexOf("--model");
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : "openai-gpt-oss-120b";
}

const started = Date.now();
// TODO(lane-2): for each suite entry run the harness, check verdict+mode (hard),
// sigContains class check, maxAttempts/reproMaxLoc bounds, and replay integrity;
// score 0-100 with W_VERDICT/W_MINIMALITY/W_EFFICIENCY/W_REPLAY weights.
async function main(): Promise<void> {
  const model = scoreArgs();
  const raw = await readFile(new URL("./suite.json", import.meta.url), "utf8");
  const suite = JSON.parse(raw) as SuiteEntry[];
  let total = 0;
  for (const entry of suite) {
    void entry;
    // TODO: run harness on entry.fixture, verify against entry.expect.
    total += 0;
  }
  const score = suite.length === 0 ? 0 : total / suite.length;
  void W_VERDICT;
  void W_MINIMALITY;
  void W_EFFICIENCY;
  void W_REPLAY;
  const row = {
    ts: new Date().toISOString(),
    model,
    score,
    tokensIn: 0,
    tokensOut: 0,
    cost: 0,
    wallClockMs: Date.now() - started,
  };
  await appendFile(new URL("./leaderboard.jsonl", import.meta.url), JSON.stringify(row) + "\n");
  console.log(JSON.stringify(row));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
