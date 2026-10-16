// Lane 1 self-check: `node src/lane1.check.ts`. No deps beyond stdlib.
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { normalizeStack, runSandbox, signature } from "./tools.ts";
import { gate, mode } from "./verify.ts";

const THROWER = 'throw new TypeError("lane1-boom");\n';

function makeThrower(): { dir: string; entry: string } {
  const dir = mkdtempSync(join(tmpdir(), "lane1-"));
  const entry = join(dir, "thrower.ts");
  writeFileSync(entry, THROWER);
  return { dir, entry };
}

// Inline thrower ×3 → equal sigs.
const a = makeThrower();
const sigs = [
  signature(await runSandbox(a.entry, a.dir)),
  signature(await runSandbox(a.entry, a.dir)),
  signature(await runSandbox(a.entry, a.dir)),
];
assert.equal(sigs[0], sigs[1]);
assert.equal(sigs[1], sigs[2]);
assert.match(sigs[0] ?? "", /^EXIT:\d+\|TypeError\|/);

// Same thrower in a second tmp dir → identical normalized output + sig.
const b = makeThrower();
const rb = await runSandbox(b.entry, b.dir);
const ra = await runSandbox(a.entry, a.dir);
assert.equal(normalizeStack(rb.stderr), normalizeStack(ra.stderr));
assert.equal(signature(rb), sigs[0]);

// Clean exit → NO_FAIL.
const cleanDir = mkdtempSync(join(tmpdir(), "lane1-clean-"));
const cleanEntry = join(cleanDir, "clean.ts");
writeFileSync(cleanEntry, 'console.log("lane1-ok");\n');
assert.equal(signature(await runSandbox(cleanEntry, cleanDir)), "NO_FAIL");

// Gate truth table.
assert.equal(gate([sigs[0] ?? "", sigs[1] ?? "", sigs[2] ?? ""]), "deterministic");
assert.equal(gate(["NO_FAIL", "NO_FAIL", "NO_FAIL"]), "no-failure");
assert.equal(gate([sigs[0] ?? "", "NO_FAIL", sigs[0] ?? ""]), "flaky");
assert.equal(
  gate(["EXIT:1|SyntaxError|a", "EXIT:1|SyntaxError|a", "EXIT:1|SyntaxError|a"]),
  "invalid-candidate",
);
assert.equal(
  gate([
    "EXIT:1|ERR_UNKNOWN_FILE_EXTENSION|a",
    "EXIT:1|ERR_UNKNOWN_FILE_EXTENSION|a",
    "EXIT:1|ERR_UNKNOWN_FILE_EXTENSION|a",
  ]),
  "invalid-candidate",
);
assert.equal(gate(["TIMEOUT", "TIMEOUT", "TIMEOUT"]), "deterministic");

// mode: hang vs crash.
assert.equal(mode("TIMEOUT"), "hang");
assert.equal(mode("EXIT:1|TypeError|x"), "crash");

// F1 regression: real missing package → load-error sig → invalid-candidate.
const missDir = mkdtempSync(join(tmpdir(), "lane1-miss-"));
const missEntry = join(missDir, "miss.ts");
writeFileSync(missEntry, 'import "nope-xyz";\n');
const missSig = signature(await runSandbox(missEntry, missDir));
assert.equal(gate([missSig, missSig, missSig]), "invalid-candidate");

// Bare `Error:` on stderr with exit 0 → EXIT form, not NO_FAIL.
const err0Dir = mkdtempSync(join(tmpdir(), "lane1-err0-"));
const err0Entry = join(err0Dir, "err0.ts");
writeFileSync(err0Entry, 'console.error("Error: boom");\n');
assert.match(signature(await runSandbox(err0Entry, err0Dir)), /^EXIT:0\|Error\|/);

console.log("lane1.check OK", sigs[0]);
