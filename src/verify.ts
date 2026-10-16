// Lane 1 — pure gate. No imports; no I/O, no LLM, deterministic.
export type GateVerdict = "deterministic" | "no-failure" | "flaky" | "invalid-candidate";

function isLoadError(sig: string): boolean {
  return (
    sig.includes("ERR_MODULE_NOT_FOUND") ||
    sig.includes("SyntaxError") ||
    sig.includes("Cannot find module")
  );
}

// 3× identical non-NO_FAIL → deterministic; 3× NO_FAIL → no-failure;
// 3× load errors → invalid-candidate; mixed → flaky.
export function gate(sigs: [string, string, string]): GateVerdict {
  const [s1, s2, s3] = sigs;
  if (s1 === "NO_FAIL" && s2 === "NO_FAIL" && s3 === "NO_FAIL") return "no-failure";
  if (s1 === s2 && s2 === s3) return isLoadError(s1) ? "invalid-candidate" : "deterministic";
  return "flaky";
}
