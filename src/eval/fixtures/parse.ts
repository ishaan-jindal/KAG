// Fixture module for volatile-trace.md — the failure is deterministic but the
// thrown error embeds a wall-clock timestamp and an absolute path, so the
// signature only matches 3x if normalizeStack strips both.

export function parse(input: string): unknown {
  const stamp = new Date().toISOString();
  // Absolute path + timestamp land in the stack, on purpose.
  throw new TypeError(`bad token ${stamp} in ${import.meta.filename}`);
}