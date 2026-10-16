// Fixture module for crash-deterministic.md — the "code under test".
// load({}) must throw a bare TypeError by reading .name off undefined.

export class ConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ConfigError";
  }
}

type Profile = { name: string };

export function load(raw: Record<string, unknown>): string {
  const profile = raw.profile as Profile | undefined;
  // Unguarded on purpose: load({}) dereferences undefined.
  return profile.name.toUpperCase();
}