# Crash: deterministic TypeError on empty config

Calling `load({})` throws `TypeError: Cannot read properties of undefined`.
Repro: `import { load } from "./config"; load({});`
Happens every run on Node 22, no env vars needed.
Expected: throw a named `ConfigError` instead.
Stack points at `config.ts:12` reading `.name`.
