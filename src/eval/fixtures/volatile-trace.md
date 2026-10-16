# Crash with volatile trace (timestamps, abs paths)

`parse("bad")` throws `TypeError: bad token` every run.
But the stack embeds timestamps and absolute paths like
`/home/user/app/parse.ts:7:15` plus `Node.js v22` banner lines.
Normalizer must strip those; signature must still match 3x.
Right class (`TypeError`), not just any failure.
