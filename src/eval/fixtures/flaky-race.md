# Flaky: race drops every ~3rd write

`queue.push(job)` intermittently throws `RangeError: buffer full`.
Only fails under parallel load; single-run repro passes.
Suspected missing await between `push` and `flush`.
Fails ~1 in 3 runs with the same script.
Do not bless a single passing run as proof.
