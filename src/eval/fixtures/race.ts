// Fixture module for flaky-race.md — fails on roughly half of all runs, keyed
// on pid parity so it varies between the 3 sandbox runs of one attempt. A repro
// importing this must be graded cannot-repro (flaky), never blessed as proof.
//
// P(all 3 runs agree) = 25%, so the scorer retries this fixture on an
// inconclusive verdict rather than reporting a coin flip as a gate failure.

const queue: string[] = [];

export function push(job: string): number {
  if (process.pid % 2 === 0) throw new RangeError("buffer full");
  return queue.push(job);
}

export function flush(): void {
  queue.length = 0;
}