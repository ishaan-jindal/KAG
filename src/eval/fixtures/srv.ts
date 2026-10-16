// Fixture module for hang.md — serve() must never return and must keep the
// event loop alive so the sandbox SIGKILLs at 10s and yields TIMEOUT.

export function serve(port = 0): Promise<never> {
  void port;
  // Holds the loop open forever; the promise never settles.
  setInterval(() => {}, 1_000);
  return new Promise<never>(() => {});
}