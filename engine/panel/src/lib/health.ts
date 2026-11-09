export function readinessLabel(ready: boolean | null, observedAt: number, now: number): string {
  if (ready === null || !observedAt || now - observedAt > 30000) return "Readiness unknown";
  return ready ? "Ready" : "Not ready";
}

export function memberHealthLabel(healthy?: boolean): string {
  return healthy === undefined ? "Unknown (registered)" : healthy ? "Healthy" : "Down";
}

export function serverVersion(body: { version?: unknown }): string | null {
  return typeof body.version === "string" && body.version.trim() ? body.version : null;
}
