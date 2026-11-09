import { afterEach, beforeEach, expect, it, vi } from "vitest";
import "vue";

function memoryStorage() {
  const values = new Map<string, string>();
  return { getItem: (k: string) => values.get(k) ?? null,
    setItem: (k: string, v: string) => values.set(k, v), removeItem: (k: string) => values.delete(k) };
}

beforeEach(() => {
  vi.resetModules();
  vi.useFakeTimers();
  vi.stubGlobal("localStorage", memoryStorage());
  vi.stubGlobal("sessionStorage", memoryStorage());
  vi.stubGlobal("location", { origin: "http://panel.test", protocol: "http:" });
  vi.stubGlobal("window", { __BETTERMQ_PANEL_MODE__: "standalone", matchMedia: () => ({ matches: false }) });
  vi.stubGlobal("document", { documentElement: { classList: { toggle: vi.fn() }, dataset: {} } });
});
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); vi.unstubAllGlobals(); });

it("initializes standalone mode through the authenticated admin API only", async () => {
  const fetch = vi.fn(async (_url: string | URL | Request, _init?: RequestInit) => new Response(JSON.stringify({ cells: [], members: [] })));
  vi.stubGlobal("fetch", fetch);
  const { initAuth, session, submitGateToken } = await import("./session");
  submitGateToken("admin-test-credential");
  await initAuth();
  expect(session.phase).toBe("app");
  expect(fetch.mock.calls.some(([url]) => String(url).includes("/v1/auth/config"))).toBe(false);
  expect(fetch.mock.calls.some(([url]) => String(url).includes("/v1/queues"))).toBe(false);
  expect(fetch.mock.calls[0]?.[0]).toBe("http://panel.test/admin/v1/cells");
});

it("shows the gate when standalone admin authentication is rejected", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response("{}", { status: 401 })));
  const { initAuth, session } = await import("./session");
  await initAuth();
  expect(session.phase).toBe("gate");
});

it("blocks local mutations after a remote cell selection before sending HTTP", async () => {
  window.__BETTERMQ_PANEL_MODE__ = "embedded";
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  const { session, setCellId } = await import("./session");
  const { apiDelete, publicUrl } = await import("./api");
  session.phase = "app";
  session.cluster = { cluster_id: "a" };
  session.cells = [{ id: "a", region: "a", controllerUrl: "" }, { id: "b", region: "b", controllerUrl: "" }];
  setCellId("b");
  await expect(apiDelete(publicUrl("/v1/queues/production"))).rejects.toThrow("local broker only");
  expect(fetch).not.toHaveBeenCalled();
});
