import { beforeEach, expect, it, vi } from "vitest";
const get = vi.hoisted(() => vi.fn());
vi.mock("./api", () => ({ apiGet: get, publicUrl: (path: string) => path }));
import { catalog, refreshQueues, refreshGroups, refreshFlows } from "./catalog";

beforeEach(() => get.mockReset());
it.each([
  ["queues", refreshQueues], ["groups", refreshGroups], ["flows", refreshFlows],
] as const)("preserves %s on failures and distinguishes successful empty results", async (kind, refresh) => {
  get.mockResolvedValueOnce({ [kind]: [{ id: "existing" }] });
  await refresh();
  for (const error of [new Error("HTTP 401"), new Error("HTTP 500"), new TypeError("offline")]) {
    get.mockRejectedValueOnce(error);
    await expect(refresh()).rejects.toBe(error);
    expect(catalog[kind]).toEqual([{ id: "existing" }]);
    expect(catalog.errors[kind]).toContain(error.message);
  }
  get.mockResolvedValueOnce({ [kind]: [] });
  await refresh();
  expect(catalog[kind]).toEqual([]);
  expect(catalog.errors[kind]).toBe("");
});
