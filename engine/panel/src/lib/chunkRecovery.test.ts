import { expect, it, vi } from "vitest";
import { recoverChunk } from "./chunkRecovery";
it("reloads missing chunks only once across navigations", () => {
  const data = new Map<string, string>();
  const storage = { getItem: (key: string) => data.get(key) ?? null, setItem: (key: string, value: string) => { data.set(key, value); } };
  const reload = vi.fn();
  const error = new Error("Failed to fetch dynamically imported module: /panel/assets/old.js");
  expect(recoverChunk(error, storage, reload)).toBe(true);
  expect(recoverChunk(error, storage, reload)).toBe(false);
  expect(reload).toHaveBeenCalledTimes(1);
  expect(recoverChunk(new Error("unrelated"), storage, reload)).toBe(false);
});
