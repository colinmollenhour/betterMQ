import { expect, it, vi } from "vitest";
vi.mock("./api", () => ({ adminUrl: (path: string) => path, apiGet: vi.fn() }));
import { dataForCell } from "./federation";
it("never substitutes another cell when the selected cell failed", () => {
  const result = { ok: [{ cell: "a", data: { nodeCount: 3 } }], failed: [{ cell: "b", error: "offline" }] };
  expect(dataForCell(result, "b")).toBeNull();
  expect(dataForCell(result, "a")).toEqual({ nodeCount: 3 });
  expect(dataForCell(result, "missing")).toBeNull();
});
