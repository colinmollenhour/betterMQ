import { describe, expect, it } from "vitest";
import { canRemoveNode, isLocalCell } from "./targets";

describe("node removal targeting", () => {
  it("recognizes the reserved local alias after cluster formation", () => {
    expect(isLocalCell("local", "cluster-a")).toBe(true);
    expect(isLocalCell("cluster-a", "cluster-a")).toBe(true);
    expect(isLocalCell("cluster-b", "cluster-a")).toBe(false);
  });
  it("rejects the same named broker in a remote cell", () => {
    const broker = { name: "broker2" };
    expect(canRemoveNode({ cellId: "a", self: false, broker }, true, "a")).toBe(true);
    expect(canRemoveNode({ cellId: "b", self: false, broker }, true, "a")).toBe(false);
  });
  it("rejects self, non-broker, non-seed and unknown local identity", () => {
    expect(canRemoveNode({ cellId: "a", self: true, broker: {} }, true, "a")).toBe(false);
    expect(canRemoveNode({ cellId: "a", self: false }, true, "a")).toBe(false);
    expect(canRemoveNode({ cellId: "a", self: false, broker: {} }, false, "a")).toBe(false);
    expect(canRemoveNode({ cellId: "a", self: false, broker: {} }, true)).toBe(false);
  });
});
