import { expect, it } from "vitest";
import { memberHealthLabel, readinessLabel } from "./health";
it("never calls an unprobed member healthy", () => {
  expect(memberHealthLabel()).toBe("Unknown (registered)");
  expect(memberHealthLabel(false)).toBe("Down");
});
it("distinguishes not ready, unknown and stale observations", () => {
  expect(readinessLabel(true, 1000, 2000)).toBe("Ready");
  expect(readinessLabel(false, 1000, 2000)).toBe("Not ready");
  expect(readinessLabel(true, 1000, 32000)).toBe("Readiness unknown");
  expect(readinessLabel(null, 1000, 2000)).toBe("Readiness unknown");
});
