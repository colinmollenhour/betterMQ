import { expect, it } from "vitest";
import { prepareDestination } from "./destination";
it("requires an explicit destination before generating a secret", () => {
  const form = { url: "", secret: "" };
  expect(prepareDestination(form)).toBe("Enter a destination URL");
  expect(form.secret).toBe("");
});
it("generates unique secrets and preserves explicit ones", () => {
  const a = { url: "https://example.com/hook", secret: "" };
  const b = { ...a };
  expect(prepareDestination(a)).toBeNull();
  expect(prepareDestination(b)).toBeNull();
  expect(a.secret).toMatch(/^whsec_[a-f0-9]{64}$/);
  expect(a.secret).not.toBe(b.secret);
  a.secret = "custom";
  expect(prepareDestination(a)).toBeNull();
  expect(a.secret).toBe("custom");
});
