import { expect, it } from "vitest";
import { cellCredentialError } from "./cellCredentials";
it("requires a local admin token for new remote cells", () => {
  expect(cellCredentialError("", false)).toBeTruthy();
  expect(cellCredentialError("cluster-secret", false)).toBeTruthy();
  expect(cellCredentialError("sk_local_", false)).toBeTruthy();
  expect(cellCredentialError("sk_local_example", false)).toBeNull();
});
it("permits blank edits to preserve write-only credentials", () => {
  expect(cellCredentialError("", true)).toBeNull();
  expect(cellCredentialError("cluster-secret", true)).toBeTruthy();
});
